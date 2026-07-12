package com.apunto.engine.service.copy.distribution;

import com.apunto.engine.service.copy.concurrency.PostgresDeadlockRetryExecutor;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Slf4j
@Component
public class CopyDistributionUnitExecutor {

    private final EntityManager entityManager;
    private final MeterRegistry meterRegistry;
    private final PostgresDeadlockRetryExecutor deadlockRetryExecutor;
    private final TransactionTemplate transactionTemplate;

    public CopyDistributionUnitExecutor(
            EntityManager entityManager,
            MeterRegistry meterRegistry,
            PostgresDeadlockRetryExecutor deadlockRetryExecutor,
            PlatformTransactionManager transactionManager,
            @Value("${copy.distribution.unit.transaction-timeout-seconds:15}") int timeoutSeconds
    ) {
        this.entityManager = entityManager;
        this.meterRegistry = meterRegistry;
        this.deadlockRetryExecutor = deadlockRetryExecutor;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.transactionTemplate.setTimeout(Math.max(1, timeoutSeconds));
    }

    public UnitMutationResult execute(
            UUID userId,
            String walletId,
            String profileKey,
            Long allocationId,
        Supplier<UnitMutationResult> work
    ) {
        if (work == null) throw new IllegalArgumentException("distribution unit work is required");
        String canonicalProfileKey = canonicalProfileKey(profileKey);
        String safeProfileKey = safe(canonicalProfileKey);
        String lockKey = lockKey(userId, walletId, canonicalProfileKey);
        AtomicInteger attempts = new AtomicInteger();
        AtomicLong lockWaitMs = new AtomicLong();
        AtomicLong transactionMs = new AtomicLong();
        long unitNs = System.nanoTime();
        log.info("event=copy.distribution.unit.started reasonCode=COPY_DISTRIBUTION_UNIT_STARTED userId={} walletId={} profileKey={} allocationId={} lockKey={} result=STARTED",
                userId, safe(walletId), safeProfileKey, allocationId, lockKey);
        try {
            UnitMutationResult result = deadlockRetryExecutor.execute(
                    "copy_distribution_unit",
                    "copy_wallet_profile,shadow_copy_allocation,user_copy_allocation",
                    safeProfileKey,
                    () -> {
                        attempts.incrementAndGet();
                        long transactionNs = System.nanoTime();
                        try {
                            UnitMutationResult committed = transactionTemplate.execute(status -> {
                                long lockNs = System.nanoTime();
                                entityManager.createNativeQuery(
                                                "select pg_advisory_xact_lock(hashtextextended(cast(:lockKey as text), 0))")
                                        .setParameter("lockKey", lockKey)
                                        .getSingleResult();
                                lockWaitMs.set(elapsedMs(lockNs));
                                return work.get();
                            });
                            long elapsedTransactionMs = elapsedMs(transactionNs);
                            transactionMs.set(elapsedTransactionMs);
                            meterRegistry.timer("copy_distribution_transaction_duration", "result", "success")
                                    .record(elapsedTransactionMs, TimeUnit.MILLISECONDS);
                            UnitMutationResult safeResult = committed == null ? UnitMutationResult.none() : committed;
                            return safeResult.withTransactionMs(elapsedTransactionMs);
                        } catch (RuntimeException ex) {
                            long elapsedTransactionMs = elapsedMs(transactionNs);
                            transactionMs.set(elapsedTransactionMs);
                            meterRegistry.timer("copy_distribution_transaction_duration", "result", "failure")
                                    .record(elapsedTransactionMs, TimeUnit.MILLISECONDS);
                            throw ex;
                        }
                    }
            );
            long totalMs = elapsedMs(unitNs);
            meterRegistry.counter("copy_distribution_unit_total", "result", "success").increment();
            log.info("event=copy.distribution.unit.completed reasonCode=COPY_DISTRIBUTION_UNIT_COMPLETED userId={} walletId={} profileKey={} allocationId={} lockKey={} lockWaitMs={} transactionMs={} rowsRead={} rowsInserted={} rowsUpdated={} rowsClosed={} retryCount={} sqlState=NA result=SUCCESS totalElapsedMs={}",
                    userId, safe(walletId), safeProfileKey, allocationId, lockKey, lockWaitMs.get(), result.transactionMs(),
                    result.rowsRead(), result.rowsInserted(), result.rowsUpdated(), result.rowsClosed(),
                    Math.max(0, attempts.get() - 1), totalMs);
            return result;
        } catch (RuntimeException ex) {
            long totalMs = elapsedMs(unitNs);
            String sqlState = PostgresDeadlockRetryExecutor.isDeadlock(ex) ? "40P01" : "NA";
            meterRegistry.counter("copy_distribution_unit_total", "result", "failure").increment();
            log.error("event=copy.distribution.unit.failed reasonCode=COPY_DISTRIBUTION_UNIT_FAILED userId={} walletId={} profileKey={} allocationId={} lockKey={} lockWaitMs={} transactionMs={} rowsRead=0 rowsInserted=0 rowsUpdated=0 rowsClosed=0 retryCount={} sqlState={} result=FAILED retryable={} errorClass={} errorMessage=\"{}\" totalElapsedMs={}",
                    userId, safe(walletId), safeProfileKey, allocationId, lockKey, lockWaitMs.get(), transactionMs.get(),
                    Math.max(0, attempts.get() - 1), sqlState, "40P01".equals(sqlState),
                    ex.getClass().getSimpleName(), safe(ex.getMessage()), totalMs);
            throw ex;
        }
    }

    private static String lockKey(UUID userId, String walletId, String profileKey) {
        // Shared with SHADOW event persistence and promotion. One canonical
        // profile lock prevents lock-order inversions across those flows.
        return "shadow-profile:" + profileKey;
    }

    private static String canonicalProfileKey(String profileKey) {
        return profileKey == null || profileKey.isBlank() ? "NA" : profileKey.trim();
    }

    private static long elapsedMs(long startedNs) {
        return Math.max(0L, System.nanoTime() - startedNs) / 1_000_000L;
    }

    private static String safe(String value) {
        if (value == null || value.isBlank()) return "NA";
        String clean = value.replace('\n', '_')
                .replace('\r', '_')
                .replace('\t', '_')
                .replace(' ', '_')
                .replace('=', '_')
                .replace('"', '\'');
        return clean.length() > 420 ? clean.substring(0, 420) : clean;
    }

    public record UnitMutationResult(
            int rowsRead,
            int rowsInserted,
            int rowsUpdated,
            int rowsClosed,
            long transactionMs
    ) {
        public static UnitMutationResult none() {
            return new UnitMutationResult(0, 0, 0, 0, 0L);
        }

        public static UnitMutationResult persisted(boolean inserted, boolean closed) {
            return new UnitMutationResult(1, inserted ? 1 : 0, inserted || closed ? 0 : 1, closed ? 1 : 0, 0L);
        }

        UnitMutationResult withTransactionMs(long value) {
            return new UnitMutationResult(rowsRead, rowsInserted, rowsUpdated, rowsClosed, Math.max(0L, value));
        }
    }
}
