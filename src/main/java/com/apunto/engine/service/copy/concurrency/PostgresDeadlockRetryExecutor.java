package com.apunto.engine.service.copy.concurrency;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

@Component
@Slf4j
public class PostgresDeadlockRetryExecutor {

    static final String DEADLOCK_SQL_STATE = "40P01";

    private final MeterRegistry meterRegistry;
    private final int maxAttempts;
    private final long initialBackoffMs;
    private final long maxBackoffMs;
    private final Sleeper sleeper;
    private final JitterSource jitterSource;

    @Autowired
    public PostgresDeadlockRetryExecutor(
            MeterRegistry meterRegistry,
            @Value("${copy.postgres.deadlock-retry.max-attempts:3}") int maxAttempts,
            @Value("${copy.postgres.deadlock-retry.initial-backoff-ms:25}") long initialBackoffMs,
            @Value("${copy.postgres.deadlock-retry.max-backoff-ms:250}") long maxBackoffMs
    ) {
        this(meterRegistry, maxAttempts, initialBackoffMs, maxBackoffMs,
                Thread::sleep, () -> ThreadLocalRandom.current().nextDouble());
    }

    public PostgresDeadlockRetryExecutor(MeterRegistry meterRegistry,
                                         int maxAttempts,
                                         long initialBackoffMs,
                                         long maxBackoffMs,
                                         Sleeper sleeper,
                                         JitterSource jitterSource) {
        this.meterRegistry = meterRegistry;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.initialBackoffMs = Math.max(0L, initialBackoffMs);
        this.maxBackoffMs = Math.max(this.initialBackoffMs, maxBackoffMs);
        this.sleeper = sleeper == null ? Thread::sleep : sleeper;
        this.jitterSource = jitterSource == null ? () -> 0.5d : jitterSource;
    }

    public <T> T execute(String flow, String table, Supplier<T> work) {
        return execute(flow, table, "NA", work);
    }

    public <T> T execute(String flow, String table, String profileKey, Supplier<T> work) {
        if (work == null) throw new IllegalArgumentException("work is required");
        String flowTag = lowCardinality(flow, "unknown");
        String tableTag = lowCardinality(table, "unknown");
        String safeProfileKey = safeLog(profileKey);
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                T result = work.get();
                if (attempt > 1) {
                    meterRegistry.counter("copy.deadlock.retry.total",
                            "flow", flowTag,
                            "attempt", Integer.toString(attempt),
                            "result", "recovered").increment();
                }
                return result;
            } catch (RuntimeException failure) {
                if (!isDeadlock(failure)) throw failure;

                meterRegistry.counter("copy.deadlock.total", "flow", flowTag, "table", tableTag).increment();
                meterRegistry.counter("copy_deadlock_total", "operation", flowTag).increment();
                if (attempt >= maxAttempts) {
                    meterRegistry.counter("copy.deadlock.retry.total",
                            "flow", flowTag,
                            "attempt", Integer.toString(attempt),
                            "result", "exhausted").increment();
                    meterRegistry.counter("copy_deadlock_retry_total", "result", "exhausted").increment();
                    log.error("event=postgres.deadlock.exhausted reasonCode=POSTGRES_DEADLOCK_RETRIES_EXHAUSTED sqlState=40P01 operation={} table={} profileKey={} attempt={} maxAttempts={} retryable=false result=EXHAUSTED recoveryAction=DLQ_OR_MANUAL_REVIEW shouldAlert=true",
                            flowTag, tableTag, safeProfileKey, attempt, maxAttempts);
                    throw failure;
                }

                meterRegistry.counter("copy.deadlock.retry.total",
                        "flow", flowTag,
                        "attempt", Integer.toString(attempt),
                        "result", "retry").increment();
                meterRegistry.counter("copy_deadlock_retry_total", "result", "retrying").increment();
                long waitMs = backoffMs(attempt);
                meterRegistry.timer("copy.lock.wait", "flow", flowTag)
                        .record(Duration.ofMillis(waitMs));
                log.warn("event=postgres.deadlock.retry reasonCode=POSTGRES_DEADLOCK_RETRY sqlState=40P01 operation={} table={} profileKey={} attempt={} maxAttempts={} backoffMs={} retryable=true result=RETRYING",
                        flowTag, tableTag, safeProfileKey, attempt, maxAttempts, waitMs);
                sleep(waitMs, failure);
            }
        }
        throw new IllegalStateException("deadlock retry loop exhausted unexpectedly");
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public static boolean isDeadlock(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable cursor = failure;
        while (cursor != null && seen.add(cursor)) {
            if (cursor instanceof SQLException sql && DEADLOCK_SQL_STATE.equals(sql.getSQLState())) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private long backoffMs(int attempt) {
        if (initialBackoffMs <= 0L) return 0L;
        long multiplier = 1L << Math.min(20, Math.max(0, attempt - 1));
        long base;
        try {
            base = Math.multiplyExact(initialBackoffMs, multiplier);
        } catch (ArithmeticException overflow) {
            base = maxBackoffMs;
        }
        base = Math.min(base, maxBackoffMs);
        double jitter = Math.max(0.0d, Math.min(1.0d, jitterSource.next()));
        long extra = Math.round(base * 0.25d * jitter);
        return Math.min(maxBackoffMs, base + extra);
    }

    private void sleep(long waitMs, RuntimeException original) {
        if (waitMs <= 0L) return;
        try {
            sleeper.sleep(waitMs);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            original.addSuppressed(interrupted);
            throw original;
        }
    }

    private static String lowCardinality(String value, String fallback) {
        if (value == null || value.isBlank()) return fallback;
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT)
                .replace('-', '_').replace(' ', '_');
        return normalized.length() > 48 ? normalized.substring(0, 48) : normalized;
    }

    private static String safeLog(String value) {
        if (value == null || value.isBlank()) return "NA";
        String clean = value.replace('\n', '_').replace('\r', '_').replace('\t', '_').replace('=', '_');
        return clean.length() > 420 ? clean.substring(0, 420) : clean;
    }

    @FunctionalInterface
    public interface Sleeper {
        void sleep(long milliseconds) throws InterruptedException;
    }

    @FunctionalInterface
    public interface JitterSource {
        double next();
    }
}
