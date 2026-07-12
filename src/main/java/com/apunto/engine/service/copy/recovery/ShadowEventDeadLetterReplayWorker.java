package com.apunto.engine.service.copy.recovery;

import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "copy.shadow.deadletter.replay", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ShadowEventDeadLetterReplayWorker {

    private final ShadowEventDeadLetterStore store;
    private final ShadowCopyTradingService shadowCopyTradingService;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final int batchSize;
    private final long leaseMs;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ShadowEventDeadLetterReplayWorker(
            ShadowEventDeadLetterStore store,
            ShadowCopyTradingService shadowCopyTradingService,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            @Value("${copy.shadow.deadletter.replay.batch-size:50}") int batchSize,
            @Value("${copy.shadow.deadletter.replay.lease-ms:60000}") long leaseMs
    ) {
        this.store = store;
        this.shadowCopyTradingService = shadowCopyTradingService;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.batchSize = Math.max(1, Math.min(500, batchSize));
        this.leaseMs = Math.max(1_000L, leaseMs);
    }

    @Scheduled(fixedDelayString = "${copy.shadow.deadletter.replay.fixed-delay-ms:10000}")
    public void replay() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            for (ShadowEventDeadLetterStore.DeadLetterItem item : store.claimRecoverable(batchSize, leaseMs)) {
                replayOne(item);
            }
        } catch (RuntimeException ex) {
            meterRegistry.counter("shadow_deadletter_replay_total", "result", "claim_failed").increment();
            log.warn("event=shadow.deadletter.replay.claim_failed reasonCode=SHADOW_DEAD_LETTER_CLAIM_FAILED decision=RETRY_NEXT_SCHEDULE retryable=true shouldAlert=true errorClass={} errorMessage=\"{}\"",
                    ex.getClass().getSimpleName(), safe(ex.getMessage()));
        } finally {
            running.set(false);
        }
    }

    private void replayOne(ShadowEventDeadLetterStore.DeadLetterItem item) {
        long startedNs = System.nanoTime();
        try {
            HyperliquidMappedDelta mappedDelta = objectMapper.readValue(
                    item.payloadJson(), HyperliquidMappedDelta.class);
            if (mappedDelta.event() == null) {
                throw new IllegalStateException("SHADOW_DEAD_LETTER_EVENT_MISSING");
            }
            int recorded = shadowCopyTradingService.recordShadowEvent(mappedDelta.event());
            store.markResolved(item.idempotencyKey());
            meterRegistry.counter("shadow_deadletter_replay_total", "result", "resolved").increment();
            log.info("event=shadow.deadletter.replay.completed reasonCode=SHADOW_DEAD_LETTER_REPLAY_RESOLVED decision=RESOLVED expected=true shouldAlert=false idempotencyKey={} attempt={} recorded={} totalElapsedMs={}",
                    safe(item.idempotencyKey()), item.attemptCount(), recorded, elapsedMs(startedNs));
        } catch (Exception ex) {
            store.markReplayFailed(item.idempotencyKey(), ex);
            meterRegistry.counter("shadow_deadletter_replay_total", "result", "retry_scheduled").increment();
            log.warn("event=shadow.deadletter.replay.failed reasonCode=SHADOW_DEAD_LETTER_REPLAY_FAILED decision=RETRY retryable=true shouldAlert={} idempotencyKey={} attempt={} errorClass={} errorMessage=\"{}\" totalElapsedMs={}",
                    item.attemptCount() >= 10, safe(item.idempotencyKey()), item.attemptCount(),
                    ex.getClass().getSimpleName(), safe(ex.getMessage()), elapsedMs(startedNs));
        }
    }

    private static long elapsedMs(long startedNs) {
        return Math.max(0L, System.nanoTime() - startedNs) / 1_000_000L;
    }

    private static String safe(String value) {
        if (value == null || value.isBlank()) return "NA";
        String clean = value.replace('\n', '_').replace('\r', '_').replace('\t', '_').replace('=', '_');
        return clean.length() > 600 ? clean.substring(0, 600) : clean;
    }
}
