package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDispatchLocalPerformanceTest {

    private static final int WARMUP = 2_000;
    private static final int SAMPLES = 20_000;
    private final CopyIdempotencyKeyFactory keyFactory = new CopyIdempotencyKeyFactory();
    private final MicroLiveBudgetPolicy budgetPolicy = new MicroLiveBudgetPolicy(
            new BigDecimal("20"), new BigDecimal("100"), 5);
    private final CopyDispatchIdentity identity = new CopyDispatchIdentity(
            "user-1", 505L, "MICRO_LIVE", "MOVEMENT_ALL", "ALL", "ALL",
            "source-event-1", "OPEN");

    @Test
    void localIdempotencyAndBudgetDecisionP95StayBelowFiveMilliseconds() {
        for (int i = 0; i < WARMUP; i++) measureOnce();

        long[] durations = new long[SAMPLES];
        for (int i = 0; i < SAMPLES; i++) durations[i] = measureOnce();
        Arrays.sort(durations);

        double p50Ms = millis(durations[(int) (SAMPLES * 0.50)]);
        double p95Ms = millis(durations[(int) (SAMPLES * 0.95)]);
        System.out.printf("copy_dispatch_local_performance samples=%d p50Ms=%.4f p95Ms=%.4f%n",
                SAMPLES, p50Ms, p95Ms);

        // This measures deterministic CPU work only. PostgreSQL latency is exported by
        // signals.copy.dispatch.intent.acquire and must be validated in the target environment.
        assertTrue(p95Ms < 5.0, "local idempotency+budget p95 was " + p95Ms + " ms");
    }

    @Test
    void oneHundredEventsAcrossTwoStrategiesRemainBelowPreEnqueueBudget() {
        long started = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            String source = "source-event-" + i;
            keyFactory.create(new CopyDispatchIdentity(
                    "user-1", 505L, "MICRO_LIVE", "MOVEMENT_ALL", "ALL", "ALL", source, "OPEN"));
            keyFactory.create(new CopyDispatchIdentity(
                    "user-1", 506L, "MICRO_LIVE", "SHORT_ONLY", "DIRECTION", "SHORT", source, "OPEN"));
        }
        double averagePerEventMs = millis(System.nanoTime() - started) / 100.0;
        System.out.printf("copy_dispatch_two_strategy_performance events=100 averagePerEventMs=%.4f%n",
                averagePerEventMs);
        assertTrue(averagePerEventMs < 20.0, "local two-strategy average was " + averagePerEventMs + " ms");
    }

    private long measureOnce() {
        long started = System.nanoTime();
        String key = keyFactory.create(identity);
        BudgetDecision decision = budgetPolicy.evaluate(
                new BudgetSnapshot(new BigDecimal("40"), new BigDecimal("20"), 2, 1),
                new BigDecimal("20"), true);
        if (key.length() != 64 || !decision.allowed()) throw new IllegalStateException("invalid benchmark fixture");
        return System.nanoTime() - started;
    }

    private double millis(long nanos) {
        return nanos / 1_000_000.0;
    }
}
