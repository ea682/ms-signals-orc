package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfSystemProperty(named = "copy.benchmark.enabled", matches = "true")
class CopyDispatchConcurrencyBenchmarkTest {

    private static final int[] SCALES = {1, 10, 50, 100, 300, 1_000};
    private final CopyIdempotencyKeyFactory keyFactory = new CopyIdempotencyKeyFactory();
    private final MicroLiveBudgetPolicy budgetPolicy = new MicroLiveBudgetPolicy(
            new BigDecimal("20"), new BigDecimal("100"), 5);

    @Test
    void cpuOnlyIdentityAndBudgetConcurrencyMatrix() throws Exception {
        for (int i = 0; i < 5_000; i++) measure("warmup-" + i, 505L);

        System.out.println("benchmark=copy_dispatch_cpu_only columns=events,threads,p50_ms,p95_ms,p99_ms,max_ms,throughput_eps");
        for (int scale : SCALES) {
            Result result = runScale(scale);
            System.out.printf(Locale.ROOT,
                    "copy_dispatch_cpu_only events=%d threads=%d p50_ms=%.6f p95_ms=%.6f p99_ms=%.6f max_ms=%.6f throughput_eps=%.2f%n",
                    scale, result.threads(), result.p50Ms(), result.p95Ms(), result.p99Ms(),
                    result.maxMs(), result.throughputPerSecond());
            assertEquals(scale, result.uniqueKeys());
            assertTrue(result.maxMs() < 5_000, "local CPU benchmark stalled at scale " + scale);
        }
    }

    private Result runScale(int events) throws Exception {
        int threads = Math.min(events, 64);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(events);
        long[] durations = new long[events];
        AtomicInteger index = new AtomicInteger();
        Set<String> keys = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < events; i++) {
            int event = i;
            pool.execute(() -> {
                ready.countDown();
                try {
                    start.await();
                    long started = System.nanoTime();
                    keys.add(measure("event-" + event, 505L + (event % 20)));
                    durations[index.getAndIncrement()] = System.nanoTime() - started;
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        ready.await(5, TimeUnit.SECONDS);
        long wallStarted = System.nanoTime();
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        long wallNanos = System.nanoTime() - wallStarted;
        pool.shutdownNow();
        Arrays.sort(durations);
        return new Result(threads, keys.size(), millis(percentile(durations, 0.50)),
                millis(percentile(durations, 0.95)), millis(percentile(durations, 0.99)),
                millis(durations[durations.length - 1]), events / (wallNanos / 1_000_000_000.0));
    }

    private String measure(String sourceEvent, long allocationId) {
        String key = keyFactory.create(new CopyDispatchIdentity(
                "user-1", allocationId, "MICRO_LIVE", "MOVEMENT_ALL", "ALL", "ALL",
                sourceEvent, "OPEN"));
        BudgetDecision decision = budgetPolicy.evaluate(
                new BudgetSnapshot(new BigDecimal("40"), new BigDecimal("20"), 2, 1),
                new BigDecimal("20"), true);
        if (!decision.allowed()) throw new IllegalStateException("invalid benchmark fixture");
        return key;
    }

    private long percentile(long[] sorted, double percentile) {
        int index = Math.min(sorted.length - 1, (int) Math.ceil(sorted.length * percentile) - 1);
        return sorted[Math.max(0, index)];
    }

    private double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private record Result(int threads, int uniqueKeys, double p50Ms, double p95Ms,
                          double p99Ms, double maxMs, double throughputPerSecond) {
    }
}
