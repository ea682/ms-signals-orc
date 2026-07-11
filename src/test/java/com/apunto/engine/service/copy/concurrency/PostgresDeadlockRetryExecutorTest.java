package com.apunto.engine.service.copy.concurrency;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresDeadlockRetryExecutorTest {

    @Test
    void retriesOnlySqlState40P01AndEventuallyReturns() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        AtomicInteger calls = new AtomicInteger();
        PostgresDeadlockRetryExecutor executor = new PostgresDeadlockRetryExecutor(
                registry, 3, 1, 4, ignored -> { }, () -> 0.0d
        );

        String result = executor.execute("shadow_event", "shadow_copy_allocation", () -> {
            if (calls.incrementAndGet() < 3) throw deadlock();
            return "ok";
        });

        assertEquals("ok", result);
        assertEquals(3, calls.get());
        double retries = registry.find("copy.deadlock.retry.total")
                .tag("flow", "shadow_event").tag("result", "retry")
                .counters().stream().mapToDouble(counter -> counter.count()).sum();
        assertEquals(2.0d, retries);
    }

    @Test
    void exhaustedDeadlockIsRethrownAfterBoundedAttempts() {
        AtomicInteger calls = new AtomicInteger();
        PostgresDeadlockRetryExecutor executor = new PostgresDeadlockRetryExecutor(
                new SimpleMeterRegistry(), 3, 1, 4, ignored -> { }, () -> 0.0d
        );

        assertThrows(DataAccessResourceFailureException.class,
                () -> executor.execute("shadow_event", "shadow_copy_allocation", () -> {
                    calls.incrementAndGet();
                    throw deadlock();
                }));

        assertEquals(3, calls.get());
    }

    @Test
    void nonDeadlockFailureIsNeverRetried() {
        AtomicInteger calls = new AtomicInteger();
        PostgresDeadlockRetryExecutor executor = new PostgresDeadlockRetryExecutor(
                new SimpleMeterRegistry(), 3, 1, 4, ignored -> { }, () -> 0.0d
        );

        assertThrows(IllegalStateException.class,
                () -> executor.execute("shadow_event", "shadow_copy_allocation", () -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("business failure");
                }));

        assertEquals(1, calls.get());
    }

    private static DataAccessResourceFailureException deadlock() {
        return new DataAccessResourceFailureException(
                "deadlock detected",
                new SQLException("deadlock detected", "40P01")
        );
    }
}
