package com.apunto.engine.service.copy.coverage;

import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowCoverageCountsProjection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresShadowCoverageQueryServiceTest {

    private static final OffsetDateTime NOW = OffsetDateTime.of(2026, 7, 10, 12, 0, 0, 0, ZoneOffset.UTC);

    @Test
    void sameWalletDifferentAllocationsAreIndependentAndLoadedInOneQuery() {
        AtomicInteger calls = new AtomicInteger();
        ShadowCopyOperationEventRepository repository = repository((method, args) -> {
            if ("findRollingCoverageBatch".equals(method.getName())) {
                calls.incrementAndGet();
                return List.of(
                        projection(12L, 111, 0, 3, 0),
                        projection(85L, 80, 0, 20, 0)
                );
            }
            return unexpected(method);
        });
        PostgresShadowCoverageQueryService service = new PostgresShadowCoverageQueryService(repository, rollingProperties());

        ShadowCoverageBatch result = service.load(List.of(12L, 85L), NOW);

        assertEquals(1, calls.get());
        assertFalse(result.queryFailed());
        assertEquals(111, result.countsFor(12L).simulatedEvents());
        assertEquals(3, result.countsFor(12L).skippedEvents());
        assertEquals(80, result.countsFor(85L).simulatedEvents());
        assertEquals(20, result.countsFor(85L).skippedEvents());
    }

    @Test
    void queryReceivesUtcWindowAndConfiguredLimit() {
        AtomicReference<Object[]> captured = new AtomicReference<>();
        ShadowCopyOperationEventRepository repository = repository((method, args) -> {
            if ("findRollingCoverageBatch".equals(method.getName())) {
                captured.set(args);
                return List.of();
            }
            return unexpected(method);
        });
        PostgresShadowCoverageQueryService service = new PostgresShadowCoverageQueryService(repository, rollingProperties());

        ShadowCoverageBatch result = service.load(List.of(12L), NOW.withOffsetSameInstant(ZoneOffset.ofHours(-4)));

        assertEquals(List.of(12L), captured.get()[0]);
        assertEquals(NOW.minusDays(14), captured.get()[1]);
        assertEquals(NOW, captured.get()[2]);
        assertEquals(500, captured.get()[3]);
        assertEquals(NOW.minusDays(14), result.windowStart());
        assertEquals(NOW, result.windowEnd());
    }

    @Test
    void missingProjectionBecomesZeroEventsForThatAllocation() {
        ShadowCopyOperationEventRepository repository = repository((method, args) -> {
            if ("findRollingCoverageBatch".equals(method.getName())) return List.of();
            return unexpected(method);
        });
        PostgresShadowCoverageQueryService service = new PostgresShadowCoverageQueryService(repository, rollingProperties());

        ShadowCoverageBatch result = service.load(List.of(12L), NOW);

        assertEquals(0, result.countsFor(12L).simulatedEvents());
        assertEquals(0, result.countsFor(12L).skippedEvents());
        assertFalse(result.countsFor(12L).queryFailed());
    }

    @Test
    void queryFailureFailsEveryRequestedAllocationClosed() {
        ShadowCopyOperationEventRepository repository = repository((method, args) -> {
            if ("findRollingCoverageBatch".equals(method.getName())) throw new IllegalStateException("database unavailable");
            return unexpected(method);
        });
        PostgresShadowCoverageQueryService service = new PostgresShadowCoverageQueryService(repository, rollingProperties());

        ShadowCoverageBatch result = service.load(List.of(12L, 85L), NOW);

        assertTrue(result.queryFailed());
        assertEquals("IllegalStateException", result.failureClass());
        assertTrue(result.countsFor(12L).queryFailed());
        assertTrue(result.countsFor(85L).queryFailed());
    }

    @Test
    void legacyModeDoesNotQueryEventLedger() {
        AtomicInteger calls = new AtomicInteger();
        ShadowCopyOperationEventRepository repository = repository((method, args) -> {
            calls.incrementAndGet();
            return unexpected(method);
        });
        ShadowCoverageWindowProperties properties = rollingProperties();
        properties.setRollingEnabled(false);
        PostgresShadowCoverageQueryService service = new PostgresShadowCoverageQueryService(repository, properties);

        ShadowCoverageBatch result = service.load(List.of(12L), NOW);

        assertEquals(0, calls.get());
        assertFalse(result.queryFailed());
    }

    @Test
    void duplicateIdsAreSentOnlyOnce() {
        AtomicReference<Object[]> captured = new AtomicReference<>();
        ShadowCopyOperationEventRepository repository = repository((method, args) -> {
            if ("findRollingCoverageBatch".equals(method.getName())) {
                captured.set(args);
                return List.of();
            }
            return unexpected(method);
        });
        PostgresShadowCoverageQueryService service = new PostgresShadowCoverageQueryService(repository, rollingProperties());

        service.load(List.of(12L, 12L, 85L), NOW);

        assertEquals(List.of(12L, 85L), captured.get()[0]);
    }

    private static ShadowCoverageWindowProperties rollingProperties() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();
        properties.setRollingEnabled(true);
        properties.setMode(ShadowCoverageMode.ROLLING);
        properties.setWindowDays(14);
        properties.setMaxEvents(500);
        properties.setMinEvaluableEvents(100);
        return properties;
    }

    private static ShadowCoverageCountsProjection projection(
            long allocationId,
            long simulated,
            long recorded,
            long skipped,
            long errors
    ) {
        return proxy(ShadowCoverageCountsProjection.class, (method, args) -> switch (method.getName()) {
            case "getShadowAllocationId" -> allocationId;
            case "getSimulatedEvents" -> simulated;
            case "getRecordedEvents" -> recorded;
            case "getSkippedEvents" -> skipped;
            case "getErrorEvents" -> errors;
            case "getOldestEventTime" -> NOW.minusHours(3);
            case "getNewestEventTime" -> NOW;
            default -> unexpected(method);
        });
    }

    private static ShadowCopyOperationEventRepository repository(Invocation invocation) {
        return proxy(ShadowCopyOperationEventRepository.class, invocation);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Invocation invocation) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "toString" -> type.getSimpleName() + "Proxy";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> null;
                };
            }
            return invocation.invoke(method, args == null ? new Object[0] : args);
        };
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object unexpected(Method method) {
        throw new AssertionError("Unexpected call: " + method);
    }

    private interface Invocation {
        Object invoke(Method method, Object[] args);
    }
}
