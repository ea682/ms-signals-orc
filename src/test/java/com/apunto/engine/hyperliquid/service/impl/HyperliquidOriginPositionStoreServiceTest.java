package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.apunto.engine.repository.FuturesPositionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperliquidOriginPositionStoreServiceTest {

    @Test
    void productionResizeWithCompleteSourceSnapshotSeedsNonEconomicBaseline() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidOriginPositionStoreService service = service(registry);
        try {
            HyperliquidMappedDelta mapped = service.bindOriginIdForCopy(productionLateAdjustment());

            service.submitAfterCopy(mapped, HyperliquidDirectCopyDispatchResult.ok(
                    0, 0, 1, 0, false, "resize_without_open_copy"));

            Counter submitted = registry.find("signals.hyperliquid.origin_store.submitted.total")
                    .tag("deltaType", "RESIZE")
                    .counter();
            assertNotNull(submitted,
                    "a valid source snapshot must become a non-economic baseline instead of being discarded");
            assertEquals(1.0d, submitted.count());
            assertNull(registry.find("signals.hyperliquid.origin_store.skipped.total")
                    .tag("reason", "late_adjustment_without_active_origin")
                    .counter());
        } finally {
            service.stop();
        }
    }

    @Test
    void incompleteSnapshotRemainsFailClosed() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidOriginPositionStoreService service = service(registry);
        try {
            HyperliquidMappedDelta mapped = service.bindOriginIdForCopy(productionLateAdjustment(request ->
                    request.put("sourcePortfolioComplete", false)));

            service.submitAfterCopy(mapped, HyperliquidDirectCopyDispatchResult.ok(
                    0, 0, 1, 0, false, "resize_without_open_copy"));

            assertNull(registry.find("signals.hyperliquid.origin_store.submitted.total").counter());
            assertEquals(1.0d, registry.find("signals.hyperliquid.origin_store.skipped.total")
                    .tag("reason", "late_adjustment_without_active_origin")
                    .counter().count());
            assertEquals(1.0d, registry.find("late_adjustment_without_origin_total")
                    .tag("baseline_status", "invalid")
                    .tag("reason", "portfolio_incomplete")
                    .counter().count());
        } finally {
            service.stop();
        }
    }

    @Test
    void validBaselinePreventsSubsequentAdjustmentsFromBeingLostIndefinitely() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidOriginPositionStoreService service = service(registry);
        try {
            HyperliquidMappedDelta baseline = service.bindOriginIdForCopy(productionLateAdjustment());
            service.submitAfterCopy(baseline, HyperliquidDirectCopyDispatchResult.ok(
                    0, 0, 1, 0, false, "resize_without_open_copy"));

            HyperliquidMappedDelta next = service.bindOriginIdForCopy(productionLateAdjustment(request -> {
                request.put("eventId", "event_sanitized_c002");
                request.put("idempotencyKey", "event_sanitized_c002");
                request.put("sourcePortfolioComplete", false);
            }));
            service.submitAfterCopy(next, HyperliquidDirectCopyDispatchResult.ok(
                    0, 0, 1, 0, false, "resize_without_open_copy"));

            assertEquals(2.0d, registry.find("signals.hyperliquid.origin_store.submitted.total")
                    .tag("deltaType", "RESIZE")
                    .counter().count());
            assertNull(registry.find("signals.hyperliquid.origin_store.skipped.total")
                    .tag("reason", "late_adjustment_without_active_origin")
                    .counter());
        } finally {
            service.stop();
        }
    }

    @Test
    void baselinePolicyIsExplicitlyNonEconomicAndPnlFree() throws Exception {
        HyperliquidOriginBaselinePolicy.Decision decision = new HyperliquidOriginBaselinePolicy()
                .evaluate(productionLateAdjustment());

        assertTrue(decision.valid());
        assertEquals("NON_ECONOMIC_ESTIMATED", decision.baselineKind());
        assertFalse(decision.economicEvent());
        assertNull(decision.realizedPnlUsd());
        assertNull(decision.unrealizedPnlUsd());
        assertEquals(0, new BigDecimal("120.58").compareTo(decision.sizeQty()));
        assertEquals(318L, decision.snapshotVersion());
    }

    @Test
    void persistedBaselineCarriesNoPnlAndKeepsReconciliationMetadata() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Map<String, Object> sqlParameters = Collections.synchronizedMap(new HashMap<>());
        HyperliquidOriginPositionStoreService service = service(registry);
        injectEntityManager(service, recordingEntityManager(sqlParameters));
        service.start();
        try {
            HyperliquidMappedDelta mapped = service.bindOriginIdForCopy(productionLateAdjustment());
            service.submitAfterCopy(mapped, HyperliquidDirectCopyDispatchResult.ok(
                    0, 0, 1, 0, false, "resize_without_open_copy"));

            awaitCounter(registry, "origin_baseline_hydrated_total", Duration.ofSeconds(3));

            assertNull(sqlParameters.get("realizedPnlUsd"));
            assertNull(sqlParameters.get("unrealizedPnlUsd"));
            assertEquals("OPEN", sqlParameters.get("status"));
            String raw = String.valueOf(sqlParameters.get("raw"));
            assertTrue(raw.contains("\"__origin_baseline_kind\":\"NON_ECONOMIC_ESTIMATED\""));
            assertTrue(raw.contains("\"__origin_baseline_economic_event\":false"));
            assertTrue(raw.contains("\"__origin_baseline_pnl_basis\":\"NONE\""));
        } finally {
            service.stop();
        }
    }

    private HyperliquidOriginPositionStoreService service(SimpleMeterRegistry registry) {
        return new HyperliquidOriginPositionStoreService(
                emptyRepository(),
                new ObjectMapper().findAndRegisterModules(),
                null,
                noOpTransactionManager(),
                registry,
                true,
                1,
                16,
                false,
                false,
                true
        );
    }

    private FuturesPositionRepository emptyRepository() {
        return (FuturesPositionRepository) Proxy.newProxyInstance(
                FuturesPositionRepository.class.getClassLoader(),
                new Class<?>[]{FuturesPositionRepository.class},
                (proxy, method, args) -> {
                    if (method.getReturnType() == Optional.class) {
                        return Optional.empty();
                    }
                    if (method.getReturnType() == List.class) {
                        return List.of();
                    }
                    if (method.getReturnType() == boolean.class) {
                        return false;
                    }
                    if (method.getReturnType() == long.class) {
                        return 0L;
                    }
                    if (method.getReturnType() == int.class) {
                        return 0;
                    }
                    return null;
                }
        );
    }

    private PlatformTransactionManager noOpTransactionManager() {
        return new PlatformTransactionManager() {
            @Override
            public TransactionStatus getTransaction(TransactionDefinition definition) {
                return new SimpleTransactionStatus();
            }

            @Override
            public void commit(TransactionStatus status) {
            }

            @Override
            public void rollback(TransactionStatus status) {
            }
        };
    }

    private HyperliquidMappedDelta productionLateAdjustment() throws Exception {
        return productionLateAdjustment(request -> {
        });
    }

    private HyperliquidMappedDelta productionLateAdjustment(Consumer<ObjectNode> requestMutation) throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        try (InputStream fixture = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-c-late-adjustment-without-origin.json")) {
            assertNotNull(fixture);
            JsonNode root = mapper.readTree(fixture);
            ObjectNode requestNode = (ObjectNode) root.get("request");
            requestMutation.accept(requestNode);
            HyperliquidDeltaRequest request = mapper.treeToValue(
                    requestNode, HyperliquidDeltaRequest.class);
            return new HyperliquidDeltaOperacionMapper().map(request, request.idempotencyKey());
        }
    }

    private EntityManager recordingEntityManager(Map<String, Object> parameters) {
        Query query = (Query) Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[]{Query.class},
                (proxy, method, args) -> {
                    if ("setParameter".equals(method.getName()) && args != null && args.length >= 2) {
                        parameters.put(String.valueOf(args[0]), args[1]);
                        return proxy;
                    }
                    if ("executeUpdate".equals(method.getName())) {
                        return 1;
                    }
                    return defaultValue(method.getReturnType());
                }
        );
        return (EntityManager) Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[]{EntityManager.class},
                (proxy, method, args) -> "createNativeQuery".equals(method.getName())
                        ? query
                        : defaultValue(method.getReturnType())
        );
    }

    private void injectEntityManager(
            HyperliquidOriginPositionStoreService service,
            EntityManager entityManager
    ) throws Exception {
        Field field = HyperliquidOriginPositionStoreService.class.getDeclaredField("entityManager");
        field.setAccessible(true);
        field.set(service, entityManager);
    }

    private void awaitCounter(
            SimpleMeterRegistry registry,
            String metric,
            Duration timeout
    ) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            Counter counter = registry.find(metric).counter();
            if (counter != null && counter.count() >= 1.0d) {
                return;
            }
            Thread.sleep(10L);
        }
        Counter counter = registry.find(metric).counter();
        assertNotNull(counter, "timed out waiting for " + metric);
        assertEquals(1.0d, counter.count(), "timed out waiting for " + metric);
    }

    private Object defaultValue(Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == int.class) {
            return 0;
        }
        return null;
    }
}
