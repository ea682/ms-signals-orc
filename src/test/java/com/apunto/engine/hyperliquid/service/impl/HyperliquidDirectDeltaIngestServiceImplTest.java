package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectCopyDispatchService;
import com.apunto.engine.repository.FuturesPositionRepository;
import com.apunto.engine.service.OperationMovementEventService;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperliquidDirectDeltaIngestServiceImplTest {

    @Test
    void httpThenKafkaWithSameSourceIdentityProducesOneCanonicalDispatch() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestProperties properties = new HyperliquidDirectIngestProperties();
        properties.setEnabled(true);
        properties.setWorkerThreads(1);
        properties.setQueueCapacity(16);
        properties.setDedupeEnabled(true);
        properties.setDedupeTtlSeconds(60);
        properties.setDistributedDedupeEnabled(false);

        CapturingDispatch dispatch = new CapturingDispatch();
        CapturingMovementLedger movementLedger = new CapturingMovementLedger();
        CapturingShadow shadow = new CapturingShadow();
        HyperliquidOriginPositionStoreService originStore = originStore(registry);
        HyperliquidDirectDeltaIngestServiceImpl service = new HyperliquidDirectDeltaIngestServiceImpl(
                properties,
                dispatch,
                new HyperliquidDirectIngestIdempotencyGuard(properties, new JdbcTemplate(), registry),
                originStore,
                movementLedger,
                shadow,
                registry,
                false,
                16,
                1,
                0L,
                100L
        );
        service.start();
        try {
            HyperliquidMappedDelta sameMovement =
                    mappedAdjustment("same-source-movement", "RESIZE", "100", "4210.60", 1778905103699L);

            var direct = service.accept(sameMovement);
            var kafkaReplay = service.accept(sameMovement);
            awaitAtLeast(movementLedger.calls, 1);

            assertFalse(direct.duplicate());
            assertTrue(kafkaReplay.duplicate());
            assertEquals(1, dispatch.calls.get());
            assertEquals(1, movementLedger.calls.get());
            assertEquals(1.0d, registry.find("signals.hyperliquid.direct_ingest.duplicates.total")
                    .counter().count());
            awaitCounter(
                    registry,
                    "signals.hyperliquid.direct_ingest.processed.total",
                    "deltaType",
                    "RESIZE",
                    1.0d);
        } finally {
            service.stop();
            originStore.stop();
        }
    }

    @Test
    void estimatedProductionFlipIsBlockedBeforeShadowAndLive() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestProperties properties = new HyperliquidDirectIngestProperties();
        properties.setEnabled(true);
        properties.setWorkerThreads(1);
        properties.setQueueCapacity(16);
        properties.setDedupeEnabled(false);
        properties.setDistributedDedupeEnabled(false);

        CapturingDispatch dispatch = new CapturingDispatch();
        CapturingMovementLedger movementLedger = new CapturingMovementLedger();
        CapturingShadow shadow = new CapturingShadow();
        HyperliquidOriginPositionStoreService originStore = originStore(registry);
        HyperliquidDirectDeltaIngestServiceImpl service = new HyperliquidDirectDeltaIngestServiceImpl(
                properties,
                dispatch,
                new HyperliquidDirectIngestIdempotencyGuard(properties, new JdbcTemplate(), registry),
                originStore,
                movementLedger,
                shadow,
                registry,
                false,
                16,
                1,
                0L,
                100L
        );
        service.start();
        try {
            service.accept(productionIncompleteFlip());
            awaitAtLeast(movementLedger.calls, 1);

            assertEquals(0, dispatch.calls.get(),
                    "an estimated POSITION_DELTA flip must not reach LIVE dispatch");
            assertEquals(0, shadow.calls.get(),
                    "an estimated POSITION_DELTA flip must not create synthetic SHADOW legs");
            assertEquals("FLIP_EXECUTION_BASIS_MISSING", movementLedger.lastReason.get());
        } finally {
            service.stop();
            originStore.stop();
        }
    }

    @Test
    void authoritativeUserFillSatisfiesFutureFlipExecutionContract() throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        try (InputStream fixture = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-d-incomplete-flip.json")) {
            assertNotNull(fixture);
            JsonNode root = mapper.readTree(fixture);
            var requestNode = (com.fasterxml.jackson.databind.node.ObjectNode) root.get("request");
            requestNode.put("economicEventKind", "USER_FILL");
            requestNode.put("sourceEstimated", false);
            HyperliquidDeltaRequest request = mapper.treeToValue(
                    requestNode, HyperliquidDeltaRequest.class);
            HyperliquidMappedDelta mapped = new HyperliquidDeltaOperacionMapper()
                    .map(request, request.idempotencyKey());

            HyperliquidFlipExecutionBasisPolicy.Decision decision =
                    new HyperliquidFlipExecutionBasisPolicy().evaluate(mapped);

            assertFalse(decision.flip() && !decision.allowed());
            assertEquals("authoritative_user_fill", decision.reason());
        }
    }

    @Test
    void historicalReplayIsAuditedWithoutShadowOrLiveDispatch() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestProperties properties = new HyperliquidDirectIngestProperties();
        properties.setEnabled(true);
        properties.setWorkerThreads(1);
        properties.setQueueCapacity(16);
        properties.setDedupeEnabled(false);
        properties.setDistributedDedupeEnabled(false);

        CapturingDispatch dispatch = new CapturingDispatch();
        CapturingMovementLedger movementLedger = new CapturingMovementLedger();
        CapturingShadow shadow = new CapturingShadow();
        HyperliquidOriginPositionStoreService originStore = originStore(registry);
        HyperliquidDirectDeltaIngestServiceImpl service = new HyperliquidDirectDeltaIngestServiceImpl(
                properties,
                dispatch,
                new HyperliquidDirectIngestIdempotencyGuard(properties, new JdbcTemplate(), registry),
                originStore,
                movementLedger,
                shadow,
                registry,
                false,
                16,
                1,
                0L,
                100L);
        service.start();
        try {
            service.accept(historicalReplay());

            assertEquals(0, dispatch.calls.get());
            assertEquals(0, shadow.calls.get());
            assertEquals(1, movementLedger.durableCalls.get());
            assertEquals(0, movementLedger.asyncCalls.get());
            assertEquals("HISTORICAL_REPLAY_AUDIT_ONLY", movementLedger.lastReason.get());
            assertEquals(1.0d, registry.get(
                            "signals.hyperliquid.user_fill.durable.total")
                    .tag("deliveryMode", "HISTORICAL_REPLAY")
                    .counter().count());
        } finally {
            service.stop();
            originStore.stop();
        }
    }

    @Test
    void liveAuthoritativeFillIsDurableBeforeAcceptReturns()
            throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestProperties properties =
                new HyperliquidDirectIngestProperties();
        properties.setEnabled(true);
        properties.setWorkerThreads(1);
        properties.setQueueCapacity(16);
        properties.setDedupeEnabled(false);
        properties.setDistributedDedupeEnabled(false);
        CapturingDispatch dispatch = new CapturingDispatch();
        CapturingMovementLedger movementLedger =
                new CapturingMovementLedger();
        HyperliquidOriginPositionStoreService originStore =
                originStore(registry);
        HyperliquidDirectDeltaIngestServiceImpl service =
                new HyperliquidDirectDeltaIngestServiceImpl(
                        properties,
                        dispatch,
                        new HyperliquidDirectIngestIdempotencyGuard(
                                properties, new JdbcTemplate(), registry),
                        originStore,
                        movementLedger,
                        new CapturingShadow(),
                        registry,
                        false,
                        16,
                        1,
                        0L,
                        100L);
        service.start();
        try {
            service.accept(authoritativeUserFill("LIVE_USER_FILL"));

            assertEquals(1, dispatch.calls.get());
            assertEquals(1, movementLedger.durableCalls.get());
            assertEquals(0, movementLedger.asyncCalls.get());
            assertEquals(1.0d, registry.get(
                            "signals.hyperliquid.user_fill.durable.total")
                    .tag("deliveryMode", "LIVE_USER_FILL")
                    .counter().count());
            assertEquals(1.0d, registry.get(
                            "signals.hyperliquid.user_fill.ingress.total")
                    .tag("deliveryMode", "LIVE_USER_FILL")
                    .counter().count());
        } finally {
            service.stop();
            originStore.stop();
        }
    }

    @Test
    void recentAuthoritativeFillCannotBypassDurablePayloadConflictGuard()
            throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestProperties properties =
                new HyperliquidDirectIngestProperties();
        properties.setEnabled(true);
        properties.setWorkerThreads(1);
        properties.setQueueCapacity(16);
        properties.setDedupeEnabled(true);
        properties.setDedupeTtlSeconds(60);
        properties.setDistributedDedupeEnabled(true);
        SecondAcquireConflictGuard guard =
                new SecondAcquireConflictGuard(properties, registry);
        HyperliquidOriginPositionStoreService originStore =
                originStore(registry);
        HyperliquidDirectDeltaIngestServiceImpl service =
                new HyperliquidDirectDeltaIngestServiceImpl(
                        properties,
                        new CapturingDispatch(),
                        guard,
                        originStore,
                        new CapturingMovementLedger(),
                        new CapturingShadow(),
                        registry,
                        false,
                        16,
                        1,
                        0L,
                        100L);
        service.start();
        try {
            HyperliquidMappedDelta original =
                    authoritativeUserFill("LIVE_USER_FILL");
            ObjectMapper mapper =
                    new ObjectMapper().findAndRegisterModules();
            var contradictoryNode =
                    (com.fasterxml.jackson.databind.node.ObjectNode)
                            mapper.valueToTree(original.request());
            contradictoryNode.put("notionalUsd", 999.99d);
            contradictoryNode.put(
                    "economicFingerprint", "contradictory-fingerprint");
            HyperliquidDeltaRequest contradictoryRequest =
                    mapper.treeToValue(
                            contradictoryNode,
                            HyperliquidDeltaRequest.class);
            HyperliquidMappedDelta contradictory =
                    new HyperliquidDeltaOperacionMapper().map(
                            contradictoryRequest,
                            original.idempotencyKey());

            service.accept(original);

            assertThrows(
                    IllegalStateException.class,
                    () -> service.accept(contradictory));
            assertEquals(2, guard.acquireCalls.get(),
                    "every authoritative USER_FILL must reach the durable "
                            + "fingerprint guard even while its key is recent");
        } finally {
            service.stop();
            originStore.stop();
        }
    }

    @Test
    void authoritativeDurabilityFailurePreventsSuccessfulAccept()
            throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestProperties properties =
                new HyperliquidDirectIngestProperties();
        properties.setEnabled(true);
        properties.setWorkerThreads(1);
        properties.setQueueCapacity(16);
        properties.setDedupeEnabled(false);
        properties.setDistributedDedupeEnabled(false);
        CapturingMovementLedger movementLedger =
                new CapturingMovementLedger();
        movementLedger.failDurable = true;
        HyperliquidOriginPositionStoreService originStore =
                originStore(registry);
        HyperliquidDirectDeltaIngestServiceImpl service =
                new HyperliquidDirectDeltaIngestServiceImpl(
                        properties,
                        new CapturingDispatch(),
                        new HyperliquidDirectIngestIdempotencyGuard(
                                properties, new JdbcTemplate(), registry),
                        originStore,
                        movementLedger,
                        new CapturingShadow(),
                        registry,
                        false,
                        16,
                        1,
                        0L,
                        100L);
        service.start();
        try {
            assertThrows(
                    IllegalStateException.class,
                    () -> service.accept(
                            authoritativeUserFill("LIVE_USER_FILL")));
            assertEquals(1, movementLedger.durableCalls.get());
            assertEquals(0, movementLedger.asyncCalls.get());
        } finally {
            service.stop();
            originStore.stop();
        }
    }

    @Test
    void adjustmentDedupeKeyIgnoresNoisyIdempotencyKeyWhenSourceStateIsTheSame() {
        HyperliquidMappedDelta first = mappedAdjustment("idempotency-a", "RESIZE", "100.0000", "4210.600", 1778905103699L);
        HyperliquidMappedDelta second = mappedAdjustment("idempotency-b", "RESIZE", "100.0", "4210.6000", 1778905103699L);

        assertEquals(
                HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(first),
                HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(second)
        );
    }

    @Test
    void adjustmentDedupeKeyChangesWhenResultingPositionStateChanges() {
        HyperliquidMappedDelta first = mappedAdjustment("idempotency-a", "RESIZE", "100", "4210.60", 1778905103699L);
        HyperliquidMappedDelta second = mappedAdjustment("idempotency-b", "RESIZE", "101", "4210.60", 1778905103699L);

        assertNotEquals(
                HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(first),
                HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(second)
        );
    }

    @Test
    void openDedupeKeyKeepsPublisherIdempotencyKey() {
        HyperliquidMappedDelta open = mappedAdjustment("open-idempotency", "OPEN", "100", "4210.60", 1778905103699L);

        assertEquals("open-idempotency", HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(open));
    }

    private HyperliquidMappedDelta mappedAdjustment(
            String idempotencyKey,
            String deltaType,
            String sizeQty,
            String notionalUsd,
            long sourceTs
    ) {
        HyperliquidDeltaRequest request = new HyperliquidDeltaRequest(
                null,
                idempotencyKey,
                null,
                deltaType,
                "hyperliquid",
                "0xabc",
                null,
                "HYPE",
                "SHORT",
                "OPEN",
                new BigDecimal(sizeQty),
                null,
                new BigDecimal(notionalUsd),
                BigDecimal.TEN,
                new BigDecimal("42.106"),
                new BigDecimal("42.1060"),
                null,
                new BigDecimal(notionalUsd),
                new BigDecimal(notionalUsd),
                null,
                null,
                null,
                new BigDecimal("42.106"),
                new BigDecimal("42.1060"),
                null,
                "NOT_CLOSING",
                "test_payload",
                sourceTs,
                null,
                null,
                null,
                null,
                null,
                null,
                false
        );
        return new HyperliquidMappedDelta(
                idempotencyKey,
                "hyperliquid-position:0xabc:HYPEUSDT:SHORT",
                "0xabc",
                "HYPEUSDT",
                "SHORT",
                deltaType,
                new OperacionEvent(OperacionEvent.Tipo.ABIERTA, null, deltaType),
                request
        );
    }

    private HyperliquidMappedDelta productionIncompleteFlip() throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        try (InputStream fixture = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-d-incomplete-flip.json")) {
            assertNotNull(fixture);
            JsonNode root = mapper.readTree(fixture);
            HyperliquidDeltaRequest request = mapper.treeToValue(
                    root.get("request"), HyperliquidDeltaRequest.class);
            return new HyperliquidDeltaOperacionMapper().map(request, request.idempotencyKey());
        }
    }

    private HyperliquidMappedDelta historicalReplay() throws Exception {
        return authoritativeUserFill("HISTORICAL_REPLAY");
    }

    private HyperliquidMappedDelta authoritativeUserFill(
            String deliveryMode
    ) throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        try (InputStream fixture = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-d-incomplete-flip.json")) {
            assertNotNull(fixture);
            JsonNode root = mapper.readTree(fixture);
            var requestNode = (com.fasterxml.jackson.databind.node.ObjectNode) root.get("request");
            requestNode.put("eventType", "HYPERLIQUID_POSITION_OPENED");
            requestNode.put("deltaType", "OPEN");
            requestNode.put("economicEventKind", "USER_FILL");
            requestNode.put("sourceEstimated", false);
            requestNode.put("sourceDeliveryMode", deliveryMode);
            requestNode.put("sourcePreviousPositionQuantity", 0);
            requestNode.put("sourceResultingPositionQuantity", 84.28);
            requestNode.put("sourceExecutionQuantity", 84.28);
            requestNode.put("sourceSignedExecutionQuantity", 84.28);
            HyperliquidDeltaRequest request = mapper.treeToValue(
                    requestNode, HyperliquidDeltaRequest.class);
            return new HyperliquidDeltaOperacionMapper().map(request, request.idempotencyKey());
        }
    }

    private HyperliquidOriginPositionStoreService originStore(SimpleMeterRegistry registry) {
        FuturesPositionRepository repository = (FuturesPositionRepository) Proxy.newProxyInstance(
                FuturesPositionRepository.class.getClassLoader(),
                new Class<?>[]{FuturesPositionRepository.class},
                (proxy, method, args) -> {
                    if (method.getReturnType() == Optional.class) return Optional.empty();
                    if (method.getReturnType() == List.class) return List.of();
                    if (method.getReturnType() == boolean.class) return false;
                    if (method.getReturnType() == long.class) return 0L;
                    if (method.getReturnType() == int.class) return 0;
                    return null;
                }
        );
        return new HyperliquidOriginPositionStoreService(
                repository,
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

    private void awaitAtLeast(AtomicInteger value, int expected) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(3);
        while (Instant.now().isBefore(deadline) && value.get() < expected) {
            Thread.sleep(10L);
        }
        assertEquals(expected, value.get(), "timed out waiting for asynchronous ingest");
    }

    private void awaitCounter(
            SimpleMeterRegistry registry,
            String name,
            String tag,
            String tagValue,
            double expected
    ) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(3);
        io.micrometer.core.instrument.Counter counter = null;
        while (Instant.now().isBefore(deadline)) {
            counter = registry.find(name).tag(tag, tagValue).counter();
            if (counter != null && counter.count() >= expected) {
                break;
            }
            Thread.sleep(10L);
        }
        assertNotNull(counter, "timed out waiting for counter " + name);
        assertEquals(expected, counter.count(), "unexpected counter " + name);
    }

    private static final class CapturingDispatch implements HyperliquidDirectCopyDispatchService {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public HyperliquidDirectCopyDispatchResult dispatch(HyperliquidMappedDelta mappedDelta) {
            calls.incrementAndGet();
            return HyperliquidDirectCopyDispatchResult.ok(
                    0, 0, 1, 0, false, "flip_without_open_copy");
        }
    }

    private static final class SecondAcquireConflictGuard
            extends HyperliquidDirectIngestIdempotencyGuard {
        private final AtomicInteger acquireCalls = new AtomicInteger();

        private SecondAcquireConflictGuard(
                HyperliquidDirectIngestProperties properties,
                SimpleMeterRegistry registry
        ) {
            super(properties, new JdbcTemplate(), registry);
        }

        @Override
        public AcquireDecision acquire(
                HyperliquidMappedDelta mappedDelta,
                String dedupeKey
        ) {
            if (acquireCalls.incrementAndGet() == 1) {
                return AcquireDecision.ACQUIRED;
            }
            throw new IllegalStateException(
                    "simulated durable payload conflict");
        }

        @Override
        public void markProcessed(
                HyperliquidMappedDelta mappedDelta,
                String reasonCode
        ) {
            // The test isolates acquire routing from persistence details.
        }
    }

    private static final class CapturingMovementLedger implements OperationMovementEventService {
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger durableCalls = new AtomicInteger();
        private final AtomicInteger asyncCalls = new AtomicInteger();
        private final AtomicReference<String> lastReason = new AtomicReference<>();
        private volatile boolean failDurable;

        @Override
        public void recordDurably(
                HyperliquidMappedDelta mappedDelta,
                HyperliquidDirectCopyDispatchResult dispatchResult,
                String reasonCode
        ) {
            lastReason.set(reasonCode);
            durableCalls.incrementAndGet();
            calls.incrementAndGet();
            if (failDurable) {
                throw new IllegalStateException(
                        "simulated durable movement failure");
            }
        }

        @Override
        public void recordAsync(
                HyperliquidMappedDelta mappedDelta,
                HyperliquidDirectCopyDispatchResult dispatchResult,
                String reasonCode
        ) {
            lastReason.set(reasonCode);
            asyncCalls.incrementAndGet();
            calls.incrementAndGet();
        }

        @Override
        public void recordAsync(
                OperacionEvent event,
                String source,
                String traceId,
                String reasonCode
        ) {
        }
    }

    private static final class CapturingShadow implements ShadowCopyTradingService {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public void syncShadowAllocations(
                UUID idUser,
                List<MetricaWalletDto> candidates,
                int userMaxWallet,
                OffsetDateTime now
        ) {
        }

        @Override
        public void linkLiveAllocations(UUID idUser, List<UserCopyAllocationEntity> liveAllocations) {
        }

        @Override
        public int recordShadowEvent(OperacionEvent event) {
            calls.incrementAndGet();
            return 1;
        }

        @Override
        public boolean isSeparateShadowEnabled() {
            return true;
        }

        @Override
        public boolean isLivePromotable(UUID idUser, MetricaWalletDto candidate) {
            return false;
        }
    }
}
