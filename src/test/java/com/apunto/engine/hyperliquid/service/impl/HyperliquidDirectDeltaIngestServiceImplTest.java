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

class HyperliquidDirectDeltaIngestServiceImplTest {

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

    private static final class CapturingDispatch implements HyperliquidDirectCopyDispatchService {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public HyperliquidDirectCopyDispatchResult dispatch(HyperliquidMappedDelta mappedDelta) {
            calls.incrementAndGet();
            return HyperliquidDirectCopyDispatchResult.ok(
                    0, 0, 1, 0, false, "flip_without_open_copy");
        }
    }

    private static final class CapturingMovementLedger implements OperationMovementEventService {
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicReference<String> lastReason = new AtomicReference<>();

        @Override
        public void recordAsync(
                HyperliquidMappedDelta mappedDelta,
                HyperliquidDirectCopyDispatchResult dispatchResult,
                String reasonCode
        ) {
            lastReason.set(reasonCode);
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
