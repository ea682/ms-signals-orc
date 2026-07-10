package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.client.BinanceFuturesMarketPriceClientDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.apunto.engine.shared.exception.CopyBinanceClientException;
import com.apunto.engine.shared.exception.CopyDispatchReconciliationPendingException;
import com.apunto.engine.shared.exception.CopyOrderRejectedException;
import com.apunto.engine.shared.exception.SkipExecutionException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDispatchCoordinatorTest {

    @Test
    void sameSourceEventSameAllocationSendsOnce() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled(null));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        coordinator.dispatch(open("evt-1"), allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-1");
        coordinator.dispatch(open("evt-1"), allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-2");

        assertEquals(1, gateway.calls.get());
        assertEquals(1, store.size());
    }

    @Test
    void sameSourceEventDifferentStrategyAllocationsMayEachSendOnce() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        coordinator.dispatch(open("evt-short"), allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-a");
        coordinator.dispatch(open("evt-short"), allocation(506L, "SHORT_ONLY", "MICRO_LIVE"), new BigDecimal("100"), "trace-b");

        assertEquals(2, gateway.calls.get());
        assertEquals(2, store.size());
        assertNotEquals(store.keys().get(0), store.keys().get(1));
    }

    @Test
    void sameClientOrderIdFilledMustNeverBeResent() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled(null));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        BinanceFuturesOrderClientResponse first = coordinator.dispatch(open("evt-2"), allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-a");
        BinanceFuturesOrderClientResponse replay = coordinator.dispatch(open("evt-2"), allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-b");

        assertEquals(first.getOrderId(), replay.getOrderId());
        assertEquals(1, gateway.calls.get());
    }

    @Test
    void duplicateDatabaseInsertBecomesNoop() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        coordinator.dispatch(open("evt-db"), allocation(505L, "MOVEMENT_ALL", "LIVE"), new BigDecimal("100"), "trace-a");
        coordinator.dispatch(open("evt-db"), allocation(505L, "MOVEMENT_ALL", "LIVE"), new BigDecimal("100"), "trace-b");

        assertEquals(1, gateway.calls.get());
    }

    @Test
    void twoReplicasRaceOnlyOneSends() throws Exception {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator firstReplica = coordinator(store, gateway);
        CopyDispatchCoordinator secondReplica = coordinator(store, gateway);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        var one = pool.submit(() -> runAfter(start, () -> firstReplica.dispatch(open("evt-race"), allocation(505L, "MOVEMENT_ALL", "LIVE"), new BigDecimal("100"), "r1")));
        var two = pool.submit(() -> runAfter(start, () -> secondReplica.dispatch(open("evt-race"), allocation(505L, "MOVEMENT_ALL", "LIVE"), new BigDecimal("100"), "r2")));
        start.countDown();
        one.get(2, TimeUnit.SECONDS);
        two.get(2, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertEquals(1, gateway.calls.get());
    }

    @Test
    void transportTimeoutBecomesReconciliationNotRetry() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(new CopyBinanceClientException("timeout"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        assertThrows(CopyDispatchReconciliationPendingException.class,
                () -> coordinator.dispatch(open("evt-timeout"), allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace"));
        assertTrue(store.hasReconciling());
        assertEquals(1, gateway.calls.get());

        assertThrows(CopyDispatchReconciliationPendingException.class,
                () -> coordinator.dispatch(open("evt-timeout"), allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-2"));
        assertEquals(1, gateway.calls.get());
    }

    @Test
    void databaseTimeoutBeforeClaimNeverTouchesBinance() {
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = new CopyDispatchCoordinator(
                new FailingAcquireStore(), gateway, new BinanceOrderExecutionNormalizer(),
                new CopyIdempotencyKeyFactory(), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());

        assertThrows(org.springframework.dao.QueryTimeoutException.class,
                () -> coordinator.dispatch(open("evt-db-timeout"),
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace"));

        assertEquals(0, gateway.calls.get());
    }

    @Test
    void httpFiveHundredAfterPossibleSendNeverRetriesBlindly() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(new CopyBinanceClientException("http 500 after request body"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        assertThrows(CopyDispatchReconciliationPendingException.class,
                () -> coordinator.dispatch(open("evt-http-500"),
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-1"));
        assertThrows(CopyDispatchReconciliationPendingException.class,
                () -> coordinator.dispatch(open("evt-http-500"),
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-2"));

        assertEquals(1, gateway.calls.get());
        assertTrue(store.hasReconciling());
    }

    @Test
    void malformedAckWithoutOrderIdNeverRetriesBlindly() {
        FakeStore store = new FakeStore();
        BinanceFuturesOrderClientResponse malformed = filled("100");
        malformed.setOrderId(null);
        FakeGateway gateway = new FakeGateway(malformed);
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        assertThrows(CopyDispatchReconciliationPendingException.class,
                () -> coordinator.dispatch(open("evt-malformed"),
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-1"));
        assertThrows(CopyDispatchReconciliationPendingException.class,
                () -> coordinator.dispatch(open("evt-malformed"),
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-2"));

        assertEquals(1, gateway.calls.get());
        assertTrue(store.hasReconciling());
    }

    @Test
    void liveFilledWithNullAvgPriceMustNotRetry() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled(null));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        BinanceFuturesOrderClientResponse result = coordinator.dispatch(open("evt-live"), allocation(700L, "LONG_ONLY", "LIVE"), new BigDecimal("100"), "trace");

        assertEquals("FILLED", result.getStatus());
        assertEquals(1, gateway.calls.get());
        assertFalse(store.hasReconciling());
    }

    @Test
    void liveAmbiguousResponseReconcilesBeforeRetry() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(new CopyBinanceClientException("connection reset"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        assertThrows(CopyDispatchReconciliationPendingException.class,
                () -> coordinator.dispatch(open("evt-live-timeout"), allocation(700L, "LONG_ONLY", "LIVE"), new BigDecimal("100"), "trace"));
        assertTrue(store.hasReconciling());
        assertEquals(1, gateway.calls.get());
    }

    @Test
    void definitiveRejectedResponseReleasesReservationWithoutRetry() {
        FakeStore store = new FakeStore();
        BinanceFuturesOrderClientResponse canceled = new BinanceFuturesOrderClientResponse();
        canceled.setOrderId(992L);
        canceled.setClientOrderId("cpO_same");
        canceled.setSymbol("BTCUSDC");
        canceled.setStatus("CANCELED");
        FakeGateway gateway = new FakeGateway(canceled);
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        assertThrows(CopyOrderRejectedException.class,
                () -> coordinator.dispatch(open("evt-rejected"), allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace"));

        assertTrue(store.hasRejected());
        assertFalse(store.hasReconciling());
        assertEquals(1, gateway.calls.get());
    }

    @Test
    void liveSameSourceEventSameAllocationSendsOnce() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        coordinator.dispatch(open("evt-live-dedupe"), allocation(700L, "LONG_ONLY", "LIVE"), new BigDecimal("100"), "trace-1");
        coordinator.dispatch(open("evt-live-dedupe"), allocation(700L, "LONG_ONLY", "LIVE"), new BigDecimal("100"), "trace-2");

        assertEquals(1, gateway.calls.get());
        assertEquals(1, store.size());
    }

    @Test
    void sameIdempotencyKeyWithDifferentPayloadFailsClosed() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto original = open("evt-payload-conflict");
        OperationDto conflicting = open("evt-payload-conflict");
        conflicting.setQuantity("2");
        conflicting.setRequestedNotionalUsd(new BigDecimal("200"));

        coordinator.dispatch(original, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-1");
        SkipExecutionException conflict = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(conflicting, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"),
                        new BigDecimal("100"), "trace-2"));

        assertEquals("COPY_IDEMPOTENCY_PAYLOAD_MISMATCH", conflict.getReasonCode());
        assertEquals(1, gateway.calls.get());
    }

    @Test
    void equivalentNumericFormattingDoesNotCreatePayloadConflict() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto first = open("evt-canonical-payload");
        OperationDto replay = open("evt-canonical-payload");
        replay.setQuantity("1.0");
        replay.setRequestedMarginUsd(new BigDecimal("20.0"));
        replay.setRequestedNotionalUsd(new BigDecimal("100.00"));

        coordinator.dispatch(first, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-1");
        coordinator.dispatch(replay, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100.0"), "trace-2");

        assertEquals(1, gateway.calls.get());
    }

    @Test
    void openWithoutPositiveMarginRejectsBeforeClaimAndGateway() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto operation = open("evt-zero-margin");
        operation.setRequestedMarginUsd(BigDecimal.ZERO);
        operation.setLeverage(null);

        SkipExecutionException rejected = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(operation, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"),
                        new BigDecimal("100"), "trace"));

        assertEquals("COPY_POSITIVE_MARGIN_REQUIRED", rejected.getReasonCode());
        assertEquals(0, store.size());
        assertEquals(0, gateway.calls.get());
    }

    @Test
    void shadowAllocationNeverReachesDurableRealDispatch() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        SkipExecutionException rejected = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(open("evt-shadow"),
                        allocation(505L, "MOVEMENT_ALL", "SHADOW"), new BigDecimal("100"), "trace"));

        assertEquals("COPY_REAL_EXECUTION_MODE_REQUIRED", rejected.getReasonCode());
        assertEquals(0, store.size());
        assertEquals(0, gateway.calls.get());
    }

    @Test
    void realDispatchRequiresExactAllocationBeforeClaim() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);

        SkipExecutionException rejected = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(open("evt-no-allocation"), null, new BigDecimal("100"), "trace"));

        assertEquals("COPY_REAL_ALLOCATION_REQUIRED", rejected.getReasonCode());
        assertEquals(0, store.size());
        assertEquals(0, gateway.calls.get());
    }

    @Test
    void nonPositiveOrMalformedQuantityRejectsBeforeClaimAndGateway() {
        for (String quantity : List.of("0", "-1", "not-a-number")) {
            FakeStore store = new FakeStore();
            FakeGateway gateway = new FakeGateway(filled("100"));
            CopyDispatchCoordinator coordinator = coordinator(store, gateway);
            OperationDto operation = open("evt-invalid-qty-" + quantity);
            operation.setQuantity(quantity);

            SkipExecutionException rejected = assertThrows(SkipExecutionException.class,
                    () -> coordinator.dispatch(operation,
                            allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"),
                            new BigDecimal("100"), "trace"));

            assertEquals("COPY_POSITIVE_QUANTITY_REQUIRED", rejected.getReasonCode());
            assertEquals(0, store.size());
            assertEquals(0, gateway.calls.get());
        }
    }

    @Test
    void immutableSourceIdentityIsRequiredBeforeClaim() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto operation = open(null);
        operation.setClientOrderId(null);
        operation.setOriginId(null);

        SkipExecutionException rejected = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(operation,
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"),
                        new BigDecimal("100"), "trace"));

        assertEquals("COPY_IMMUTABLE_SOURCE_ID_REQUIRED", rejected.getReasonCode());
        assertEquals(0, store.size());
        assertEquals(0, gateway.calls.get());
    }

    @Test
    void persistedIntentReplayIsControlledNoopAndDoesNotReapplyLocally() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto operation = open("evt-persisted-noop");

        BinanceFuturesOrderClientResponse first = coordinator.dispatch(
                operation, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-1");
        store.markPersisted(first.getDispatchIntentId(), first.getClientOrderId(), UUID.randomUUID());

        SkipExecutionException replay = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(operation, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"),
                        new BigDecimal("100"), "trace-2"));

        assertEquals("COPY_DISPATCH_ALREADY_APPLIED_NOOP", replay.getReasonCode());
        assertEquals(1, gateway.calls.get());
    }

    @Test
    void liveTwoReplicasOnlyOneSends() throws Exception {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator firstReplica = coordinator(store, gateway);
        CopyDispatchCoordinator secondReplica = coordinator(store, gateway);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        var one = pool.submit(() -> runAfter(start, () -> firstReplica.dispatch(
                open("evt-live-race"), allocation(700L, "LONG_ONLY", "LIVE"), new BigDecimal("100"), "r1")));
        var two = pool.submit(() -> runAfter(start, () -> secondReplica.dispatch(
                open("evt-live-race"), allocation(700L, "LONG_ONLY", "LIVE"), new BigDecimal("100"), "r2")));
        start.countDown();
        one.get(2, TimeUnit.SECONDS);
        two.get(2, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertEquals(1, gateway.calls.get());
    }

    @Test
    void liveOpenPersistenceFailureDoesNotResend() {
        assertLivePersistenceFailureDoesNotResend("OPEN", false, true, true);
    }

    @Test
    void liveIncreasePersistenceFailureDoesNotResend() {
        assertLivePersistenceFailureDoesNotResend("INCREASE", false, false, false);
    }

    @Test
    void liveReducePersistenceFailureDoesNotResend() {
        assertLivePersistenceFailureDoesNotResend("REDUCE", true, false, false);
    }

    @Test
    void liveClosePersistenceFailureDoesNotResend() {
        assertLivePersistenceFailureDoesNotResend("CLOSE", true, false, false);
    }

    private void assertLivePersistenceFailureDoesNotResend(String copyIntent,
                                                            boolean reduceOnly,
                                                            boolean configureAccountSettings,
                                                            boolean reservePosition) {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled(null));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto operation = open("evt-live-persist-" + copyIntent.toLowerCase());
        operation.setCopyIntent(copyIntent);
        operation.setReduceOnly(reduceOnly);
        operation.setConfigureAccountSettings(configureAccountSettings);
        operation.setReservePosition(reservePosition);
        operation.setRequestedMarginUsd(reduceOnly ? BigDecimal.ZERO : new BigDecimal("20"));

        BinanceFuturesOrderClientResponse response = coordinator.dispatch(
                operation, allocation(700L, "LONG_ONLY", "LIVE"), new BigDecimal("100"), "trace-1");
        coordinator.markPersistencePending(response.getDispatchIntentId(), response.getClientOrderId(),
                "COPY_OPERATION_PERSIST_FAILED", "db unavailable");
        coordinator.dispatch(operation, allocation(700L, "LONG_ONLY", "LIVE"), new BigDecimal("100"), "trace-2");

        assertTrue(store.hasPersistencePending());
        assertEquals(1, gateway.calls.get());
    }

    @Test
    void persistenceFailureAfterFillMovesToPersistencePending() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled(null));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto operation = open("evt-persist");

        coordinator.dispatch(operation, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace");
        coordinator.markPersistencePending(operation.getClientOrderId(), "COPY_OPERATION_INSERT_FAILED", "db unavailable");

        assertTrue(store.hasPersistencePending());
        coordinator.dispatch(operation, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-2");
        assertEquals(1, gateway.calls.get());
    }

    private CopyDispatchCoordinator coordinator(FakeStore store, FakeGateway gateway) {
        return new CopyDispatchCoordinator(store, gateway, new BinanceOrderExecutionNormalizer(),
                new CopyIdempotencyKeyFactory(), new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

    private OperationDto open(String sourceEventId) {
        return OperationDto.builder()
                .symbol("BTCUSDC")
                .side(Side.BUY)
                .type(OrderType.MARKET)
                .positionSide(PositionSide.LONG)
                .quantity("1")
                .leverage(5)
                .reduceOnly(false)
                .configureAccountSettings(true)
                .clientOrderId("cpO_same")
                .originId("origin-1")
                .sourceEventId(sourceEventId)
                .copyIntent("OPEN")
                .requestedMarginUsd(new BigDecimal("20"))
                .requestedNotionalUsd(new BigDecimal("100"))
                .reservePosition(true)
                .userId("user-1")
                .walletId("0xabc")
                .apiKey("test-key")
                .secret("test-secret")
                .build();
    }

    private UserCopyAllocationEntity allocation(Long id, String strategy, String mode) {
        return UserCopyAllocationEntity.builder()
                .id(id)
                .copyStrategyCode(strategy)
                .scopeType("DIRECTION")
                .scopeValue("LONG")
                .executionMode(mode)
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .build();
    }

    private static BinanceFuturesOrderClientResponse filled(String avgPrice) {
        BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
        response.setOrderId(991L);
        response.setClientOrderId("cpO_same");
        response.setSymbol("BTCUSDC");
        response.setStatus("FILLED");
        response.setExecutedQty(BigDecimal.ONE);
        response.setAvgPrice(avgPrice == null ? null : new BigDecimal(avgPrice));
        return response;
    }

    private static void runAfter(CountDownLatch start, Runnable action) {
        try {
            start.await(1, TimeUnit.SECONDS);
            action.run();
        } catch (CopyDispatchReconciliationPendingException expectedDuplicateRace) {
            // A losing replica must reconcile/no-op; it must never send.
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
        }
    }

    private static final class FakeGateway implements ProcesBinanceService {
        private final AtomicInteger calls = new AtomicInteger();
        private final BinanceFuturesOrderClientResponse response;
        private final RuntimeException failure;

        private FakeGateway(BinanceFuturesOrderClientResponse response) {
            this.response = response;
            this.failure = null;
        }

        private FakeGateway(RuntimeException failure) {
            this.response = null;
            this.failure = failure;
        }

        @Override
        public BinanceFuturesOrderClientResponse operationPosition(OperationDto dto) {
            calls.incrementAndGet();
            if (failure != null) throw failure;
            return response;
        }

        @Override public Optional<BinanceFuturesOrderClientResponse> findOrderByClientOrderId(OperationDto dto) { return Optional.ofNullable(response); }
        @Override public List<BinanceFuturesSymbolInfoClientDto> getSymbols(String apiKey) { return List.of(); }
        @Override public Optional<BinanceFuturesMarketPriceClientDto> getMarketPrice(String symbol, String usage, boolean allowStale) { return Optional.empty(); }
    }

    private static final class FakeStore implements CopyDispatchIntentStore {
        private final Map<String, UUID> ids = new ConcurrentHashMap<>();
        private final Map<UUID, BinanceFuturesOrderClientResponse> responses = new ConcurrentHashMap<>();
        private final Map<UUID, String> statuses = new ConcurrentHashMap<>();
        private final Map<UUID, String> requestHashes = new ConcurrentHashMap<>();

        @Override
        public CopyDispatchPermit acquire(CopyDispatchRequest request) {
            AtomicBoolean created = new AtomicBoolean(false);
            UUID id = ids.computeIfAbsent(request.idempotencyKey(), ignored -> {
                created.set(true);
                return UUID.randomUUID();
            });
            if (created.get()) {
                statuses.put(id, "DISPATCHING");
                requestHashes.put(id, request.requestHash());
                return CopyDispatchPermit.send(id);
            }
            if (!Objects.equals(requestHashes.get(id), request.requestHash()))
                return CopyDispatchPermit.conflict(id, statuses.get(id));
            BinanceFuturesOrderClientResponse known = responses.get(id);
            if ("PERSISTED".equals(statuses.get(id)))
                return CopyDispatchPermit.noop(id, "PERSISTED");
            if (known != null) return CopyDispatchPermit.reuse(id, known, statuses.get(id));
            return CopyDispatchPermit.reconcile(id, statuses.get(id));
        }

        @Override
        public void acknowledge(UUID intentId, NormalizedBinanceExecution execution, BinanceFuturesOrderClientResponse response) {
            responses.put(intentId, response);
            statuses.put(intentId, execution.executionState().name());
        }

        @Override public void markAmbiguous(UUID intentId, String reasonCode, String detail) { statuses.put(intentId, "RECONCILING"); }
        @Override public void markRejected(UUID intentId, String reasonCode, String detail) { statuses.put(intentId, "REJECTED"); }

        @Override
        public void markPersistencePending(String clientOrderId, String reasonCode, String detail) {
            ids.values().forEach(id -> statuses.computeIfPresent(id, (ignored, status) -> "PERSISTENCE_PENDING"));
        }

        @Override
        public void markPersisted(UUID intentId, String clientOrderId, UUID copyOperationId) {
            statuses.put(intentId, "PERSISTED");
        }

        int size() { return ids.size(); }
        List<String> keys() { return ids.keySet().stream().sorted().toList(); }
        boolean hasReconciling() { return statuses.containsValue("RECONCILING"); }
        boolean hasPersistencePending() { return statuses.containsValue("PERSISTENCE_PENDING"); }
        boolean hasRejected() { return statuses.containsValue("REJECTED"); }
    }

    @Test
    void blankSymbolRejectsBeforeClaimAndGateway() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto operation = open("evt-blank-symbol");
        operation.setSymbol("  ");

        SkipExecutionException rejected = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(operation,
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace"));

        assertEquals("COPY_SYMBOL_REQUIRED", rejected.getReasonCode());
        assertEquals(0, store.size());
        assertEquals(0, gateway.calls.get());
    }

    @Test
    void restartDerivesSameClientOrderIdFromDurableIdentity() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto first = open("evt-derived-client-id");
        OperationDto replay = open("evt-derived-client-id");
        first.setClientOrderId(null);
        replay.setClientOrderId(null);

        coordinator.dispatch(first, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-1");
        coordinator.dispatch(replay, allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace-2");

        assertEquals(first.getClientOrderId(), replay.getClientOrderId());
        assertTrue(first.getClientOrderId().matches("[A-Za-z0-9._-]{1,36}"));
        assertEquals(1, gateway.calls.get());
    }

    @Test
    void binanceEngineTimerClassifiesTransportFailureAsError() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(new CopyBinanceClientException("connection reset"));
        var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        CopyDispatchCoordinator coordinator = new CopyDispatchCoordinator(
                store, gateway, new BinanceOrderExecutionNormalizer(), new CopyIdempotencyKeyFactory(), registry);

        assertThrows(CopyDispatchReconciliationPendingException.class,
                () -> coordinator.dispatch(open("evt-error-timer"),
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace"));

        assertEquals(1L, registry.find("copy_binance_engine_duration")
                .tag("result", "error").timer().count());
        assertTrue(registry.find("copy_binance_engine_duration")
                .tag("result", "completed").timer() == null);
    }

    @Test
    void invalidClientOrderIdRejectsBeforeClaimAndGateway() {
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway(filled("100"));
        CopyDispatchCoordinator coordinator = coordinator(store, gateway);
        OperationDto operation = open("evt-invalid-client-id");
        operation.setClientOrderId("invalid client id with spaces");

        SkipExecutionException rejected = assertThrows(SkipExecutionException.class,
                () -> coordinator.dispatch(operation,
                        allocation(505L, "MOVEMENT_ALL", "MICRO_LIVE"), new BigDecimal("100"), "trace"));

        assertEquals("COPY_CLIENT_ORDER_ID_INVALID", rejected.getReasonCode());
        assertEquals(0, store.size());
        assertEquals(0, gateway.calls.get());
    }

    private static final class FailingAcquireStore implements CopyDispatchIntentStore {
        @Override public CopyDispatchPermit acquire(CopyDispatchRequest request) {
            throw new org.springframework.dao.QueryTimeoutException("simulated intent insert timeout");
        }
        @Override public void acknowledge(UUID id, NormalizedBinanceExecution execution, BinanceFuturesOrderClientResponse response) { }
        @Override public void markAmbiguous(UUID id, String code, String detail) { }
        @Override public void markRejected(UUID id, String code, String detail) { }
        @Override public void markPersistencePending(String clientOrderId, String code, String detail) { }
    }
}
