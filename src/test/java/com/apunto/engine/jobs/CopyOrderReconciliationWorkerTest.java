package com.apunto.engine.jobs;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesMarketPriceClientDto;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.service.ProcesBinanceService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.service.copy.dispatch.BinanceOrderExecutionNormalizer;
import com.apunto.engine.service.copy.dispatch.CopyDispatchIntentStore;
import com.apunto.engine.service.copy.dispatch.CopyDispatchPermit;
import com.apunto.engine.service.copy.dispatch.CopyDispatchRequest;
import com.apunto.engine.service.copy.dispatch.CopyExecutionPersistenceService;
import com.apunto.engine.service.copy.dispatch.CopyOrderReconciliationService;
import com.apunto.engine.service.copy.dispatch.NormalizedBinanceExecution;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CopyOrderReconciliationWorkerTest {

    @Test
    void restartReconcilesUnfinishedIntentWithoutSendingNewOrder() {
        FakeReconciliation claims = new FakeReconciliation(List.of(intent("good")));
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway();
        FakePersistence persistence = new FakePersistence();
        CopyOrderReconciliationWorker worker = worker(claims, store, gateway, persistence);

        assertEquals(1, worker.runOnce());
        assertEquals(0, gateway.sendCalls.get());
        assertEquals(1, gateway.lookupCalls.get());
        assertEquals(1, persistence.persisted.get());
    }

    @Test
    void reconciliationBatchContinuesWhenOneItemFails() {
        FakeReconciliation claims = new FakeReconciliation(List.of(intent("bad"), intent("good")));
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway();
        FakePersistence persistence = new FakePersistence();
        CopyOrderReconciliationWorker worker = worker(claims, store, gateway, persistence);

        assertEquals(1, worker.runOnce());
        assertEquals(1, claims.failures.get());
        assertEquals(1, persistence.persisted.get());
        assertEquals(0, gateway.sendCalls.get());
    }

    @Test
    void binanceOrderIdLookupIsPreferredBeforeClientOrderId() {
        CopyDispatchIntentEntity pending = intent("order-first");
        pending.setBinanceOrderId(77L);
        FakeReconciliation claims = new FakeReconciliation(List.of(pending));
        FakeGateway gateway = new FakeGateway();
        FakePersistence persistence = new FakePersistence();

        assertEquals(1, worker(claims, new FakeStore(), gateway, persistence).runOnce());

        assertEquals(1, gateway.orderIdLookupCalls.get());
        assertEquals(0, gateway.lookupCalls.get());
        assertEquals(1, persistence.persisted.get());
        assertEquals(0, gateway.sendCalls.get());
    }

    @Test
    void definitiveCanceledWithoutFillReleasesReservation() {
        FakeReconciliation claims = new FakeReconciliation(List.of(intent("rejected")));
        FakeStore store = new FakeStore();
        FakeGateway gateway = new FakeGateway();
        FakePersistence persistence = new FakePersistence();

        assertEquals(1, worker(claims, store, gateway, persistence).runOnce());
        assertEquals(1, store.rejected.get());
        assertEquals(0, persistence.persisted.get());
        assertEquals(0, gateway.sendCalls.get());
    }

    @Test
    void newOrderAtMaxAttemptsStopsAutomaticLoopWithoutResend() {
        CopyDispatchIntentEntity pending = intent("new");
        pending.setReconciliationAttempts(20);
        FakeReconciliation claims = new FakeReconciliation(List.of(pending));
        FakeGateway gateway = new FakeGateway();

        worker(claims, new FakeStore(), gateway, new FakePersistence()).runOnce();

        assertEquals(1, claims.terminal.get());
        assertEquals(0, claims.deferred.get());
        assertEquals(0, gateway.sendCalls.get());
    }

    @Test
    void unresolvedPriceAtMaxAttemptsStopsAutomaticLoopWithoutResend() {
        CopyDispatchIntentEntity pending = intent("price");
        pending.setReconciliationAttempts(20);
        FakeReconciliation claims = new FakeReconciliation(List.of(pending));
        FakeGateway gateway = new FakeGateway();

        worker(claims, new FakeStore(), gateway, new FakePersistence()).runOnce();

        assertEquals(1, claims.priceExhausted.get());
        assertEquals(0, gateway.sendCalls.get());
    }

    private CopyOrderReconciliationWorker worker(FakeReconciliation claims, FakeStore store,
                                                  FakeGateway gateway, FakePersistence persistence) {
        CopyOrderReconciliationWorker worker = new CopyOrderReconciliationWorker(claims, store, gateway, users(),
                new BinanceOrderExecutionNormalizer(), persistence, new SimpleMeterRegistry());
        setField(worker, "batchSize", 50);
        setField(worker, "maxAttempts", 20);
        setField(worker, "dispatchStaleAfter", Duration.ofSeconds(30));
        return worker;
    }

    private void setField(Object target, String name, Object value) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError("Unable to set test field " + name, ex);
        }
    }

    private UserDetailCachedService users() {
        UserEntity user = new UserEntity(); user.setId(UUID.nameUUIDFromBytes("user-1".getBytes()));
        UserApiKeyEntity key = new UserApiKeyEntity(); key.setApiKey("test-key"); key.setApiSecret("test-secret");
        UserDetailDto detail = new UserDetailDto(user, null, key);
        return new UserDetailCachedService() {
            @Override public List<UserDetailDto> getUsers() { return List.of(detail); }
            @Override public Optional<UserDetailDto> getUserById(String id) { return Optional.of(detail); }
            @Override public void updateRuntimeCapital(UUID id, Integer capital, String asset) { }
        };
    }

    private CopyDispatchIntentEntity intent(String clientOrderId) {
        return CopyDispatchIntentEntity.builder().id(UUID.randomUUID()).idUser("user-1")
                .userCopyAllocationId(505L).executionMode("MICRO_LIVE").walletId("0xabc")
                .strategyCode("MOVEMENT_ALL").sourceEventId("evt-" + clientOrderId)
                .idOrderOrigin("origin-1").copyIntent("OPEN").symbol("BTCUSDC")
                .side("BUY").positionSide("LONG").requestedQty(BigDecimal.ONE)
                .requestedMarginUsd(new BigDecimal("20")).requestedNotionalUsd(new BigDecimal("100"))
                .referencePrice(new BigDecimal("100")).requestedLeverage(5).clientOrderId(clientOrderId)
                .status("RECONCILING").reservationStatus("PENDING").averagePriceStatus("NOT_AVAILABLE")
                .requestHash("a".repeat(64)).createdAt(OffsetDateTime.now()).updatedAt(OffsetDateTime.now()).build();
    }

    private static final class FakeReconciliation extends CopyOrderReconciliationService {
        private final List<CopyDispatchIntentEntity> batch;
        private final AtomicInteger failures = new AtomicInteger();
        private final AtomicInteger deferred = new AtomicInteger();
        private final AtomicInteger terminal = new AtomicInteger();
        private final AtomicInteger priceExhausted = new AtomicInteger();
        private FakeReconciliation(List<CopyDispatchIntentEntity> batch) { super(null); this.batch = batch; }
        @Override public List<CopyDispatchIntentEntity> claimBatch(int limit, Duration stale, String worker) { return batch; }
        @Override public void markLookupNotFound(UUID id, int max) { }
        @Override public void deferNewOrder(UUID id) { deferred.incrementAndGet(); }
        @Override public void markUnresolvedTerminal(UUID id, String reasonCode) { terminal.incrementAndGet(); }
        @Override public void markPriceResolutionExhausted(UUID id) { priceExhausted.incrementAndGet(); }
        @Override public void markFailure(UUID id, int max, String code, String detail) { failures.incrementAndGet(); }
    }

    private static final class FakeGateway implements ProcesBinanceService {
        private final AtomicInteger sendCalls = new AtomicInteger();
        private final AtomicInteger lookupCalls = new AtomicInteger();
        private final AtomicInteger orderIdLookupCalls = new AtomicInteger();
        @Override public BinanceFuturesOrderClientResponse operationPosition(OperationDto dto) { sendCalls.incrementAndGet(); throw new AssertionError("reconciler must not send"); }
        @Override public Optional<BinanceFuturesOrderClientResponse> findOrderByOrderId(OperationDto dto, Long orderId) {
            orderIdLookupCalls.incrementAndGet();
            return Optional.of(response(dto, "FILLED"));
        }
        @Override public Optional<BinanceFuturesOrderClientResponse> findOrderByClientOrderId(OperationDto dto) {
            lookupCalls.incrementAndGet();
            if ("bad".equals(dto.getClientOrderId())) throw new IllegalStateException("lookup failed");
            return Optional.of(response(dto, null));
        }
        @Override public List<com.apunto.engine.dto.client.BinanceFuturesPositionClientDto> getPositions(
                String apiKey, String secret, String traceId) { return List.of(); }
        private BinanceFuturesOrderClientResponse response(OperationDto dto, String forcedStatus) {
            BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
            response.setOrderId(77L);
            response.setClientOrderId(dto.getClientOrderId());
            response.setSymbol(dto.getSymbol());
            response.setPositionSide("LONG"); response.setSide("BUY");
            if (forcedStatus != null) {
                response.setStatus(forcedStatus);
                response.setExecutedQty(BigDecimal.ONE);
                response.setAvgPrice(new BigDecimal("100"));
            } else if ("new".equals(dto.getClientOrderId())) {
                response.setStatus("NEW");
            } else if ("rejected".equals(dto.getClientOrderId())) {
                response.setStatus("CANCELED");
            } else {
                response.setStatus("FILLED");
                response.setExecutedQty(BigDecimal.ONE);
                if (!"price".equals(dto.getClientOrderId())) response.setAvgPrice(new BigDecimal("100"));
            }
            return response;
        }
        @Override public List<BinanceFuturesSymbolInfoClientDto> getSymbols(String key) { return List.of(); }
        @Override public Optional<BinanceFuturesMarketPriceClientDto> getMarketPrice(String s, String u, boolean a) { return Optional.empty(); }
    }

    private static final class FakePersistence extends CopyExecutionPersistenceService {
        private final AtomicInteger persisted = new AtomicInteger();
        private FakePersistence() { super(null, null, null); }
        @Override public com.apunto.engine.dto.CopyOperationDto persistRecovered(CopyDispatchIntentEntity intent, BinanceFuturesOrderClientResponse response) { persisted.incrementAndGet(); return null; }
    }

    private static final class FakeStore implements CopyDispatchIntentStore {
        private final AtomicInteger rejected = new AtomicInteger();
        @Override public CopyDispatchPermit acquire(CopyDispatchRequest request) { throw new UnsupportedOperationException(); }
        @Override public void acknowledge(UUID id, NormalizedBinanceExecution execution, BinanceFuturesOrderClientResponse response) { }
        @Override public void markAmbiguous(UUID id, String code, String detail) { }
        @Override public void markRejected(UUID id, String code, String detail) { rejected.incrementAndGet(); }
        @Override public void markPersistencePending(String clientOrderId, String code, String detail) { }
    }
}
