package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.dto.CopyOperationEventRecordCommand;
import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.entity.CopyOperationEntity;
import com.apunto.engine.service.CopyOperationEventService;
import com.apunto.engine.service.CopyOperationService;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyExecutionPersistenceServiceTest {

    @Test
    void processCrashAfterFillBeforePersistenceRecoversWithoutResend() {
        FakeOperations operations = new FakeOperations();
        FakeEvents events = new FakeEvents();
        FakeStore store = new FakeStore();
        CopyExecutionPersistenceService service = new CopyExecutionPersistenceService(operations, events, store);
        CopyDispatchIntentEntity intent = openIntent();

        CopyOperationDto recovered = service.persistRecovered(intent, filled(null));

        assertNotNull(recovered.getIdOperation());
        assertTrue(recovered.isActive());
        assertEquals(new BigDecimal("100"), recovered.getPriceEntry());
        assertEquals("PENDING_RESOLUTION", recovered.getPriceStatus());
        assertEquals(1, operations.rows.size());
        assertEquals(1, events.uniqueClientOrderIds.size());
        assertEquals(1, store.persisted);
    }

    @Test
    void orphanFilledOrderCanBeBackfilledIdempotently() {
        FakeOperations operations = new FakeOperations();
        FakeEvents events = new FakeEvents();
        FakeStore store = new FakeStore();
        CopyExecutionPersistenceService service = new CopyExecutionPersistenceService(operations, events, store);
        CopyDispatchIntentEntity intent = openIntent();

        service.persistRecovered(intent, filled("101"));
        service.persistRecovered(intent, filled("101"));

        assertEquals(1, operations.rows.size());
        assertEquals(BigDecimal.ONE, operations.only().getSizePar());
        assertEquals(1, events.uniqueClientOrderIds.size());
    }

    @Test
    void recoveredPartialReductionKeepsOperationActive() {
        FakeOperations operations = new FakeOperations();
        FakeEvents events = new FakeEvents();
        FakeStore store = new FakeStore();
        CopyOperationDto current = openCopy(new BigDecimal("2"));
        operations.upsertActiveOperation(current);
        CopyExecutionPersistenceService service = new CopyExecutionPersistenceService(operations, events, store);
        CopyDispatchIntentEntity reduce = openIntent();
        reduce.setCopyIntent("REDUCE");
        reduce.setReduceOnly(true);
        reduce.setRequestedQty(new BigDecimal("0.5"));
        BinanceFuturesOrderClientResponse response = filled("99");
        response.setExecutedQty(new BigDecimal("0.5"));

        CopyOperationDto recovered = service.persistRecovered(reduce, response);

        assertTrue(recovered.isActive());
        assertEquals(new BigDecimal("1.5"), recovered.getSizePar());
    }

    @Test
    void recoveredCloseMarksOperationClosed() {
        FakeOperations operations = new FakeOperations();
        FakeEvents events = new FakeEvents();
        FakeStore store = new FakeStore();
        operations.upsertActiveOperation(openCopy(BigDecimal.ONE));
        CopyExecutionPersistenceService service = new CopyExecutionPersistenceService(operations, events, store);
        CopyDispatchIntentEntity close = openIntent();
        close.setCopyIntent("CLOSE");
        close.setReduceOnly(true);

        CopyOperationDto recovered = service.persistRecovered(close, filled("99"));

        assertFalse(recovered.isActive());
        assertEquals(BigDecimal.ZERO, recovered.getSizePar());
    }

    @Test
    void cumulativePartialOpenAppliesOnlyNewExecutedDelta() {
        FakeOperations operations = new FakeOperations();
        FakeEvents events = new FakeEvents();
        FakeStore store = new FakeStore();
        CopyExecutionPersistenceService service = new CopyExecutionPersistenceService(operations, events, store);
        CopyDispatchIntentEntity intent = openIntent();
        intent.setStatus("PARTIALLY_FILLED");
        BinanceFuturesOrderClientResponse partial = filled("100");
        partial.setStatus("PARTIALLY_FILLED");
        partial.setExecutedQty(new BigDecimal("0.4"));

        CopyOperationDto first = service.persistRecovered(intent, partial);
        BinanceFuturesOrderClientResponse completed = filled("100");
        completed.setExecutedQty(BigDecimal.ONE);
        CopyOperationDto second = service.persistRecovered(intent, completed);

        assertEquals(new BigDecimal("0.4"), first.getSizePar());
        assertEquals(0, BigDecimal.ONE.compareTo(second.getSizePar()));
        assertEquals(0, BigDecimal.ONE.compareTo(intent.getPersistedExecutedQty()));
    }

    @Test
    void cumulativePartialIncreaseIsNotAddedTwice() {
        FakeOperations operations = new FakeOperations();
        FakeEvents events = new FakeEvents();
        FakeStore store = new FakeStore();
        operations.upsertActiveOperation(openCopy(BigDecimal.ONE));
        CopyExecutionPersistenceService service = new CopyExecutionPersistenceService(operations, events, store);
        CopyDispatchIntentEntity increase = openIntent();
        increase.setCopyIntent("INCREASE");
        BinanceFuturesOrderClientResponse partial = filled("100");
        partial.setStatus("PARTIALLY_FILLED");
        partial.setExecutedQty(new BigDecimal("0.4"));

        service.persistRecovered(increase, partial);
        BinanceFuturesOrderClientResponse completed = filled("100");
        completed.setExecutedQty(BigDecimal.ONE);
        CopyOperationDto result = service.persistRecovered(increase, completed);

        assertEquals(new BigDecimal("2.0"), result.getSizePar());
    }

    @Test
    void cumulativePartialReductionIsNotSubtractedTwice() {
        FakeOperations operations = new FakeOperations();
        FakeEvents events = new FakeEvents();
        FakeStore store = new FakeStore();
        operations.upsertActiveOperation(openCopy(new BigDecimal("2")));
        CopyExecutionPersistenceService service = new CopyExecutionPersistenceService(operations, events, store);
        CopyDispatchIntentEntity reduce = openIntent();
        reduce.setCopyIntent("REDUCE");
        reduce.setReduceOnly(true);
        BinanceFuturesOrderClientResponse partial = filled("100");
        partial.setStatus("PARTIALLY_FILLED");
        partial.setExecutedQty(new BigDecimal("0.4"));

        service.persistRecovered(reduce, partial);
        BinanceFuturesOrderClientResponse completed = filled("100");
        completed.setExecutedQty(BigDecimal.ONE);
        CopyOperationDto result = service.persistRecovered(reduce, completed);

        assertEquals(new BigDecimal("1.0"), result.getSizePar());
    }

    private CopyDispatchIntentEntity openIntent() {
        return CopyDispatchIntentEntity.builder()
                .id(UUID.randomUUID()).idempotencyKey("a".repeat(64)).idUser("user-1")
                .userCopyAllocationId(505L).executionMode("MICRO_LIVE").walletId("0xabc")
                .strategyCode("MOVEMENT_ALL").scopeType("ALL").scopeValue("ALL")
                .sourceEventId("evt-1").idOrderOrigin("origin-1").copyIntent("OPEN")
                .symbol("BTCUSDC").side("BUY").positionSide("LONG").reduceOnly(false)
                .requestedQty(BigDecimal.ONE).requestedMarginUsd(new BigDecimal("20"))
                .requestedNotionalUsd(new BigDecimal("100")).referencePrice(new BigDecimal("100"))
                .requestedLeverage(5).clientOrderId("cpO_recovery").binanceOrderId(99L)
                .binanceStatus("FILLED").executedQty(BigDecimal.ONE)
                .averagePriceStatus("PENDING_RESOLUTION").status("FILLED")
                .reservationStatus("PENDING").requestHash("b".repeat(64))
                .createdAt(OffsetDateTime.now()).updatedAt(OffsetDateTime.now()).build();
    }

    private BinanceFuturesOrderClientResponse filled(String avg) {
        BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
        response.setOrderId(99L); response.setClientOrderId("cpO_recovery"); response.setSymbol("BTCUSDC");
        response.setStatus("FILLED"); response.setExecutedQty(BigDecimal.ONE); response.setPositionSide("LONG");
        response.setSide("BUY"); response.setAvgPrice(avg == null ? null : new BigDecimal(avg));
        response.setAveragePriceStatus(avg == null ? "PENDING_RESOLUTION" : "AVAILABLE");
        return response;
    }

    private CopyOperationDto openCopy(BigDecimal qty) {
        return CopyOperationDto.builder().idOperation(UUID.randomUUID()).idOrden("original-order")
                .idUser("user-1").idOrderOrigin("origin-1").idWalletOrigin("0xabc")
                .parsymbol("BTCUSDC").typeOperation("LONG").leverage(new BigDecimal("5"))
                .siseUsd(qty.multiply(new BigDecimal("100"))).sizePar(qty).priceEntry(new BigDecimal("100"))
                .dateCreation(OffsetDateTime.now()).active(true).userCopyAllocationId(505L)
                .copyStrategyCode("MOVEMENT_ALL").executionMode("MICRO_LIVE").build();
    }

    private static final class FakeOperations implements CopyOperationService {
        private final Map<String, CopyOperationDto> rows = new LinkedHashMap<>();
        private String key(CopyOperationDto op) { return op.getUserCopyAllocationId() + "|" + op.getIdOrderOrigin() + "|" + op.getTypeOperation(); }
        private CopyOperationDto only() { return rows.values().iterator().next(); }
        @Override public void newOperation(CopyOperationDto operation) { upsertActiveOperation(operation); }
        @Override public void closeOperation(CopyOperationDto operation) { operation.setActive(false); rows.put(key(operation), operation); }
        @Override public CopyOperationDto findOperation(String idOrden) { return rows.values().stream().filter(v -> idOrden.equals(v.getIdOrden())).findFirst().orElse(null); }
        @Override public List<CopyOperationDto> findOperationsByOrigin(String id) { return new ArrayList<>(rows.values()); }
        @Override public Optional<CopyOperationEntity> findOperationByOrigin(String id) { return Optional.empty(); }
        @Override public CopyOperationDto findOperationForUser(String a, String b) { return null; }
        @Override public CopyOperationDto findOperationForAllocation(String origin, String user, Long allocation, String strategy, String type) { return rows.get(allocation + "|" + origin + "|" + type); }
        @Override public boolean existsByOriginAndUser(String a, String b) { return !rows.isEmpty(); }
        @Override public List<CopyOperationDto> findActiveOperationsForUserOrigin(String a, String b) { return rows.values().stream().filter(CopyOperationDto::isActive).toList(); }
        @Override public List<CopyOperationDto> findActiveOperationsByUserAndWallet(String a, String b) { return findActiveOperationsForUserOrigin(a, b); }
        @Override public void upsertActiveOperation(CopyOperationDto operation) { if (operation.getIdOperation() == null) operation.setIdOperation(UUID.randomUUID()); rows.put(key(operation), operation); }
        @Override public CopyOperationDto findOperationForUserAndType(String a, String b, String c) { return null; }
        @Override public BigDecimal sumBufferedMarginActive(String a, String b, BigDecimal c) { return BigDecimal.ZERO; }
        @Override public BigDecimal sumBufferedMarginActiveForUser(String a, BigDecimal b) { return BigDecimal.ZERO; }
    }

    private static final class FakeEvents implements CopyOperationEventService {
        private final java.util.Set<String> uniqueClientOrderIds = new java.util.HashSet<>();
        @Override public void record(CopyOperationEventRecordCommand command) { uniqueClientOrderIds.add(command.getClientOrderId()); }
        @Override public void recordRequired(CopyOperationEventRecordCommand command) { uniqueClientOrderIds.add(command.getClientOrderId()); }
    }

    private static final class FakeStore implements CopyDispatchIntentStore {
        private int persisted;
        @Override public CopyDispatchPermit acquire(CopyDispatchRequest request) { throw new UnsupportedOperationException(); }
        @Override public void acknowledge(UUID id, NormalizedBinanceExecution execution, BinanceFuturesOrderClientResponse response) { }
        @Override public void markAmbiguous(UUID id, String code, String detail) { }
        @Override public void markRejected(UUID id, String code, String detail) { }
        @Override public void markPersistencePending(String clientOrderId, String code, String detail) { }
        @Override public void markPersisted(String clientOrderId, UUID operationId) { persisted++; }
    }
}
