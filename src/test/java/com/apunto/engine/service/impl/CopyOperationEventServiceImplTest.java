package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationEventRecordCommand;
import com.apunto.engine.outbox.service.MetricCopyOperationOutboxService;
import com.apunto.engine.repository.CopyOperationEventRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import com.apunto.engine.entity.CopyOperationEventEntity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CopyOperationEventServiceImplTest {

    @Test
    void incompleteRequiredLedgerEventFailsInsteadOfSilentlyMarkingIntentPersisted() {
        CopyOperationEventServiceImpl service = new CopyOperationEventServiceImpl(null, null, null);
        CopyOperationEventRecordCommand incomplete = CopyOperationEventRecordCommand.builder()
                .idOrderOrigin("origin-1")
                .idUser("user-1")
                .parsymbol("BTCUSDC")
                .typeOperation("LONG")
                .eventType("OPEN")
                .build();

        assertThrows(IllegalArgumentException.class, () -> service.recordRequired(incomplete));
    }

    @Test
    void incompleteLegacyBestEffortLedgerEventRemainsNonBlocking() {
        CopyOperationEventServiceImpl service = new CopyOperationEventServiceImpl(null, null, null);
        CopyOperationEventRecordCommand incomplete = CopyOperationEventRecordCommand.builder()
                .idOrderOrigin("origin-1")
                .idUser("user-1")
                .eventType("OPEN")
                .build();

        assertDoesNotThrow(() -> service.record(incomplete));
    }

    @Test
    void externalFlatReconciliationIsDurableWithoutInventingADispatchIntent() {
        AtomicReference<CopyOperationEventEntity> saved = new AtomicReference<>();
        CopyOperationEventRepository repository = (CopyOperationEventRepository) Proxy.newProxyInstance(
                CopyOperationEventRepository.class.getClassLoader(),
                new Class<?>[]{CopyOperationEventRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "findByClientOrderId" -> Optional.empty();
                    case "saveAndFlush" -> {
                        CopyOperationEventEntity entity = (CopyOperationEventEntity) args[0];
                        entity.setIdEvent(UUID.randomUUID());
                        saved.set(entity);
                        yield entity;
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        AtomicInteger outboxWrites = new AtomicInteger();
        CopyOperationEventServiceImpl service = new CopyOperationEventServiceImpl(
                repository, null, entity -> outboxWrites.incrementAndGet());

        UUID eventId = service.recordReconciliationRequired(CopyOperationEventRecordCommand.builder()
                .decision("RECONCILED_CLOSE")
                .decisionReason("RECONCILED_EXTERNAL_POSITION_ALREADY_FLAT")
                .idOrderOrigin("origin-1")
                .idUser("user-1")
                .idWalletOrigin("0xabc")
                .parsymbol("BTCUSDT")
                .typeOperation("LONG")
                .eventType("CLOSE")
                .copyIntent("EXTERNAL_CLOSE")
                .clientOrderId("cpC_external_flat")
                .qtyRequested(BigDecimal.ZERO)
                .qtyExecuted(BigDecimal.ZERO)
                .previousQty(BigDecimal.ONE)
                .resultingQty(BigDecimal.ZERO)
                .economicDataStatus("UNAVAILABLE")
                .build());

        assertNotNull(eventId);
        assertNotNull(saved.get());
        assertEquals(null, saved.get().getDispatchIntentId());
        assertEquals("RECONCILED_CLOSE", saved.get().getDecision());
        assertEquals("UNAVAILABLE", saved.get().getEconomicDataStatus());
        assertEquals(1, outboxWrites.get());
    }

    @Test
    void repeatedDurableProgressIsNoopBeforeInsertAndCannotPoisonTransaction() {
        AtomicInteger inserts = new AtomicInteger();
        AtomicInteger outboxWrites = new AtomicInteger();
        CopyOperationEventRepository repository = (CopyOperationEventRepository) Proxy.newProxyInstance(
                CopyOperationEventRepository.class.getClassLoader(),
                new Class<?>[]{CopyOperationEventRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "lockDispatchProgress" -> null;
                    case "findDispatchProgress" -> Optional.of(CopyOperationEventEntity.builder()
                            .idEvent(UUID.randomUUID()).build());
                    case "saveAndFlush" -> {
                        inserts.incrementAndGet();
                        yield args[0];
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        MetricCopyOperationOutboxService outbox = entity -> outboxWrites.incrementAndGet();
        UUID dispatchIntentId = UUID.randomUUID();
        CopyOperationEventServiceImpl service = new CopyOperationEventServiceImpl(repository, null, outbox);
        CopyOperationEventRecordCommand replay = CopyOperationEventRecordCommand.builder()
                .dispatchIntentId(dispatchIntentId)
                .idOrderOrigin("origin-1")
                .idUser("user-1")
                .idWalletOrigin("0xabc")
                .parsymbol("BTCUSDC")
                .typeOperation("LONG")
                .eventType("OPEN")
                .copyIntent("OPEN")
                .clientOrderId("cpO_same")
                .qtyExecuted(BigDecimal.ZERO)
                .resultingQty(BigDecimal.ONE)
                .executionMode("MICRO_LIVE")
                .build();

        assertNotNull(service.recordRequired(replay));

        assertEquals(0, inserts.get());
        assertEquals(0, outboxWrites.get());
    }

    @Test
    void repeatedProgressMonotonicallyUpgradesEconomicEvidence() {
        UUID eventId = UUID.randomUUID();
        CopyOperationEventEntity pending = CopyOperationEventEntity.builder()
                .idEvent(eventId)
                .price(new BigDecimal("100"))
                .priceStatus("PENDING_RESOLUTION")
                .notionalUsd(new BigDecimal("100"))
                .economicDataStatus("PENDING_RECONCILIATION")
                .build();
        AtomicReference<CopyOperationEventEntity> saved = new AtomicReference<>();
        CopyOperationEventRepository repository = (CopyOperationEventRepository) Proxy.newProxyInstance(
                CopyOperationEventRepository.class.getClassLoader(),
                new Class<?>[]{CopyOperationEventRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "lockDispatchProgress" -> null;
                    case "findDispatchProgress" -> Optional.of(pending);
                    case "saveAndFlush" -> {
                        saved.set((CopyOperationEventEntity) args[0]);
                        yield args[0];
                    }
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        AtomicInteger outboxWrites = new AtomicInteger();
        CopyOperationEventServiceImpl service = new CopyOperationEventServiceImpl(
                repository, null, entity -> outboxWrites.incrementAndGet());

        UUID dispatchIntentId = UUID.randomUUID();
        UUID result = service.recordRequired(CopyOperationEventRecordCommand.builder()
                .dispatchIntentId(dispatchIntentId)
                .idOrderOrigin("origin-1")
                .idUser("user-1")
                .idWalletOrigin("0xabc")
                .parsymbol("BTCUSDT")
                .typeOperation("LONG")
                .eventType("OPEN")
                .qtyExecuted(BigDecimal.ONE)
                .resultingQty(BigDecimal.ONE)
                .price(new BigDecimal("101"))
                .priceStatus("AVAILABLE")
                .notionalUsd(new BigDecimal("101"))
                .realizedPnlUsd(new BigDecimal("1"))
                .feeUsd(new BigDecimal("0.01"))
                .totalFees(new BigDecimal("0.01"))
                .grossRealizedPnl(BigDecimal.ZERO)
                .economicDataStatus("KNOWN")
                .build());

        assertEquals(eventId, result);
        assertNotNull(saved.get());
        assertEquals("KNOWN", saved.get().getEconomicDataStatus());
        assertEquals("AVAILABLE", saved.get().getPriceStatus());
        assertEquals(new BigDecimal("101"), saved.get().getPrice());
        assertEquals(new BigDecimal("101"), saved.get().getNotionalUsd());
        assertEquals(new BigDecimal("1"), saved.get().getRealizedPnlUsd());
        assertEquals(new BigDecimal("0.01"), saved.get().getFeeUsd());
        assertEquals(new BigDecimal("0.01"), saved.get().getTotalFees());
        assertEquals(1, outboxWrites.get());
    }
}
