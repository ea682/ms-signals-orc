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
import com.apunto.engine.entity.CopyOperationEventEntity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CopyOperationEventServiceImplTest {

    @Test
    void incompleteRequiredLedgerEventFailsInsteadOfSilentlyMarkingIntentPersisted() {
        CopyOperationEventServiceImpl service = new CopyOperationEventServiceImpl(null, null);
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
        CopyOperationEventServiceImpl service = new CopyOperationEventServiceImpl(null, null);
        CopyOperationEventRecordCommand incomplete = CopyOperationEventRecordCommand.builder()
                .idOrderOrigin("origin-1")
                .idUser("user-1")
                .eventType("OPEN")
                .build();

        assertDoesNotThrow(() -> service.record(incomplete));
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
        CopyOperationEventServiceImpl service = new CopyOperationEventServiceImpl(repository, outbox);
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
}
