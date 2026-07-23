package com.apunto.engine.service.copy.flip;

import com.apunto.engine.entity.CopyFlipSagaEntity;
import com.apunto.engine.repository.CopyFlipSagaRepository;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyFlipSagaServiceTest {

    @Test
    void restartReusesSagaAndNewLegCannotOpenBeforeFlatConfirmation() {
        AtomicReference<CopyFlipSagaEntity> stored = new AtomicReference<>();
        CopyFlipSagaService service = new CopyFlipSagaService(repository(stored));
        UUID newCycle = UUID.randomUUID();
        UUID account = UUID.randomUUID();

        CopyFlipSagaDecision started = service.start(
                10L, account, UUID.randomUUID(), newCycle, "BTCUSDC", "LONG", "SHORT");
        CopyFlipSagaDecision restarted = service.start(
                10L, account, UUID.randomUUID(), newCycle, "BTCUSDC", "LONG", "SHORT");
        assertEquals(started.saga().getId(), restarted.saga().getId());
        assertFalse(service.confirmOldLegClosed(started.saga().getId(), false, "PARTIAL").mayOpenNewLeg());
        assertEquals("FLIP_OLD_LEG_PARTIAL_CLOSE", stored.get().getReasonCode());

        stored.get().setSagaStatus("CONFIRM_FLAT_PENDING");
        CopyFlipSagaDecision flat = service.confirmOldLegClosed(started.saga().getId(), true, "CLOSED");
        assertTrue(flat.mayOpenNewLeg());
        assertEquals("OPEN_NEW_PENDING", stored.get().getSagaStatus());
    }

    @Test
    void skippedNewLegCompletesSagaFlatWithExactReason() {
        AtomicReference<CopyFlipSagaEntity> stored = new AtomicReference<>();
        CopyFlipSagaService service = new CopyFlipSagaService(repository(stored));
        CopyFlipSagaDecision started = service.start(
                10L, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                "ETHUSDC", "SHORT", "LONG");
        service.confirmOldLegClosed(started.saga().getId(), true, "CLOSED");

        CopyFlipSagaDecision completed = service.skipNewLeg(
                started.saga().getId(), "FLIP_NEW_LEG_BELOW_MIN_NOTIONAL");
        assertFalse(completed.mayOpenNewLeg());
        assertEquals("COMPLETED_FLAT_NEW_SKIPPED", stored.get().getSagaStatus());
        assertEquals("FLIP_CLOSED_OLD_BUT_NEW_SKIPPED", stored.get().getReasonCode());
        assertEquals("FLIP_NEW_LEG_BELOW_MIN_NOTIONAL", stored.get().getNewLegResult());
    }

    private static CopyFlipSagaRepository repository(AtomicReference<CopyFlipSagaEntity> stored) {
        return (CopyFlipSagaRepository) Proxy.newProxyInstance(
                CopyFlipSagaRepository.class.getClassLoader(),
                new Class<?>[]{CopyFlipSagaRepository.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "findByUserCopyAllocationIdAndNewSourcePositionCycleId" -> Optional.ofNullable(stored.get());
                    case "findByIdForUpdate" -> Optional.ofNullable(stored.get());
                    case "saveAndFlush", "save" -> {
                        CopyFlipSagaEntity value = (CopyFlipSagaEntity) args[0];
                        if (value.getId() == null) value.setId(UUID.randomUUID());
                        stored.set(value);
                        yield value;
                    }
                    case "toString" -> "flip-saga-repository-stub";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
