package com.apunto.engine.service.copy.flip;

import com.apunto.engine.entity.CopyFlipSagaEntity;
import com.apunto.engine.repository.CopyFlipSagaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.UUID;

@Service
public class CopyFlipSagaService {

    private final CopyFlipSagaRepository repository;

    public CopyFlipSagaService(CopyFlipSagaRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public CopyFlipSagaDecision start(Long allocationId,
                                      UUID exchangeAccountId,
                                      UUID oldCycleId,
                                      UUID newCycleId,
                                      String symbol,
                                      String oldSide,
                                      String newSide) {
        CopyFlipSagaEntity existing = repository
                .findByUserCopyAllocationIdAndNewSourcePositionCycleId(allocationId, newCycleId)
                .orElse(null);
        if (existing != null) return decision(existing);
        if (allocationId == null || exchangeAccountId == null || newCycleId == null
                || !text(symbol) || !text(newSide)) {
            return new CopyFlipSagaDecision(false, "FLIP_SAGA_IDENTITY_INCOMPLETE", null);
        }
        CopyFlipSagaEntity saga = new CopyFlipSagaEntity();
        saga.setId(UUID.randomUUID());
        saga.setUserCopyAllocationId(allocationId);
        saga.setExchangeAccountId(exchangeAccountId);
        saga.setOldSourcePositionCycleId(oldCycleId);
        saga.setNewSourcePositionCycleId(newCycleId);
        saga.setSymbol(code(symbol));
        saga.setOldSide(code(oldSide));
        saga.setNewSide(code(newSide));
        saga.setSagaStatus("CLOSE_OLD_PENDING");
        try {
            return decision(repository.saveAndFlush(saga));
        } catch (DataIntegrityViolationException race) {
            return repository.findByUserCopyAllocationIdAndNewSourcePositionCycleId(allocationId, newCycleId)
                    .map(this::decision)
                    .orElse(new CopyFlipSagaDecision(false, "FLIP_SAGA_CONCURRENCY_LOST", null));
        }
    }

    @Transactional
    public CopyFlipSagaDecision confirmOldLegClosed(UUID sagaId, boolean flatConfirmed, String result) {
        CopyFlipSagaEntity saga = required(sagaId);
        saga.setOldLegResult(code(result));
        if (!flatConfirmed) {
            saga.setSagaStatus("RECONCILIATION_REQUIRED");
            saga.setReasonCode(reasonForOldLeg(result));
            return decision(repository.saveAndFlush(saga));
        }
        saga.setSagaStatus("OPEN_NEW_PENDING");
        saga.setReasonCode(null);
        return decision(repository.saveAndFlush(saga));
    }

    @Transactional
    public CopyFlipSagaDecision skipNewLeg(UUID sagaId, String newLegReason) {
        CopyFlipSagaEntity saga = required(sagaId);
        saga.setSagaStatus("COMPLETED_FLAT_NEW_SKIPPED");
        saga.setNewLegResult(code(newLegReason));
        saga.setReasonCode("FLIP_CLOSED_OLD_BUT_NEW_SKIPPED");
        saga.setCompletedAt(OffsetDateTime.now(ZoneOffset.UTC));
        return decision(repository.saveAndFlush(saga));
    }

    @Transactional
    public CopyFlipSagaDecision complete(UUID sagaId, String newOpenClientOrderId) {
        CopyFlipSagaEntity saga = required(sagaId);
        if (!"OPEN_NEW_PENDING".equals(saga.getSagaStatus())) {
            return decision(saga);
        }
        saga.setSagaStatus("COMPLETED");
        saga.setNewLegResult("OPENED");
        saga.setReasonCode("FLIP_COMPLETED");
        saga.setNewOpenClientOrderId(newOpenClientOrderId);
        saga.setCompletedAt(OffsetDateTime.now(ZoneOffset.UTC));
        return decision(repository.saveAndFlush(saga));
    }

    private CopyFlipSagaEntity required(UUID id) {
        return repository.findByIdForUpdate(id)
                .orElseThrow(() -> new IllegalStateException("FLIP_SAGA_NOT_FOUND"));
    }

    private CopyFlipSagaDecision decision(CopyFlipSagaEntity saga) {
        boolean mayOpen = saga != null && "OPEN_NEW_PENDING".equals(saga.getSagaStatus());
        return new CopyFlipSagaDecision(mayOpen, saga == null ? "FLIP_SAGA_NOT_FOUND" : saga.getReasonCode(), saga);
    }

    private static String reasonForOldLeg(String result) {
        String code = code(result);
        if (code != null && code.contains("AMBIGUOUS")) return "FLIP_OLD_LEG_CLOSE_AMBIGUOUS";
        if (code != null && code.contains("PARTIAL")) return "FLIP_OLD_LEG_PARTIAL_CLOSE";
        return "FLIP_OLD_LEG_CLOSE_FAILED";
    }

    private static boolean text(String value) {
        return value != null && !value.isBlank();
    }

    private static String code(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }
}
