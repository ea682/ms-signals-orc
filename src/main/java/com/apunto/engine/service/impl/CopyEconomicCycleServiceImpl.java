package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyEconomicCycleEntity;
import com.apunto.engine.repository.CopyEconomicCycleRepository;
import com.apunto.engine.service.CopyEconomicCycleService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;

import static com.apunto.engine.entity.UserCopyAllocationEntity.normalizeExecutionMode;

@Slf4j
@Service
@RequiredArgsConstructor
public class CopyEconomicCycleServiceImpl implements CopyEconomicCycleService {

    private static final String PENDING_RECONCILIATION = "PENDING_RECONCILIATION";

    private final CopyEconomicCycleRepository repository;

    @Override
    @Transactional
    public CycleIdentity open(CopyOperationDto operation, UUID copyOperationId) {
        Objects.requireNonNull(operation, "operation no puede ser null");
        Objects.requireNonNull(copyOperationId, "copyOperationId no puede ser null");

        CopyEconomicCycleEntity existing = repository.findByCopyOperationId(copyOperationId).orElse(null);
        if (existing != null) {
            return identity(existing);
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        Long sequence = Objects.requireNonNull(repository.nextCycleSequence(), "cycleSequence no puede ser null");
        CopyEconomicCycleEntity cycle = CopyEconomicCycleEntity.builder()
                .copyOperationId(copyOperationId)
                .cycleSequence(sequence)
                .idUser(operation.getIdUser())
                .idWalletOrigin(operation.getIdWalletOrigin())
                .userCopyAllocationId(operation.getUserCopyAllocationId())
                .exchangeAccountId(operation.getExchangeAccountId())
                .sourcePositionCycleId(operation.getSourcePositionCycleId())
                .fixedLeverage(operation.getLeverage())
                .fixedMarginMode(operation.getFixedMarginMode())
                .fixedPositionMode(operation.getFixedPositionMode())
                .virtualOwnedQty(operation.getSizePar() == null ? java.math.BigDecimal.ZERO : operation.getSizePar())
                .copyStrategyCode(operation.getCopyStrategyCode())
                .parsymbol(operation.getParsymbol())
                .positionSide(operation.getTypeOperation())
                .executionMode(normalizeExecutionMode(operation.getExecutionMode()))
                .sourceFirstEventId(operation.getSourceEventId())
                .sourceLastEventId(operation.getSourceEventId())
                .openedAt(operation.getDateCreation() == null ? now : operation.getDateCreation())
                .cycleStatus("OPEN")
                .economicDataStatus(PENDING_RECONCILIATION)
                .createdAt(now)
                .updatedAt(now)
                .build();
        try {
            return identity(repository.saveAndFlush(cycle));
        } catch (DataIntegrityViolationException duplicate) {
            CopyEconomicCycleEntity recovered = repository.findByCopyOperationId(copyOperationId).orElseThrow(() -> duplicate);
            log.info("event=copy_economic_cycle.open_idempotent copyOperationId={} cycleId={} cycleSequence={}",
                    copyOperationId, recovered.getCycleId(), recovered.getCycleSequence());
            return identity(recovered);
        }
    }

    @Override
    @Transactional
    public void close(UUID copyOperationId, String sourceEventId, OffsetDateTime closedAt) {
        if (copyOperationId == null) {
            log.warn("event=copy_economic_cycle.close_skipped reasonCode=COPY_OPERATION_ID_MISSING");
            return;
        }
        CopyEconomicCycleEntity cycle = repository.findByCopyOperationId(copyOperationId).orElse(null);
        if (cycle == null) {
            log.error("event=copy_economic_cycle.close_missing reasonCode=ECONOMIC_CYCLE_NOT_FOUND copyOperationId={}", copyOperationId);
            return;
        }
        if ("CLOSED".equals(cycle.getCycleStatus())) {
            return;
        }
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        cycle.setSourceLastEventId(sourceEventId == null ? cycle.getSourceLastEventId() : sourceEventId);
        cycle.setClosedAt(closedAt == null ? now : closedAt);
        cycle.setCycleStatus("CLOSED");
        cycle.setUpdatedAt(now);
        repository.saveAndFlush(cycle);
    }

    private CycleIdentity identity(CopyEconomicCycleEntity entity) {
        return new CycleIdentity(entity.getCycleId(), entity.getCycleSequence());
    }
}
