package com.apunto.engine.service.copy.ownership;

import com.apunto.engine.entity.CopyPositionOwnershipEntity;
import com.apunto.engine.repository.CopyPositionOwnershipRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

@Service
public class CopyPositionOwnershipService {

    private final CopyPositionOwnershipRepository repository;

    public CopyPositionOwnershipService(CopyPositionOwnershipRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public CopyPositionOwnershipDecision claimOpen(Long allocationId,
                                                   UUID exchangeAccountId,
                                                   UUID sourcePositionCycleId,
                                                   String symbol,
                                                   String positionSide,
                                                   BigDecimal fixedLeverage,
                                                   String fixedMarginMode,
                                                   String fixedPositionMode) {
        if (allocationId == null || exchangeAccountId == null || sourcePositionCycleId == null
                || !text(symbol) || !positive(fixedLeverage)
                || !text(fixedMarginMode) || !text(fixedPositionMode)) {
            return new CopyPositionOwnershipDecision(false, "VIRTUAL_OWNERSHIP_MISMATCH", null);
        }
        Optional<CopyPositionOwnershipEntity> current = repository.findActiveForUpdate(
                exchangeAccountId, symbol);
        if (current.isPresent()) return existingDecision(current.get(), allocationId, sourcePositionCycleId);

        CopyPositionOwnershipEntity ownership = new CopyPositionOwnershipEntity();
        ownership.setId(UUID.randomUUID());
        ownership.setExchangeAccountId(exchangeAccountId);
        ownership.setUserCopyAllocationId(allocationId);
        ownership.setSourcePositionCycleId(sourcePositionCycleId);
        ownership.setSymbol(code(symbol));
        ownership.setPositionSide(code(positionSide));
        ownership.setOwnedQty(BigDecimal.ZERO);
        ownership.setFixedLeverage(fixedLeverage);
        ownership.setFixedMarginMode(code(fixedMarginMode));
        ownership.setFixedPositionMode(code(fixedPositionMode));
        ownership.setOwnershipStatus("OPEN");
        try {
            return new CopyPositionOwnershipDecision(true, "ACCOUNT_SYMBOL_OWNERSHIP_CLAIMED",
                    repository.saveAndFlush(ownership));
        } catch (DataIntegrityViolationException race) {
            return repository.findActiveForUpdate(exchangeAccountId, symbol)
                    .map(value -> existingDecision(value, allocationId, sourcePositionCycleId))
                    .orElse(new CopyPositionOwnershipDecision(false, "ACCOUNT_SYMBOL_ALREADY_OWNED", null));
        }
    }

    @Transactional
    public CopyPositionOwnershipDecision verifyOwner(Long allocationId, UUID exchangeAccountId, String symbol) {
        CopyPositionOwnershipEntity ownership = repository.findActiveForUpdate(exchangeAccountId, symbol)
                .orElse(null);
        if (ownership == null || !allocationId.equals(ownership.getUserCopyAllocationId())) {
            return new CopyPositionOwnershipDecision(false,
                    ownership == null ? "VIRTUAL_OWNERSHIP_MISMATCH" : "ACCOUNT_SYMBOL_ALREADY_OWNED",
                    ownership);
        }
        return new CopyPositionOwnershipDecision(true, "ACCOUNT_SYMBOL_OWNERSHIP_VERIFIED", ownership);
    }

    @Transactional
    public CopyPositionOwnershipDecision authorizeDerisk(Long allocationId,
                                                         UUID exchangeAccountId,
                                                         String symbol,
                                                         BigDecimal requestedQuantity) {
        CopyPositionOwnershipDecision verification = verifyOwner(allocationId, exchangeAccountId, symbol);
        if (!verification.allowed()) return verification;
        if (!positive(requestedQuantity)) {
            return new CopyPositionOwnershipDecision(false, "VIRTUAL_OWNERSHIP_MISMATCH",
                    verification.ownership(), BigDecimal.ZERO);
        }
        CopyPositionOwnershipEntity ownership = verification.ownership();
        BigDecimal owned = nonNegative(ownership.getOwnedQty());
        BigDecimal actual = ownership.getActualBinanceQty() == null
                ? owned : nonNegative(ownership.getActualBinanceQty());
        BigDecimal authorized = requestedQuantity.min(owned).min(actual);
        if (authorized.signum() <= 0) {
            ownership.setOwnershipStatus("RECONCILING");
            ownership.setReconciliationRequired(true);
            repository.saveAndFlush(ownership);
            return new CopyPositionOwnershipDecision(false, "ACCOUNT_POSITION_DRIFT",
                    ownership, BigDecimal.ZERO);
        }
        String reason = authorized.compareTo(requestedQuantity) < 0
                ? "DERISK_QUANTITY_CAPPED_TO_OWNERSHIP"
                : "DERISK_QUANTITY_OWNERSHIP_VERIFIED";
        return new CopyPositionOwnershipDecision(true, reason, ownership, authorized);
    }

    @Transactional
    public CopyPositionOwnershipDecision recordFill(Long allocationId,
                                                    UUID exchangeAccountId,
                                                    String symbol,
                                                    CopyOwnershipFillType fillType,
                                                    BigDecimal executedQty,
                                                    BigDecimal actualBinanceQty) {
        CopyPositionOwnershipDecision verification = verifyOwner(allocationId, exchangeAccountId, symbol);
        if (!verification.allowed()) return verification;
        CopyPositionOwnershipEntity ownership = verification.ownership();
        BigDecimal executed = nonNegative(executedQty);
        BigDecimal owned = nonNegative(ownership.getOwnedQty());
        BigDecimal next = switch (fillType) {
            case OPEN, INCREASE -> owned.add(executed);
            case REDUCE, CLOSE -> owned.subtract(executed).max(BigDecimal.ZERO);
        };
        ownership.setOwnedQty(next);
        BigDecimal observedActual = actualBinanceQty == null ? next : nonNegative(actualBinanceQty);
        ownership.setActualBinanceQty(observedActual);
        if (next.signum() == 0 && observedActual.signum() == 0) {
            ownership.setOwnershipStatus("CLOSED");
            ownership.setClosedAt(OffsetDateTime.now(ZoneOffset.UTC));
        } else if (observedActual.compareTo(next) != 0) {
            ownership.setOwnershipStatus("RECONCILING");
            ownership.setReconciliationRequired(true);
        }
        return new CopyPositionOwnershipDecision(true, "VIRTUAL_OWNERSHIP_UPDATED",
                repository.saveAndFlush(ownership));
    }

    @Transactional
    public void markReconciliationRequired(Long allocationId, UUID exchangeAccountId, String symbol) {
        CopyPositionOwnershipDecision verification = verifyOwner(allocationId, exchangeAccountId, symbol);
        if (!verification.allowed()) return;
        verification.ownership().setOwnershipStatus("RECONCILING");
        verification.ownership().setReconciliationRequired(true);
        repository.saveAndFlush(verification.ownership());
    }

    private static CopyPositionOwnershipDecision existingDecision(CopyPositionOwnershipEntity current,
                                                                  Long allocationId,
                                                                  UUID sourcePositionCycleId) {
        boolean same = allocationId.equals(current.getUserCopyAllocationId())
                && sourcePositionCycleId.equals(current.getSourcePositionCycleId());
        return new CopyPositionOwnershipDecision(same,
                same ? "ACCOUNT_SYMBOL_OWNERSHIP_IDEMPOTENT" : "ACCOUNT_SYMBOL_ALREADY_OWNED",
                current);
    }

    private static BigDecimal nonNegative(BigDecimal value) {
        return value == null || value.signum() < 0 ? BigDecimal.ZERO : value;
    }

    private static boolean positive(BigDecimal value) {
        return value != null && value.signum() > 0;
    }

    private static boolean text(String value) {
        return value != null && !value.isBlank();
    }

    private static String code(String value) {
        return value == null ? null : value.trim().toUpperCase(Locale.ROOT);
    }
}
