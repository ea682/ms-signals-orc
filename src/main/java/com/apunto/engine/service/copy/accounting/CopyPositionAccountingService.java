package com.apunto.engine.service.copy.accounting;

import com.apunto.engine.shared.enums.PositionSide;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
@Service
public class CopyPositionAccountingService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final int SCALE = 12;

    private final PositionDeltaClassifier classifier;

    public CopyPositionAccountingService() {
        this(new PositionDeltaClassifier());
    }

    public CopyPositionAccountingService(PositionDeltaClassifier classifier) {
        this.classifier = classifier == null ? new PositionDeltaClassifier() : classifier;
    }

    public CopyAccountingResult apply(CopyAccountingInput input) {
        BigDecimal previousQty = nonNegative(input == null ? null : input.previousQty());
        BigDecimal resultingQty = nonNegative(input == null ? null : input.resultingQty());
        BigDecimal feeUsd = nonNegative(input == null ? null : input.feeUsd());
        BigDecimal slippageUsd = nonNegative(input == null ? null : input.slippageUsd());
        BigDecimal avgEntry = input == null ? null : input.currentAvgEntryPrice();
        BigDecimal executionPrice = input == null ? null : input.executionPrice();
        PositionDeltaType deltaType = classifier.classify(previousQty, resultingQty);

        if (deltaType != PositionDeltaType.NOOP && !isPositive(executionPrice)) {
            return rejected(input, deltaType, previousQty, resultingQty, avgEntry, feeUsd, slippageUsd, "PRICE_SOURCE_UNAVAILABLE");
        }
        if ((deltaType == PositionDeltaType.REDUCE || deltaType == PositionDeltaType.CLOSE_FULL) && !isPositive(avgEntry)) {
            return rejected(input, deltaType, previousQty, resultingQty, avgEntry, feeUsd, slippageUsd, "ENTRY_PRICE_MISSING");
        }
        if ((deltaType == PositionDeltaType.REDUCE || deltaType == PositionDeltaType.CLOSE_FULL) && !isDirectionalSide(input == null ? null : input.side())) {
            return rejected(input, deltaType, previousQty, resultingQty, avgEntry, feeUsd, slippageUsd, "INVALID_SIDE");
        }

        BigDecimal deltaAddedQty = ZERO;
        BigDecimal deltaClosedQty = ZERO;
        BigDecimal deltaNotionalUsd = ZERO;
        BigDecimal previousAvgEntry = avgEntry;
        BigDecimal newAvgEntry = avgEntry;
        BigDecimal grossPnl = ZERO;
        BigDecimal netPnl = ZERO;
        boolean positionClosed = false;
        boolean positionRemainsOpen = false;
        String reasonCode = deltaType.name();

        switch (deltaType) {
            case OPEN -> {
                deltaAddedQty = resultingQty;
                deltaNotionalUsd = resultingQty.multiply(executionPrice).setScale(SCALE, RoundingMode.HALF_UP);
                newAvgEntry = executionPrice.setScale(SCALE, RoundingMode.HALF_UP);
                positionRemainsOpen = true;
            }
            case INCREASE -> {
                deltaAddedQty = resultingQty.subtract(previousQty).abs();
                deltaNotionalUsd = deltaAddedQty.multiply(executionPrice).setScale(SCALE, RoundingMode.HALF_UP);
                newAvgEntry = weightedAverageEntryPrice(previousQty, avgEntry, deltaAddedQty, executionPrice, resultingQty);
                positionRemainsOpen = true;
            }
            case REDUCE -> {
                deltaClosedQty = previousQty.subtract(resultingQty).abs();
                deltaNotionalUsd = deltaClosedQty.multiply(executionPrice).setScale(SCALE, RoundingMode.HALF_UP);
                grossPnl = realizedPnl(input.side(), deltaClosedQty, avgEntry, executionPrice);
                netPnl = grossPnl.subtract(feeUsd).subtract(slippageUsd).setScale(SCALE, RoundingMode.HALF_UP);
                newAvgEntry = avgEntry;
                positionRemainsOpen = true;
            }
            case CLOSE_FULL -> {
                deltaClosedQty = previousQty;
                deltaNotionalUsd = deltaClosedQty.multiply(executionPrice).setScale(SCALE, RoundingMode.HALF_UP);
                grossPnl = realizedPnl(input.side(), deltaClosedQty, avgEntry, executionPrice);
                netPnl = grossPnl.subtract(feeUsd).subtract(slippageUsd).setScale(SCALE, RoundingMode.HALF_UP);
                newAvgEntry = avgEntry;
                positionClosed = true;
            }
            case NOOP -> {
                feeUsd = ZERO;
                slippageUsd = ZERO;
                positionRemainsOpen = previousQty.compareTo(ZERO) > 0;
            }
        }

        CopyAccountingResult result = new CopyAccountingResult(
                true,
                deltaType,
                previousQty,
                resultingQty,
                deltaAddedQty,
                deltaClosedQty,
                normalizeAmount(deltaNotionalUsd),
                previousAvgEntry,
                newAvgEntry,
                grossPnl,
                netPnl,
                feeUsd,
                slippageUsd,
                positionRemainsOpen,
                positionClosed,
                reasonCode
        );
        logApplied(input, result);
        return result;
    }

    public String pnlFormula(PositionSide side) {
        return side == PositionSide.SHORT ? "SHORT_ENTRY_MINUS_EXEC" : "LONG_EXEC_MINUS_ENTRY";
    }

    public PositionDeltaClassification classify(PositionDeltaClassificationInput input) {
        return classifier.classify(input);
    }

    public BigDecimal weightedAverageEntryPrice(
            BigDecimal previousQty,
            BigDecimal previousEntryPrice,
            BigDecimal addedQty,
            BigDecimal addedEntryPrice,
            BigDecimal resultingQty
    ) {
        BigDecimal safePreviousQty = nonNegative(previousQty);
        BigDecimal safeAddedQty = nonNegative(addedQty);
        BigDecimal safeResultingQty = nonNegative(resultingQty);
        if (!isPositive(safeResultingQty)) {
            return firstPositive(previousEntryPrice, addedEntryPrice);
        }
        if (!isPositive(previousEntryPrice) || safePreviousQty.signum() <= 0) {
            return firstPositive(addedEntryPrice, previousEntryPrice);
        }
        if (!isPositive(addedEntryPrice) || safeAddedQty.signum() <= 0) {
            return previousEntryPrice;
        }
        return previousEntryPrice.multiply(safePreviousQty)
                .add(addedEntryPrice.multiply(safeAddedQty))
                .divide(safeResultingQty, SCALE, RoundingMode.HALF_UP);
    }

    public BigDecimal realizedPnl(PositionSide side, BigDecimal qty, BigDecimal entryPrice, BigDecimal executionPrice) {
        BigDecimal safeQty = nonNegative(qty);
        if (safeQty.signum() <= 0 || !isPositive(entryPrice) || !isPositive(executionPrice) || !isDirectionalSide(side)) {
            return ZERO;
        }
        BigDecimal diff = side == PositionSide.SHORT
                ? entryPrice.subtract(executionPrice)
                : executionPrice.subtract(entryPrice);
        return diff.multiply(safeQty).setScale(SCALE, RoundingMode.HALF_UP);
    }

    private CopyAccountingResult rejected(CopyAccountingInput input,
                                          PositionDeltaType deltaType,
                                          BigDecimal previousQty,
                                          BigDecimal resultingQty,
                                          BigDecimal avgEntry,
                                          BigDecimal feeUsd,
                                          BigDecimal slippageUsd,
                                          String reasonCode) {
        CopyAccountingResult result = CopyAccountingResult.rejected(deltaType, previousQty, resultingQty, avgEntry, feeUsd, slippageUsd, reasonCode);
        log.warn("event=copy_position_accounting_rejected mode={} reasonCode={} symbol={} side={} previousQty={} resultingQty={} executionPrice={} avgEntryPrice={}",
                input == null || input.mode() == null ? "NA" : input.mode(),
                reasonCode,
                input == null ? null : input.symbol(),
                input == null ? null : input.side(),
                previousQty,
                resultingQty,
                input == null ? null : input.executionPrice(),
                avgEntry);
        return result;
    }

    private void logApplied(CopyAccountingInput input, CopyAccountingResult result) {
        log.info("event=copy_position_accounting_applied mode={} deltaType={} symbol={} side={} previousQty={} resultingQty={} deltaAddedQty={} deltaClosedQty={} avgEntryBefore={} avgEntryAfter={} executionPrice={} grossRealizedPnlUsd={} netRealizedPnlUsd={} feeUsd={} slippageUsd={}",
                input == null || input.mode() == null ? "NA" : input.mode(),
                result.deltaType(),
                input == null ? null : input.symbol(),
                input == null ? null : input.side(),
                result.previousQty(),
                result.resultingQty(),
                result.deltaAddedQty(),
                result.deltaClosedQty(),
                result.previousAvgEntryPrice(),
                result.newAvgEntryPrice(),
                input == null ? null : input.executionPrice(),
                result.grossRealizedPnlUsd(),
                result.netRealizedPnlUsd(),
                result.feeUsd(),
                result.slippageUsd());
    }

    private boolean isDirectionalSide(PositionSide side) {
        return side == PositionSide.LONG || side == PositionSide.SHORT;
    }

    private boolean isPositive(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0;
    }

    private BigDecimal nonNegative(BigDecimal value) {
        if (value == null || value.compareTo(ZERO) == 0) {
            return ZERO;
        }
        return value.signum() < 0 ? value.abs() : value;
    }

    private BigDecimal firstPositive(BigDecimal... values) {
        if (values == null) {
            return null;
        }
        for (BigDecimal value : values) {
            if (isPositive(value)) {
                return value;
            }
        }
        return null;
    }

    private BigDecimal normalizeAmount(BigDecimal value) {
        if (value == null) {
            return null;
        }
        BigDecimal stripped = value.stripTrailingZeros();
        return stripped.scale() < 0 ? stripped.setScale(0) : stripped;
    }
}
