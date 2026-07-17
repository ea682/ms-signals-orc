package com.apunto.copytarget;

import java.math.BigDecimal;
import java.util.Objects;

public record TargetLegDecision(
        String sourceLegId,
        String sourceSymbol,
        String targetSymbol,
        SourceSide side,
        boolean selected,
        DecisionCode decisionCode,
        String reasonDetail,
        BigDecimal sourceExposureRatio,
        BigDecimal rawTargetNotionalUsd,
        BigDecimal targetNotionalUsd,
        BigDecimal targetMarginUsd,
        BigDecimal rawQuantity,
        BigDecimal roundedQuantity,
        BigDecimal targetQuantity,
        BigDecimal existingQuantity,
        BigDecimal deltaQuantity,
        DeltaAction deltaAction,
        BigDecimal roundingLossUsd,
        BigDecimal liquidityScore,
        boolean waitsForOppositeClose
) {
    public TargetLegDecision {
        sourceLegId = sourceLegId == null ? "" : sourceLegId;
        sourceSymbol = sourceSymbol == null ? "" : sourceSymbol;
        targetSymbol = targetSymbol == null ? "" : targetSymbol;
        side = Objects.requireNonNull(side, "side");
        decisionCode = Objects.requireNonNull(decisionCode, "decisionCode");
        reasonDetail = reasonDetail == null ? "" : reasonDetail;
        sourceExposureRatio = DecimalSupport.nonNegativeOrZero(sourceExposureRatio);
        rawTargetNotionalUsd = DecimalSupport.nonNegativeOrZero(rawTargetNotionalUsd);
        targetNotionalUsd = DecimalSupport.nonNegativeOrZero(targetNotionalUsd);
        targetMarginUsd = DecimalSupport.nonNegativeOrZero(targetMarginUsd);
        rawQuantity = DecimalSupport.nonNegativeOrZero(rawQuantity);
        roundedQuantity = DecimalSupport.nonNegativeOrZero(roundedQuantity);
        targetQuantity = DecimalSupport.nonNegativeOrZero(targetQuantity);
        existingQuantity = DecimalSupport.nonNegativeOrZero(existingQuantity);
        deltaQuantity = DecimalSupport.normalize(deltaQuantity);
        deltaAction = Objects.requireNonNull(deltaAction, "deltaAction");
        roundingLossUsd = DecimalSupport.nonNegativeOrZero(roundingLossUsd);
        liquidityScore = DecimalSupport.nonNegativeOrZero(liquidityScore);
    }
}
