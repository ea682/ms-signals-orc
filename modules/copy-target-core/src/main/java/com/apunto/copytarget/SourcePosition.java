package com.apunto.copytarget;

import java.math.BigDecimal;
import java.util.Objects;

public record SourcePosition(
        String sourceLegId,
        String sourceSymbol,
        String targetSymbol,
        SourceSide side,
        BigDecimal quantity,
        BigDecimal notionalUsd,
        BigDecimal markPrice,
        BigDecimal entryPrice,
        BigDecimal leverage,
        long snapshotVersion,
        BigDecimal liquidityScore
) {
    public SourcePosition {
        sourceLegId = required(sourceLegId, "sourceLegId");
        sourceSymbol = required(sourceSymbol, "sourceSymbol");
        targetSymbol = targetSymbol == null ? null : targetSymbol.trim().toUpperCase();
        side = Objects.requireNonNull(side, "side");
        quantity = DecimalSupport.nonNegative(quantity, "quantity");
        notionalUsd = DecimalSupport.nonNegative(notionalUsd, "notionalUsd");
        markPrice = DecimalSupport.nonNegative(markPrice, "markPrice");
        entryPrice = DecimalSupport.nonNegative(entryPrice, "entryPrice");
        leverage = DecimalSupport.nonNegative(leverage, "leverage");
        liquidityScore = DecimalSupport.nonNegativeOrZero(liquidityScore);
    }

    private static String required(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
