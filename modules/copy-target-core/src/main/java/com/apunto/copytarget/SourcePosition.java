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
        BigDecimal marginUsedUsd,
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
        marginUsedUsd = DecimalSupport.nonNegative(marginUsedUsd, "marginUsedUsd");
        markPrice = DecimalSupport.nonNegative(markPrice, "markPrice");
        entryPrice = DecimalSupport.nonNegative(entryPrice, "entryPrice");
        leverage = DecimalSupport.nonNegative(leverage, "leverage");
        liquidityScore = DecimalSupport.nonNegativeOrZero(liquidityScore);
    }

    public SourcePosition(
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
        this(sourceLegId, sourceSymbol, targetSymbol, side, quantity, notionalUsd,
                marginFrom(notionalUsd, leverage), markPrice, entryPrice, leverage,
                snapshotVersion, liquidityScore);
    }

    private static BigDecimal marginFrom(BigDecimal notionalUsd, BigDecimal leverage) {
        if (notionalUsd == null || leverage == null || leverage.signum() <= 0) {
            return BigDecimal.ZERO;
        }
        return DecimalSupport.divideDown(notionalUsd.abs(), leverage);
    }

    private static String required(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
