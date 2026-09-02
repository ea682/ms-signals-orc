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
        MarginProvenance marginProvenance,
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
        marginProvenance = Objects.requireNonNull(marginProvenance, "marginProvenance");
        marginUsedUsd = marginProvenance.usableForEntrySizing()
                ? DecimalSupport.nonNegative(marginUsedUsd, "marginUsedUsd")
                : null;
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
            BigDecimal marginUsedUsd,
            BigDecimal markPrice,
            BigDecimal entryPrice,
            BigDecimal leverage,
            long snapshotVersion,
            BigDecimal liquidityScore
    ) {
        this(sourceLegId, sourceSymbol, targetSymbol, side, quantity, notionalUsd,
                marginUsedUsd, MarginProvenance.EXPLICIT, markPrice, entryPrice, leverage,
                snapshotVersion, liquidityScore);
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
                null, MarginProvenance.UNAVAILABLE, markPrice, entryPrice, leverage,
                snapshotVersion, liquidityScore);
    }

    public static SourcePosition derivedCertified(
            String sourceLegId,
            String sourceSymbol,
            String targetSymbol,
            SourceSide side,
            BigDecimal quantity,
            BigDecimal certifiedNotionalUsd,
            BigDecimal markPrice,
            BigDecimal entryPrice,
            BigDecimal certifiedLeverage,
            long snapshotVersion,
            BigDecimal liquidityScore
    ) {
        if (certifiedNotionalUsd == null) {
            throw new IllegalArgumentException("certifiedNotionalUsd must not be null");
        }
        if (certifiedLeverage == null || certifiedLeverage.signum() <= 0) {
            throw new IllegalArgumentException("certifiedLeverage must be positive");
        }
        BigDecimal derivedMargin = DecimalSupport.divideDown(
                certifiedNotionalUsd.abs(), certifiedLeverage);
        return new SourcePosition(
                sourceLegId, sourceSymbol, targetSymbol, side, quantity,
                certifiedNotionalUsd, derivedMargin, MarginProvenance.DERIVED_CERTIFIED,
                markPrice, entryPrice, certifiedLeverage, snapshotVersion, liquidityScore);
    }

    private static String required(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
