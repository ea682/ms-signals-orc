package com.apunto.copytarget;

import java.math.BigDecimal;
import java.util.Objects;

public record BinanceSymbolFilter(
        String symbol,
        boolean trading,
        String quoteAsset,
        BigDecimal minQty,
        BigDecimal maxQty,
        BigDecimal stepSize,
        BigDecimal minNotional,
        BigDecimal tickSize,
        BigDecimal maximumLeverage,
        BigDecimal liquidityScore
) {
    public BinanceSymbolFilter {
        Objects.requireNonNull(symbol, "symbol");
        Objects.requireNonNull(quoteAsset, "quoteAsset");
        symbol = symbol.trim().toUpperCase();
        quoteAsset = quoteAsset.trim().toUpperCase();
        if (symbol.isBlank() || quoteAsset.isBlank()) {
            throw new IllegalArgumentException("symbol and quoteAsset must not be blank");
        }
        minQty = DecimalSupport.nonNegativeOrZero(minQty);
        maxQty = DecimalSupport.nonNegativeOrZero(maxQty);
        stepSize = DecimalSupport.nonNegativeOrZero(stepSize);
        minNotional = DecimalSupport.nonNegativeOrZero(minNotional);
        tickSize = DecimalSupport.nonNegativeOrZero(tickSize);
        maximumLeverage = DecimalSupport.nonNegativeOrZero(maximumLeverage);
        liquidityScore = DecimalSupport.nonNegativeOrZero(liquidityScore);
    }
}
