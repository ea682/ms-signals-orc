package com.apunto.copytarget;

import java.math.BigDecimal;
import java.util.Objects;

public record ExistingTargetPosition(
        String symbol,
        SourceSide side,
        BigDecimal quantity,
        BigDecimal markPrice,
        BigDecimal marginUsd
) {
    public ExistingTargetPosition {
        Objects.requireNonNull(symbol, "symbol");
        symbol = symbol.trim().toUpperCase();
        if (symbol.isBlank()) {
            throw new IllegalArgumentException("symbol must not be blank");
        }
        side = Objects.requireNonNull(side, "side");
        quantity = DecimalSupport.nonNegative(quantity, "quantity");
        markPrice = DecimalSupport.nonNegativeOrZero(markPrice);
        marginUsd = DecimalSupport.nonNegativeOrZero(marginUsd);
    }

    String key() {
        return symbol + "|" + side;
    }
}
