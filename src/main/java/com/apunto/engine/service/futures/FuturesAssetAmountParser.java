package com.apunto.engine.service.futures;

import java.math.BigDecimal;

public final class FuturesAssetAmountParser {

    private FuturesAssetAmountParser() {
    }

    public static BigDecimal positiveOrZero(String raw) {
        if (raw == null || raw.isBlank()) {
            return BigDecimal.ZERO;
        }
        try {
            BigDecimal value = new BigDecimal(raw.trim());
            return value.compareTo(BigDecimal.ZERO) > 0 ? value : BigDecimal.ZERO;
        } catch (NumberFormatException ex) {
            return BigDecimal.ZERO;
        }
    }
}
