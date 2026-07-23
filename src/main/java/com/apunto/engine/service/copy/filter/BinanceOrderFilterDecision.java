package com.apunto.engine.service.copy.filter;

import java.math.BigDecimal;

public record BinanceOrderFilterDecision(
        boolean sendOrder,
        boolean reconciliationRequired,
        String reasonCode,
        BigDecimal requestedQty,
        BigDecimal cappedQty,
        BigDecimal roundedQty,
        BigDecimal roundedNotional,
        BinanceMarketOrderRules rules
) {
}
