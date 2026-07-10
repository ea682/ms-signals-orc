package com.apunto.engine.service.copy.dispatch;

import java.math.BigDecimal;

public record BudgetDecision(
        boolean allowed,
        String reasonCode,
        BigDecimal projectedMarginUsd,
        int projectedPositions
) {
}
