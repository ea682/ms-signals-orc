package com.apunto.engine.service.copy.dispatch;

import java.math.BigDecimal;

public record BudgetSnapshot(
        BigDecimal usedMarginUsd,
        BigDecimal reservedPendingMarginUsd,
        int openPositions,
        int reservedPositions
) {
    public BudgetSnapshot {
        usedMarginUsd = usedMarginUsd == null ? BigDecimal.ZERO : usedMarginUsd.max(BigDecimal.ZERO);
        reservedPendingMarginUsd = reservedPendingMarginUsd == null ? BigDecimal.ZERO : reservedPendingMarginUsd.max(BigDecimal.ZERO);
        openPositions = Math.max(0, openPositions);
        reservedPositions = Math.max(0, reservedPositions);
    }

    public static BudgetSnapshot empty() {
        return new BudgetSnapshot(BigDecimal.ZERO, BigDecimal.ZERO, 0, 0);
    }
}
