package com.apunto.engine.service.copy.dispatch;

import java.math.BigDecimal;
import java.util.Objects;

public final class MicroLiveBudgetPolicy {

    private final BigDecimal maxMarginPerOrderUsd;
    private final BigDecimal maxTotalMarginUsd;
    private final int maxPositions;

    public MicroLiveBudgetPolicy(BigDecimal maxMarginPerOrderUsd, BigDecimal maxTotalMarginUsd, int maxPositions) {
        this.maxMarginPerOrderUsd = positive(maxMarginPerOrderUsd, "maxMarginPerOrderUsd");
        this.maxTotalMarginUsd = positive(maxTotalMarginUsd, "maxTotalMarginUsd");
        if (maxPositions <= 0) throw new IllegalArgumentException("maxPositions must be positive");
        this.maxPositions = maxPositions;
    }

    public BudgetDecision evaluate(BudgetSnapshot snapshot, BigDecimal requestedMarginUsd, boolean reservePosition) {
        BudgetSnapshot current = Objects.requireNonNullElse(snapshot, BudgetSnapshot.empty());
        BigDecimal requested = requestedMarginUsd == null ? BigDecimal.ZERO : requestedMarginUsd.max(BigDecimal.ZERO);
        BigDecimal currentTotal = current.usedMarginUsd().add(current.reservedPendingMarginUsd());
        BigDecimal projected = currentTotal.add(requested);
        int projectedPositions = current.openPositions() + current.reservedPositions() + (reservePosition ? 1 : 0);

        if (requested.compareTo(maxMarginPerOrderUsd) > 0) {
            return new BudgetDecision(false, "MICRO_LIVE_ORDER_MARGIN_EXCEEDED", projected, projectedPositions);
        }
        if (projected.compareTo(maxTotalMarginUsd) > 0) {
            return new BudgetDecision(false, "MICRO_LIVE_TOTAL_MARGIN_EXCEEDED", projected, projectedPositions);
        }
        if (projectedPositions > maxPositions) {
            return new BudgetDecision(false, "MICRO_LIVE_POSITION_LIMIT_EXCEEDED", projected, projectedPositions);
        }
        return new BudgetDecision(true, "MICRO_LIVE_BUDGET_RESERVED", projected, projectedPositions);
    }

    public BudgetDecision evaluateDuplicate(BudgetSnapshot snapshot) {
        BudgetSnapshot current = Objects.requireNonNullElse(snapshot, BudgetSnapshot.empty());
        return new BudgetDecision(true, "DUPLICATE_REUSES_EXISTING_RESERVATION",
                current.usedMarginUsd().add(current.reservedPendingMarginUsd()),
                current.openPositions() + current.reservedPositions());
    }

    // IMPORTANT:
    // MICRO_LIVE limits margin, not leveraged notional. Pending/ambiguous orders
    // keep their reservation until reconciliation so retries cannot exceed the
    // allocation budget.
    private BigDecimal positive(BigDecimal value, String field) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }
}
