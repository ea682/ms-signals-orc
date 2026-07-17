package com.apunto.engine.service.copy.dispatch;

import java.math.BigDecimal;
import java.util.Objects;

public final class MicroLiveBudgetPolicy {

    private final BigDecimal maxTotalMarginUsd;
    private final Integer userMaxPositions;

    public MicroLiveBudgetPolicy(BigDecimal maxTotalMarginUsd, Integer userMaxPositions) {
        this.maxTotalMarginUsd = positive(maxTotalMarginUsd, "maxTotalMarginUsd");
        if (userMaxPositions != null && userMaxPositions <= 0) {
            throw new IllegalArgumentException("userMaxPositions must be null or positive");
        }
        this.userMaxPositions = userMaxPositions;
    }

    /**
     * Compatibility constructor for callers compiled against V2. The legacy
     * per-order and global position values are deliberately ignored.
     */
    @Deprecated(forRemoval = false)
    public MicroLiveBudgetPolicy(BigDecimal legacyMaxMarginPerOrderUsd,
                                 BigDecimal maxTotalMarginUsd,
                                 int legacyGlobalMaxPositions) {
        this(maxTotalMarginUsd, null);
    }

    public BudgetDecision evaluate(BudgetSnapshot snapshot, BigDecimal requestedMarginUsd, boolean reservePosition) {
        BudgetSnapshot current = Objects.requireNonNullElse(snapshot, BudgetSnapshot.empty());
        BigDecimal requested = requestedMarginUsd == null ? BigDecimal.ZERO : requestedMarginUsd.max(BigDecimal.ZERO);
        BigDecimal currentTotal = current.usedMarginUsd().add(current.reservedPendingMarginUsd());
        BigDecimal projected = currentTotal.add(requested);
        int projectedPositions = current.openPositions() + current.reservedPositions() + (reservePosition ? 1 : 0);

        if (!reservePosition && requested.compareTo(BigDecimal.ZERO) == 0) {
            return new BudgetDecision(true, "MICRO_LIVE_EXIT_ALWAYS_ALLOWED", currentTotal,
                    current.openPositions() + current.reservedPositions());
        }
        if (projected.compareTo(maxTotalMarginUsd) > 0) {
            return new BudgetDecision(false, "MICRO_LIVE_TOTAL_MARGIN_EXCEEDED", projected, projectedPositions);
        }
        if (userMaxPositions != null && projectedPositions > userMaxPositions) {
            return new BudgetDecision(false, "SKIPPED_USER_POSITION_LIMIT", projected, projectedPositions);
        }
        return new BudgetDecision(true, "MICRO_LIVE_PORTFOLIO_MARGIN_RESERVED", projected, projectedPositions);
    }

    public BudgetDecision evaluateDuplicate(BudgetSnapshot snapshot) {
        BudgetSnapshot current = Objects.requireNonNullElse(snapshot, BudgetSnapshot.empty());
        return new BudgetDecision(true, "DUPLICATE_REUSES_EXISTING_RESERVATION",
                current.usedMarginUsd().add(current.reservedPendingMarginUsd()),
                current.openPositions() + current.reservedPositions());
    }

    private static BigDecimal positive(BigDecimal value, String field) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }
}
