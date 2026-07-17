package com.apunto.engine.service.copy.capital;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class AdaptiveCapitalDecisionEngine {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal QUARTER = new BigDecimal("0.25");
    private static final BigDecimal MIN_EVIDENCE = new BigDecimal("15");

    public AdaptiveCapitalDecision evaluate(AdaptiveCapitalInput input) {
        if (input == null || input.action() == null) {
            return blocked(StrategyOperationalState.CANDIDATE, BigDecimal.ZERO,
                    false, false, "ADAPTIVE_CAPITAL_INPUT_INVALID");
        }
        StrategyOperationalState state = input.operationalState() == null
                ? StrategyOperationalState.CANDIDATE : input.operationalState();
        BigDecimal multiplier = normalizedMultiplier(input.currentCapitalMultiplier());

        if (input.action() == CopyExposureAction.CLOSE || input.action() == CopyExposureAction.REDUCE) {
            return new AdaptiveCapitalDecision(true, true, false, false,
                    state == StrategyOperationalState.PAUSED || state == StrategyOperationalState.RETIRED
                            ? ZERO : multiplier,
                    state, List.of("EXIT_ALWAYS_ALLOWED"));
        }

        List<String> reasons = new ArrayList<>();
        String mode = normalize(input.executionMode());
        boolean realMode = "MICRO_LIVE".equals(mode) || "LIVE".equals(mode);
        if ("MICRO_LIVE".equals(mode) && !input.microLiveEnabled()) reasons.add("MICRO_LIVE_DISABLED");
        if ("LIVE".equals(mode) && !input.liveEnabled()) reasons.add("LIVE_DISABLED");
        if (!input.decisionFinal()) reasons.add("SUMMARY_REQUIRES_FULL_SIMULATION");
        if (!input.eligibleForShadow()) reasons.add("FULL_NOT_ELIGIBLE_FOR_SHADOW");
        if (!input.metricsFresh()) reasons.add("METRIC_DECISION_STALE");
        if (!input.sourceEquityAvailable()) reasons.add("SOURCE_EQUITY_REQUIRED_FOR_NEW_EXPOSURE");
        if (guardBlocks(input.copyGuardAction())) reasons.add("COPY_GUARD_BLOCKS_NEW_EXPOSURE");
        if (state == StrategyOperationalState.PAUSED) reasons.add("STRATEGY_PAUSED");
        if (state == StrategyOperationalState.RETIRED) reasons.add("STRATEGY_RETIRED");
        if (input.evidenceScore() == null || input.evidenceScore().compareTo(MIN_EVIDENCE) < 0) {
            reasons.add("EVIDENCE_INSUFFICIENT");
        }
        if (input.capacityUsd() == null && realMode) {
            reasons.add("CAPACITY_UNKNOWN");
        } else if (realMode && (input.requestedCapitalUsd() == null || input.requestedCapitalUsd().signum() <= 0)) {
            reasons.add("REQUESTED_CAPITAL_INVALID");
        } else if (realMode && input.requestedCapitalUsd().compareTo(input.capacityUsd()) > 0) {
            reasons.add("CAPACITY_EXCEEDED");
        }

        boolean flip = input.action() == CopyExposureAction.FLIP;
        if (!reasons.isEmpty()) {
            return new AdaptiveCapitalDecision(false, flip, false, false, ZERO, state,
                    List.copyOf(reasons));
        }

        if (state == StrategyOperationalState.WATCH
                && input.healthyConsecutiveEvaluations() >= 2) {
            multiplier = multiplier.add(QUARTER).min(BigDecimal.ONE).setScale(2, RoundingMode.DOWN);
            reasons.add("GRADUAL_RECOVERY_ONE_STEP");
        } else if (state == StrategyOperationalState.WATCH) {
            reasons.add("RECOVERY_PERSISTENCE_REQUIRED");
        } else {
            reasons.add("ADAPTIVE_CAPITAL_ALLOW");
        }
        if (input.capacityUsd() == null && !realMode) {
            reasons.add("CAPACITY_UNKNOWN_SHADOW_ONLY");
        }

        boolean allowsMoney = ("MICRO_LIVE".equals(mode) && input.microLiveEnabled())
                || ("LIVE".equals(mode) && input.liveEnabled());
        return new AdaptiveCapitalDecision(true, flip, flip || input.action() != CopyExposureAction.FLIP,
                allowsMoney, multiplier, state, List.copyOf(reasons));
    }

    public BigDecimal targetNotional(BigDecimal allocatedCapital,
                                     BigDecimal sourceExposureRatio,
                                     BigDecimal capitalMultiplier,
                                     BigDecimal targetLeverage) {
        if (allocatedCapital == null || sourceExposureRatio == null || capitalMultiplier == null
                || targetLeverage == null || allocatedCapital.signum() < 0
                || sourceExposureRatio.signum() < 0 || capitalMultiplier.signum() < 0
                || targetLeverage.signum() <= 0) {
            throw new IllegalArgumentException("INVALID_TARGET_NOTIONAL_INPUT");
        }
        return allocatedCapital.multiply(sourceExposureRatio).multiply(capitalMultiplier)
                .setScale(12, RoundingMode.DOWN).stripTrailingZeros();
    }

    private AdaptiveCapitalDecision blocked(StrategyOperationalState state, BigDecimal multiplier,
                                            boolean close, boolean open, String reason) {
        return new AdaptiveCapitalDecision(false, close, open, false, multiplier, state, List.of(reason));
    }

    private boolean guardBlocks(String action) {
        String normalized = normalize(action);
        return !("ALLOW".equals(normalized) || "WARNING".equals(normalized));
    }

    private BigDecimal normalizedMultiplier(BigDecimal value) {
        if (value == null || value.signum() < 0) return ZERO;
        BigDecimal capped = value.min(BigDecimal.ONE);
        BigDecimal steps = capped.divide(QUARTER, 0, RoundingMode.DOWN);
        return steps.multiply(QUARTER).setScale(2, RoundingMode.DOWN);
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
