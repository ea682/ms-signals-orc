package com.apunto.engine.service.copy.capital;

import java.math.BigDecimal;

public record AdaptiveCapitalInput(
        String strategyKey,
        CopyExposureAction action,
        String executionMode,
        boolean decisionFinal,
        boolean eligibleForShadow,
        boolean metricsFresh,
        boolean sourceEquityAvailable,
        String copyGuardAction,
        StrategyOperationalState operationalState,
        BigDecimal currentCapitalMultiplier,
        BigDecimal requestedCapitalUsd,
        BigDecimal capacityUsd,
        BigDecimal evidenceScore,
        int healthyConsecutiveEvaluations,
        boolean microLiveEnabled,
        boolean liveEnabled
) {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String strategyKey;
        private CopyExposureAction action;
        private String executionMode;
        private boolean decisionFinal;
        private boolean eligibleForShadow;
        private boolean metricsFresh;
        private boolean sourceEquityAvailable;
        private String copyGuardAction;
        private StrategyOperationalState operationalState;
        private BigDecimal currentCapitalMultiplier;
        private BigDecimal requestedCapitalUsd;
        private BigDecimal capacityUsd;
        private BigDecimal evidenceScore;
        private int healthyConsecutiveEvaluations;
        private boolean microLiveEnabled;
        private boolean liveEnabled;

        public Builder strategyKey(String value) { this.strategyKey = value; return this; }
        public Builder action(CopyExposureAction value) { this.action = value; return this; }
        public Builder executionMode(String value) { this.executionMode = value; return this; }
        public Builder decisionFinal(boolean value) { this.decisionFinal = value; return this; }
        public Builder eligibleForShadow(boolean value) { this.eligibleForShadow = value; return this; }
        public Builder metricsFresh(boolean value) { this.metricsFresh = value; return this; }
        public Builder sourceEquityAvailable(boolean value) { this.sourceEquityAvailable = value; return this; }
        public Builder copyGuardAction(String value) { this.copyGuardAction = value; return this; }
        public Builder operationalState(StrategyOperationalState value) { this.operationalState = value; return this; }
        public Builder currentCapitalMultiplier(BigDecimal value) { this.currentCapitalMultiplier = value; return this; }
        public Builder requestedCapitalUsd(BigDecimal value) { this.requestedCapitalUsd = value; return this; }
        public Builder capacityUsd(BigDecimal value) { this.capacityUsd = value; return this; }
        public Builder evidenceScore(BigDecimal value) { this.evidenceScore = value; return this; }
        public Builder healthyConsecutiveEvaluations(int value) { this.healthyConsecutiveEvaluations = value; return this; }
        public Builder microLiveEnabled(boolean value) { this.microLiveEnabled = value; return this; }
        public Builder liveEnabled(boolean value) { this.liveEnabled = value; return this; }

        public AdaptiveCapitalInput build() {
            return new AdaptiveCapitalInput(strategyKey, action, executionMode, decisionFinal,
                    eligibleForShadow, metricsFresh, sourceEquityAvailable, copyGuardAction,
                    operationalState, currentCapitalMultiplier, requestedCapitalUsd, capacityUsd,
                    evidenceScore, healthyConsecutiveEvaluations, microLiveEnabled, liveEnabled);
        }
    }
}

