package com.apunto.copytarget;

import java.math.BigDecimal;

public record LiquiditySimulationAssumptions(
        BigDecimal maximumDepthConsumptionPct,
        BigDecimal participationCapPct,
        int fragmentCount,
        long intervalMillis,
        BigDecimal disappearingLiquidityPct,
        BigDecimal adverseSelectionBps,
        long networkLatencyMillis,
        Long sourceCloseAfterMillis,
        BigDecimal takerFeeBps,
        BigDecimal fundingBpsPerEightHours
) {
    public LiquiditySimulationAssumptions {
        maximumDepthConsumptionPct = percentage(maximumDepthConsumptionPct, "maximumDepthConsumptionPct");
        participationCapPct = percentage(participationCapPct, "participationCapPct");
        disappearingLiquidityPct = percentage(disappearingLiquidityPct, "disappearingLiquidityPct");
        adverseSelectionBps = DecimalSupport.nonNegativeOrZero(adverseSelectionBps);
        takerFeeBps = DecimalSupport.nonNegativeOrZero(takerFeeBps);
        fundingBpsPerEightHours = DecimalSupport.nonNegativeOrZero(fundingBpsPerEightHours);
        if (fragmentCount <= 0) {
            throw new IllegalArgumentException("fragmentCount must be positive");
        }
        if (intervalMillis < 0 || networkLatencyMillis < 0) {
            throw new IllegalArgumentException("latencies must not be negative");
        }
        if (sourceCloseAfterMillis != null && sourceCloseAfterMillis < 0) {
            throw new IllegalArgumentException("sourceCloseAfterMillis must not be negative");
        }
    }

    private static BigDecimal percentage(BigDecimal value, String name) {
        BigDecimal normalized = DecimalSupport.nonNegativeOrZero(value);
        if (normalized.compareTo(BigDecimal.ONE) > 0) {
            throw new IllegalArgumentException(name + " must be between zero and one");
        }
        return normalized;
    }
}
