package com.apunto.copytarget;

import java.math.BigDecimal;
import java.time.Instant;

public record LiquiditySimulationResult(
        LiquidityExecutionStrategy executionStrategy,
        LiquiditySimulationStatus status,
        BigDecimal requestedNotionalUsd,
        BigDecimal filledNotionalUsd,
        BigDecimal unfilledNotionalUsd,
        BigDecimal bestBookPrice,
        BigDecimal vwap,
        BigDecimal expectedSlippageBps,
        BigDecimal depthConsumedPct,
        BigDecimal fillPercentage,
        long estimatedExecutionMillis,
        BigDecimal marketParticipationPct,
        BigDecimal estimatedFeesUsd,
        BigDecimal estimatedFundingUsd,
        BigDecimal adverseSelectionBps,
        boolean sourceClosedBeforeCompletion,
        LiquidityEvidenceLevel evidenceLevel,
        boolean realValidated,
        Instant orderBookCapturedAt,
        String modelVersion
) {
    public LiquiditySimulationResult {
        if (realValidated) {
            throw new IllegalArgumentException("a modeled liquidity scenario cannot be real validated");
        }
    }
}
