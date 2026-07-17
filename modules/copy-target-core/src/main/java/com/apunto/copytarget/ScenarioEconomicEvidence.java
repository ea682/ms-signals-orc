package com.apunto.copytarget;

import java.math.BigDecimal;
import java.util.List;

public record ScenarioEconomicEvidence(
        BigDecimal turnoverUsd,
        BigDecimal expectancyNetPerCycle,
        BigDecimal fillRatio,
        BigDecimal missedMovementPnlUsd,
        BigDecimal venueBasisPnlUsd,
        BigDecimal latencyPnlUsd,
        List<BigDecimal> latencyGridSeconds,
        BigDecimal breakEvenLatencySeconds,
        BigDecimal edgeHalfLifeSeconds,
        BigDecimal normalCapacityUsd,
        BigDecimal stressCapacityUsd,
        BigDecimal emergencyExitCapacityUsd,
        BigDecimal liquidationSurvivalRate,
        BigDecimal minimumLiquidationDistancePct,
        BigDecimal marginStressP95,
        BigDecimal marginStressP99,
        BigDecimal leverageRobustnessScore,
        BigDecimal executionConfidence,
        String status,
        List<String> reasonCodes
) {
    public ScenarioEconomicEvidence {
        latencyGridSeconds = latencyGridSeconds == null ? List.of() : List.copyOf(latencyGridSeconds);
        reasonCodes = reasonCodes == null ? List.of() : reasonCodes.stream().distinct().toList();
    }
}

