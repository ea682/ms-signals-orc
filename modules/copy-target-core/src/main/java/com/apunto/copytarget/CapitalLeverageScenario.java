package com.apunto.copytarget;

import java.math.BigDecimal;

public record CapitalLeverageScenario(
        BigDecimal capitalUsd,
        BigDecimal leverage,
        BigDecimal targetNotionalUsd,
        BigDecimal targetMarginUsd,
        int positionsCopied,
        int positionsOmitted,
        BigDecimal movementCoverage,
        BigDecimal notionalCoverage,
        BigDecimal exposureCoverage,
        BigDecimal roundingLossUsd,
        int minNotionalSkips,
        BigDecimal feesUsd,
        BigDecimal fundingUsd,
        BigDecimal slippageUsd,
        BigDecimal grossPnlUsd,
        BigDecimal netPnlUsd,
        BigDecimal drawdownPct,
        BigDecimal profitFactor,
        BigDecimal liquidationRisk,
        String modeledEconomicsStatus,
        ScenarioEconomicEvidence economicEvidence,
        boolean simulationOnly,
        TargetPortfolioResult targetPortfolio
) {
}
