package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.CapitalLeverageScenario;
import com.apunto.copytarget.ScenarioEconomicEvidence;
import com.apunto.copytarget.TargetPortfolioResult;

import java.math.BigDecimal;

public record CopySimulationScenarioFact(
        int scenarioIndex,
        BigDecimal capitalUsd,
        BigDecimal targetLeverage,
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
        TargetPortfolioResult targetPortfolio
) {
    public static CopySimulationScenarioFact from(int index, CapitalLeverageScenario scenario) {
        return new CopySimulationScenarioFact(
                index,
                scenario.capitalUsd(),
                scenario.leverage(),
                scenario.targetNotionalUsd(),
                scenario.targetMarginUsd(),
                scenario.positionsCopied(),
                scenario.positionsOmitted(),
                scenario.movementCoverage(),
                scenario.notionalCoverage(),
                scenario.exposureCoverage(),
                scenario.roundingLossUsd(),
                scenario.minNotionalSkips(),
                scenario.feesUsd(),
                scenario.fundingUsd(),
                scenario.slippageUsd(),
                scenario.grossPnlUsd(),
                scenario.netPnlUsd(),
                scenario.drawdownPct(),
                scenario.profitFactor(),
                scenario.liquidationRisk(),
                scenario.modeledEconomicsStatus(),
                scenario.economicEvidence(),
                scenario.targetPortfolio()
        );
    }
}
