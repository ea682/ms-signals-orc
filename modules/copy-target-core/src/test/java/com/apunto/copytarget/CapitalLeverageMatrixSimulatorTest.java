package com.apunto.copytarget;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CapitalLeverageMatrixSimulatorTest {

    @Test
    void producesTheExactTenByFourColdMatrixWithoutChangingTheRealRequest() {
        TargetPortfolioRequest real = request(new BigDecimal("100"), new BigDecimal("5"));

        List<CapitalLeverageScenario> matrix = new CapitalLeverageMatrixSimulator(
                new TargetPortfolioCalculator()).simulate(real);

        assertEquals(40, matrix.size());
        assertEquals(0, matrix.getFirst().capitalUsd().compareTo(new BigDecimal("100")));
        assertEquals(0, matrix.getFirst().leverage().compareTo(new BigDecimal("5")));
        assertEquals(0, matrix.getLast().capitalUsd().compareTo(new BigDecimal("1000000")));
        assertEquals(0, matrix.getLast().leverage().compareTo(new BigDecimal("20")));
        assertEquals(0, real.targetAllocatedCapitalUsd().compareTo(new BigDecimal("100")));
        assertEquals(0, real.targetLeverage().compareTo(new BigDecimal("5")));
        assertTrue(matrix.stream().allMatch(CapitalLeverageScenario::simulationOnly));
    }

    @Test
    void microLiveBandMatchesTheSamePureCalculatorAtOneHundredAndFiveX() {
        TargetPortfolioRequest request = request(new BigDecimal("100"), new BigDecimal("5"));
        TargetPortfolioResult direct = new TargetPortfolioCalculator().calculate(request);

        CapitalLeverageScenario scenario = new CapitalLeverageMatrixSimulator(
                new TargetPortfolioCalculator()).simulate(request).getFirst();

        assertEquals(0, direct.totalTargetNotionalUsd().compareTo(scenario.targetNotionalUsd()));
        assertEquals(0, direct.totalTargetMarginUsd().compareTo(scenario.targetMarginUsd()));
        assertEquals(direct.selectedLegs().size(), scenario.positionsCopied());
        assertEquals(direct.omittedLegs().size(), scenario.positionsOmitted());
    }

    @Test
    void missingEquityMakesEveryBandUnavailableWithoutFabricatingEconomics() {
        TargetPortfolioRequest request = request(
                new BigDecimal("100"), new BigDecimal("5"), null);

        List<CapitalLeverageScenario> matrix = new CapitalLeverageMatrixSimulator(
                new TargetPortfolioCalculator()).simulate(request);

        assertEquals(40, matrix.size());
        assertTrue(matrix.stream().allMatch(scenario ->
                scenario.targetPortfolio().portfolioDecisionCode()
                        == DecisionCode.BLOCKED_SOURCE_EQUITY_MISSING));
        assertTrue(matrix.stream().allMatch(scenario ->
                "UNAVAILABLE_BLOCKED_SOURCE_EQUITY_MISSING".equals(
                        scenario.modeledEconomicsStatus())));
        assertTrue(matrix.stream().allMatch(scenario ->
                scenario.movementCoverage().signum() == 0
                        && scenario.notionalCoverage().signum() == 0
                        && scenario.exposureCoverage().signum() == 0));
        assertTrue(matrix.stream().allMatch(scenario ->
                scenario.positionsCopied() == 0 && scenario.positionsOmitted() == 1));
        assertTrue(matrix.stream().allMatch(scenario -> scenario.netPnlUsd() == null));
    }

    private TargetPortfolioRequest request(BigDecimal capital, BigDecimal leverage) {
        return request(capital, leverage, new BigDecimal("500000"));
    }

    private TargetPortfolioRequest request(BigDecimal capital,
                                           BigDecimal leverage,
                                           BigDecimal equity) {
        Instant now = Instant.parse("2026-07-13T00:00:00Z");
        return TargetPortfolioRequest.builder()
                .calculatedAt(now)
                .sourceAccountEquityUsd(equity)
                .equityObservedAt(now.minusSeconds(1))
                .equitySource("hyperliquid.marginSummary.accountValue")
                .maximumEquityAge(Duration.ofSeconds(30))
                .sourceSnapshotVersion(7)
                .sourcePositions(List.of(new SourcePosition(
                        "leg-1", "BTC", "BTCUSDT", SourceSide.LONG,
                        new BigDecimal("1"), new BigDecimal("100000"),
                        new BigDecimal("100000"), new BigDecimal("90000"),
                        new BigDecimal("5"), 7L, new BigDecimal("100"))))
                .targetAllocatedCapitalUsd(capital)
                .targetLeverage(leverage)
                .availableMarginUsd(capital)
                .filters(List.of(new BinanceSymbolFilter(
                        "BTCUSDT", true, "USDT", new BigDecimal("0.001"),
                        new BigDecimal("100"), new BigDecimal("0.001"),
                        new BigDecimal("5"), new BigDecimal("0.1"),
                        new BigDecimal("125"), new BigDecimal("100"))))
                .quoteAsset("USDT")
                .versions(new CalculationVersions("strategy-v3", "sizing-v3", "symbols-v3"))
                .build();
    }
}
