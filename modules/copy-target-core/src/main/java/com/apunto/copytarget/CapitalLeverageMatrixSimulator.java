package com.apunto.copytarget;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class CapitalLeverageMatrixSimulator {

    public static final List<BigDecimal> CAPITAL_BANDS = decimals(
            "100", "250", "1000", "5000", "10000", "50000",
            "100000", "250000", "500000", "1000000");
    public static final List<BigDecimal> LEVERAGE_BANDS = decimals("5", "10", "15", "20");

    private final TargetPortfolioCalculator calculator;

    public CapitalLeverageMatrixSimulator(TargetPortfolioCalculator calculator) {
        this.calculator = Objects.requireNonNull(calculator, "calculator");
    }

    public List<CapitalLeverageScenario> simulate(TargetPortfolioRequest sourceSnapshot) {
        Objects.requireNonNull(sourceSnapshot, "sourceSnapshot");
        List<CapitalLeverageScenario> scenarios = new ArrayList<>(CAPITAL_BANDS.size() * LEVERAGE_BANDS.size());
        for (BigDecimal capital : CAPITAL_BANDS) {
            for (BigDecimal leverage : LEVERAGE_BANDS) {
                TargetPortfolioResult result = calculator.calculate(simulationRequest(sourceSnapshot, capital, leverage));
                BigDecimal roundingLoss = result.allLegs().stream()
                        .map(TargetLegDecision::roundingLossUsd)
                        .reduce(BigDecimal.ZERO, BigDecimal::add);
                int minNotionalSkips = (int) result.omittedLegs().stream()
                        .filter(leg -> leg.decisionCode() == DecisionCode.SKIPPED_BELOW_MIN_NOTIONAL)
                        .count();
                scenarios.add(new CapitalLeverageScenario(
                        capital,
                        leverage,
                        result.totalTargetNotionalUsd(),
                        result.totalTargetMarginUsd(),
                        result.selectedLegs().size(),
                        result.omittedLegs().size(),
                        result.movementCoverage(),
                        result.notionalCoverage(),
                        result.exposureCoverage(),
                        roundingLoss,
                        minNotionalSkips,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        modeledEconomicsStatus(result),
                        true,
                        result
                ));
            }
        }
        return List.copyOf(scenarios);
    }

    private String modeledEconomicsStatus(TargetPortfolioResult result) {
        return switch (result.portfolioDecisionCode()) {
            case BLOCKED_SOURCE_EQUITY_MISSING,
                 BLOCKED_SOURCE_EQUITY_STALE,
                 BLOCKED_SOURCE_EQUITY_INVALID,
                 BLOCKED_SOURCE_SNAPSHOT_MISMATCH ->
                    "UNAVAILABLE_" + result.portfolioDecisionCode().name();
            default -> "NOT_CALCULATED";
        };
    }

    private TargetPortfolioRequest simulationRequest(TargetPortfolioRequest source,
                                                     BigDecimal capital,
                                                     BigDecimal leverage) {
        return TargetPortfolioRequest.builder()
                .calculatedAt(source.calculatedAt())
                .sourceAccountEquityUsd(source.sourceAccountEquityUsd())
                .equityObservedAt(source.equityObservedAt())
                .equitySource(source.equitySource())
                .maximumEquityAge(source.maximumEquityAge())
                .sourceSnapshotVersion(source.sourceSnapshotVersion())
                .sourcePositions(source.sourcePositions())
                .targetAllocatedCapitalUsd(capital)
                .targetLeverage(leverage)
                .availableMarginUsd(capital)
                .usedMarginUsd(BigDecimal.ZERO)
                .reservedMarginUsd(BigDecimal.ZERO)
                .existingPositions(List.of())
                .filters(source.filters())
                .quoteAsset(source.quoteAsset())
                .userMaxConcurrentPositions(source.userMaxConcurrentPositions())
                .versions(source.versions())
                .build();
    }

    private static List<BigDecimal> decimals(String... values) {
        return java.util.Arrays.stream(values).map(BigDecimal::new).toList();
    }
}
