package com.apunto.copytarget;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

public record TargetPortfolioResult(
        DecisionCode portfolioDecisionCode,
        boolean entrySizingAllowed,
        BigDecimal portfolioScaleFactor,
        BigDecimal availableTargetMarginUsd,
        BigDecimal totalRawTargetNotionalUsd,
        BigDecimal totalTargetNotionalUsd,
        BigDecimal totalTargetMarginUsd,
        BigDecimal movementCoverage,
        BigDecimal notionalCoverage,
        BigDecimal exposureCoverage,
        List<TargetLegDecision> selectedLegs,
        List<TargetLegDecision> omittedLegs,
        CalculationVersions versions,
        long sourceSnapshotVersion,
        Instant calculatedAt
) {
    private static final Comparator<TargetLegDecision> ORDER = Comparator
            .comparing(TargetLegDecision::targetSymbol)
            .thenComparing(decision -> decision.side().name())
            .thenComparing(TargetLegDecision::sourceLegId);

    public TargetPortfolioResult {
        portfolioScaleFactor = DecimalSupport.nonNegativeOrZero(portfolioScaleFactor);
        availableTargetMarginUsd = DecimalSupport.nonNegativeOrZero(availableTargetMarginUsd);
        totalRawTargetNotionalUsd = DecimalSupport.nonNegativeOrZero(totalRawTargetNotionalUsd);
        totalTargetNotionalUsd = DecimalSupport.nonNegativeOrZero(totalTargetNotionalUsd);
        totalTargetMarginUsd = DecimalSupport.nonNegativeOrZero(totalTargetMarginUsd);
        movementCoverage = DecimalSupport.nonNegativeOrZero(movementCoverage);
        notionalCoverage = DecimalSupport.nonNegativeOrZero(notionalCoverage);
        exposureCoverage = DecimalSupport.nonNegativeOrZero(exposureCoverage);
        selectedLegs = selectedLegs.stream().sorted(ORDER).toList();
        omittedLegs = omittedLegs.stream().sorted(ORDER).toList();
    }

    public List<TargetLegDecision> allLegs() {
        List<TargetLegDecision> all = new ArrayList<>(selectedLegs.size() + omittedLegs.size());
        all.addAll(selectedLegs);
        all.addAll(omittedLegs);
        return all.stream().sorted(ORDER).toList();
    }

    public TargetLegDecision leg(String symbol, SourceSide side) {
        return allLegs().stream()
                .filter(value -> value.targetSymbol().equalsIgnoreCase(symbol) && value.side() == side)
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException(symbol + " " + side));
    }
}
