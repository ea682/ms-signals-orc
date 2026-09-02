package com.apunto.copytarget;

import java.math.BigDecimal;

/** Incremental sizing boundary. Metrica remains owner of all economic state transitions. */
public final class CopyPolicyV1SizingStepRunner {

    public static final String POLICY_VERSION = "copy-policy-v1";
    private static final BigDecimal HISTORICAL_LEVERAGE = new BigDecimal("5");
    private static final BigDecimal MIN_LIVE_LEVERAGE = BigDecimal.ONE;
    private static final BigDecimal MAX_LIVE_LEVERAGE = new BigDecimal("20");

    private final TargetPortfolioCalculator calculator;

    public CopyPolicyV1SizingStepRunner() {
        this(new TargetPortfolioCalculator());
    }

    CopyPolicyV1SizingStepRunner(TargetPortfolioCalculator calculator) {
        this.calculator = calculator;
    }

    public CopyPolicyV1SizingStepResult run(CopyPolicyV1SizingStepRequest step) {
        TargetPortfolioRequest supplied = step.sizingInput();
        if (!POLICY_VERSION.equals(supplied.versions().sizingPolicyVersion())) {
            throw new IllegalArgumentException("sizingPolicyVersion must be " + POLICY_VERSION);
        }
        validateLeverage(step.executionMode(), supplied.targetLeverage());

        BigDecimal defensiveEquity = CopyPolicyV1Capital.defensiveSizingEquity(
                step.currentMtmEquityUsd(), step.initialAccountEquityUsd());
        BigDecimal authorizedCapital = CopyPolicyV1Capital.authorizedSizingCapital(
                step.currentMtmEquityUsd(), step.initialAccountEquityUsd());
        BigDecimal policyAvailableMargin = authorizedCapital
                .subtract(supplied.usedMarginUsd())
                .max(BigDecimal.ZERO);
        BigDecimal availableMargin = supplied.availableMarginUsd().min(policyAvailableMargin);

        TargetPortfolioRequest effective = supplied.toBuilder()
                .targetAllocatedCapitalUsd(authorizedCapital)
                .availableMarginUsd(availableMargin)
                .reservedMarginUsd(BigDecimal.ZERO)
                .build();
        String inputDigest = CopyPolicyV1SizingDigest.sha256(step, effective, defensiveEquity,
                authorizedCapital);
        TargetPortfolioResult portfolio = calculator.calculate(effective);
        return new CopyPolicyV1SizingStepResult(
                step.wallet(), step.strategyKey(), step.world(), step.executionMode(),
                DecimalSupport.normalize(step.initialAccountEquityUsd()),
                DecimalSupport.normalize(step.currentMtmEquityUsd()), defensiveEquity,
                authorizedCapital, inputDigest, portfolio);
    }

    private void validateLeverage(CopyExecutionMode mode, BigDecimal leverage) {
        if ((mode == CopyExecutionMode.HISTORICAL || mode == CopyExecutionMode.MICRO_LIVE)
                && leverage.compareTo(HISTORICAL_LEVERAGE) != 0) {
            throw new IllegalArgumentException(mode + " target leverage must be fixed at x5");
        }
        if ((mode == CopyExecutionMode.LIVE || mode == CopyExecutionMode.SHADOW)
                && (leverage.compareTo(MIN_LIVE_LEVERAGE) < 0
                || leverage.compareTo(MAX_LIVE_LEVERAGE) > 0)) {
            throw new IllegalArgumentException(mode + " target leverage must be between x1 and x20");
        }
    }
}
