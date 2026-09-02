package com.apunto.copytarget;

import java.math.BigDecimal;

public record CopyPolicyV1SizingStepResult(
        String wallet,
        String strategyKey,
        CopyPolicyWorld world,
        CopyExecutionMode executionMode,
        BigDecimal initialAccountEquityUsd,
        BigDecimal currentMtmEquityUsd,
        BigDecimal defensiveEquityUsd,
        BigDecimal authorizedSizingCapitalUsd,
        String inputDigest,
        TargetPortfolioResult portfolio
) {
}
