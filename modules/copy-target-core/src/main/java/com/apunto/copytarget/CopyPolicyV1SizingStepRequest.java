package com.apunto.copytarget;

import java.math.BigDecimal;
import java.util.Objects;

public record CopyPolicyV1SizingStepRequest(
        String wallet,
        String strategyKey,
        CopyPolicyWorld world,
        CopyExecutionMode executionMode,
        BigDecimal initialAccountEquityUsd,
        BigDecimal currentMtmEquityUsd,
        TargetPortfolioRequest sizingInput
) {
    public CopyPolicyV1SizingStepRequest {
        wallet = required(wallet, "wallet");
        strategyKey = required(strategyKey, "strategyKey");
        world = Objects.requireNonNull(world, "world");
        executionMode = Objects.requireNonNull(executionMode, "executionMode");
        if (initialAccountEquityUsd == null || initialAccountEquityUsd.signum() <= 0) {
            throw new IllegalArgumentException("initialAccountEquityUsd must be positive");
        }
        currentMtmEquityUsd = Objects.requireNonNull(currentMtmEquityUsd, "currentMtmEquityUsd");
        sizingInput = Objects.requireNonNull(sizingInput, "sizingInput");
    }

    private static String required(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
