package com.apunto.engine.service.copy.certification;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

public record LiveEntryAuthorizationRequest(
        UUID userId,
        Long allocationId,
        String walletId,
        String strategyCode,
        String strategyVersion,
        String scopeType,
        String scopeValue,
        BigDecimal allocatedCapitalUsd,
        BigDecimal targetLeverage,
        String exchange,
        String quoteAsset,
        String sizingPolicyVersion,
        String symbolMappingVersion,
        String feeModelVersion,
        String fundingModelVersion,
        String slippageModelVersion,
        String liquidityModelVersion
) {

    public LiveEntryAuthorizationRequest {
        Objects.requireNonNull(userId, "userId is required");
        if (allocationId == null || allocationId <= 0) throw new IllegalArgumentException("allocationId is required");
        walletId = required(walletId).toLowerCase(Locale.ROOT);
        strategyCode = normalizedCode(strategyCode);
        strategyVersion = required(strategyVersion);
        scopeType = normalizedCode(scopeType);
        scopeValue = normalizedCode(scopeValue);
        allocatedCapitalUsd = positive(allocatedCapitalUsd, "allocatedCapitalUsd");
        targetLeverage = positive(targetLeverage, "targetLeverage");
        exchange = normalizedCode(exchange);
        quoteAsset = normalizedCode(quoteAsset);
        sizingPolicyVersion = required(sizingPolicyVersion);
        symbolMappingVersion = required(symbolMappingVersion);
        feeModelVersion = required(feeModelVersion);
        fundingModelVersion = required(fundingModelVersion);
        slippageModelVersion = required(slippageModelVersion);
        liquidityModelVersion = required(liquidityModelVersion);
    }

    private static String normalizedCode(String value) {
        return required(value).toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String required(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("required value is missing");
        return value.trim();
    }

    private static BigDecimal positive(BigDecimal value, String field) {
        Objects.requireNonNull(value, field + " is required");
        if (value.signum() <= 0) throw new IllegalArgumentException(field + " must be positive");
        return value.stripTrailingZeros();
    }
}
