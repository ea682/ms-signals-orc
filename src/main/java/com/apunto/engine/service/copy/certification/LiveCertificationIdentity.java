package com.apunto.engine.service.copy.certification;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Objects;

public record LiveCertificationIdentity(
        String walletId,
        String strategyCode,
        String strategyVersion,
        String scopeType,
        String scopeValue,
        BigDecimal capitalBandMin,
        BigDecimal capitalBandMax,
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

    public LiveCertificationIdentity {
        walletId = lower(walletId, "walletId");
        strategyCode = upper(strategyCode, "strategyCode");
        strategyVersion = exact(strategyVersion, "strategyVersion");
        scopeType = upper(scopeType, "scopeType");
        scopeValue = upper(scopeValue, "scopeValue");
        capitalBandMin = nonNegative(capitalBandMin, "capitalBandMin");
        capitalBandMax = nonNegative(capitalBandMax, "capitalBandMax");
        if (capitalBandMax.compareTo(capitalBandMin) < 0) {
            throw new IllegalArgumentException("capitalBandMax must be >= capitalBandMin");
        }
        targetLeverage = positive(targetLeverage, "targetLeverage");
        exchange = upper(exchange, "exchange");
        quoteAsset = upper(quoteAsset, "quoteAsset");
        sizingPolicyVersion = exact(sizingPolicyVersion, "sizingPolicyVersion");
        symbolMappingVersion = exact(symbolMappingVersion, "symbolMappingVersion");
        feeModelVersion = exact(feeModelVersion, "feeModelVersion");
        fundingModelVersion = exact(fundingModelVersion, "fundingModelVersion");
        slippageModelVersion = exact(slippageModelVersion, "slippageModelVersion");
        liquidityModelVersion = exact(liquidityModelVersion, "liquidityModelVersion");
    }

    public boolean matches(LiveEntryAuthorizationRequest request) {
        if (request == null || request.allocatedCapitalUsd() == null || request.targetLeverage() == null) {
            return false;
        }
        return walletId.equals(request.walletId())
                && strategyCode.equals(request.strategyCode())
                && strategyVersion.equals(request.strategyVersion())
                && scopeType.equals(request.scopeType())
                && scopeValue.equals(request.scopeValue())
                && request.allocatedCapitalUsd().compareTo(capitalBandMin) >= 0
                && request.allocatedCapitalUsd().compareTo(capitalBandMax) <= 0
                && targetLeverage.compareTo(request.targetLeverage()) == 0
                && exchange.equals(request.exchange())
                && quoteAsset.equals(request.quoteAsset())
                && sizingPolicyVersion.equals(request.sizingPolicyVersion())
                && symbolMappingVersion.equals(request.symbolMappingVersion())
                && feeModelVersion.equals(request.feeModelVersion())
                && fundingModelVersion.equals(request.fundingModelVersion())
                && slippageModelVersion.equals(request.slippageModelVersion())
                && liquidityModelVersion.equals(request.liquidityModelVersion());
    }

    private static String lower(String value, String field) {
        return exact(value, field).toLowerCase(Locale.ROOT);
    }

    private static String upper(String value, String field) {
        return exact(value, field).toUpperCase(Locale.ROOT).replace('-', '_');
    }

    static String exact(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " is required");
        return value.trim();
    }

    private static BigDecimal nonNegative(BigDecimal value, String field) {
        Objects.requireNonNull(value, field + " is required");
        if (value.signum() < 0) throw new IllegalArgumentException(field + " must be non-negative");
        return value.stripTrailingZeros();
    }

    private static BigDecimal positive(BigDecimal value, String field) {
        Objects.requireNonNull(value, field + " is required");
        if (value.signum() <= 0) throw new IllegalArgumentException(field + " must be positive");
        return value.stripTrailingZeros();
    }
}
