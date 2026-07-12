package com.apunto.engine.service.copy.allocation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.Objects;

public record LiveAllocationDistributionEntry(
        String walletId,
        String strategyCode,
        String scopeType,
        String scopeValue,
        BigDecimal strategyPercentage
) {
    public LiveAllocationDistributionEntry {
        walletId = required(walletId, "walletId").toLowerCase(Locale.ROOT);
        strategyCode = required(strategyCode, "strategyCode").toUpperCase(Locale.ROOT).replace('-', '_');
        scopeType = scopeType == null || scopeType.isBlank()
                ? "strategy"
                : scopeType.trim().toLowerCase(Locale.ROOT);
        scopeValue = scopeValue == null || scopeValue.isBlank() ? strategyCode : scopeValue.trim();
        strategyPercentage = Objects.requireNonNull(strategyPercentage, "strategyPercentage");
        if (strategyPercentage.scale() > 6) {
            throw new IllegalArgumentException("strategyPercentage scale must be <= 6");
        }
        strategyPercentage = strategyPercentage.setScale(6, RoundingMode.HALF_UP);
        if (strategyPercentage.signum() <= 0 || strategyPercentage.compareTo(BigDecimal.ONE) > 0) {
            throw new IllegalArgumentException("strategyPercentage must be in (0,1]");
        }
    }

    public String profileKey() {
        return walletId + '|' + strategyCode + '|' + scopeType + '|' + scopeValue;
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }
}
