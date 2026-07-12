package com.apunto.engine.service.copy.allocation;

import java.time.OffsetDateTime;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

public record LiveAllocationPercentageRequest(
        UUID userId,
        String walletId,
        String strategyCode,
        String scopeType,
        String scopeValue,
        OffsetDateTime promotionTime
) {
    public LiveAllocationPercentageRequest {
        Objects.requireNonNull(userId, "userId");
        walletId = required(walletId, "walletId").toLowerCase(Locale.ROOT);
        strategyCode = required(strategyCode, "strategyCode").toUpperCase(Locale.ROOT).replace('-', '_');
        scopeType = normalizeScopeType(scopeType);
        scopeValue = normalizeScopeValue(scopeValue, strategyCode);
        promotionTime = promotionTime == null ? OffsetDateTime.now() : promotionTime;
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

    private static String normalizeScopeType(String value) {
        return value == null || value.isBlank() ? "strategy" : value.trim().toLowerCase(Locale.ROOT);
    }

    private static String normalizeScopeValue(String value, String strategyCode) {
        return value == null || value.isBlank() ? Objects.requireNonNull(strategyCode) : value.trim();
    }
}
