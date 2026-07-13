package com.apunto.engine.service.copy.simulation;

import java.util.Locale;
import java.util.Objects;

public record CopySimulationContext(
        String executionMode,
        String sourceEventId,
        String userId,
        Long allocationId,
        String walletId,
        String strategyCode,
        String strategyVersion,
        String scopeType,
        String scopeValue
) {
    public CopySimulationContext {
        executionMode = normalize(executionMode, "executionMode").toUpperCase(Locale.ROOT);
        sourceEventId = normalize(sourceEventId, "sourceEventId");
        userId = normalize(userId, "userId");
        walletId = normalize(walletId, "walletId").toLowerCase(Locale.ROOT);
        strategyCode = normalize(strategyCode, "strategyCode").toUpperCase(Locale.ROOT);
        strategyVersion = normalize(strategyVersion, "strategyVersion");
        scopeType = normalize(scopeType, "scopeType").toUpperCase(Locale.ROOT);
        scopeValue = normalize(scopeValue, "scopeValue").toUpperCase(Locale.ROOT);
    }

    public boolean isMicroLive() {
        return "MICRO_LIVE".equals(executionMode);
    }

    private static String normalize(String value, String name) {
        Objects.requireNonNull(value, name);
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }
}
