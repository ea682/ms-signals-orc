package com.apunto.engine.service.copy.certification;

import java.time.OffsetDateTime;
import java.util.UUID;

public record LiveAllocationActivationSnapshot(
        Long allocationId,
        UUID userId,
        String walletId,
        String strategyCode,
        String scopeType,
        String scopeValue,
        String executionMode,
        String status,
        boolean active,
        OffsetDateTime endsAt
) {
    public LiveAllocationActivationSnapshot withExecutionMode(String mode) {
        return new LiveAllocationActivationSnapshot(
                allocationId, userId, walletId, strategyCode, scopeType, scopeValue,
                mode, status, active, endsAt);
    }
}
