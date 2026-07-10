package com.apunto.engine.service.copy.dispatch;

public record CopyDispatchIdentity(
        String userId,
        Long userCopyAllocationId,
        String executionMode,
        String strategyCode,
        String scopeType,
        String scopeValue,
        String sourceEventId,
        String copyIntent
) {
}
