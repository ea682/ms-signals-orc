package com.apunto.engine.service.copy.decision;

public record CopyDecisionRequest(
        String walletId,
        String strategyCode,
        String scopeType,
        String scopeValue,
        String mode,
        String simulation,
        int minHistoryDays,
        int simulationLookbackDays,
        int maxFactsPerUnit,
        int timeoutMs,
        boolean debug
) {
}
