package com.apunto.engine.service.copy;

@FunctionalInterface
public interface CopyStrategyGuardRuntimeCache {

    CopyStrategyGuardDecision evaluateCached(String walletId, String strategyCode);
}
