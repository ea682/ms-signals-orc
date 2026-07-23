package com.apunto.engine.service.copy.dispatch;

import java.math.BigDecimal;

public record CopyOrderRuntimePreflightDecision(
        boolean allowed,
        boolean reconciliationRequired,
        String reasonCode,
        BigDecimal executableQuantity
) {
    public static CopyOrderRuntimePreflightDecision allowed(BigDecimal quantity, String reasonCode) {
        return new CopyOrderRuntimePreflightDecision(true, false, reasonCode, quantity);
    }

    public static CopyOrderRuntimePreflightDecision blocked(String reasonCode, boolean reconciliationRequired) {
        return new CopyOrderRuntimePreflightDecision(false, reconciliationRequired, reasonCode, BigDecimal.ZERO);
    }
}
