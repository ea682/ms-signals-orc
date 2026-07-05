package com.apunto.engine.service.copy.capital;

import java.math.BigDecimal;

public record CopyGlobalCapitalAllocation(
        BigDecimal availableBalanceAmount,
        BigDecimal usedMarginAmount,
        BigDecimal safetyReserveAmount,
        BigDecimal requiredMarginAmount,
        BigDecimal availableAfterRequiredAmount,
        String capitalCurrency,
        boolean capitalOverbooked,
        boolean allowNewExposure,
        String reasonCode
) {
}
