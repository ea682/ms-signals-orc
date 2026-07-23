package com.apunto.engine.service.copy.account;

import java.math.BigDecimal;

public record MicroLiveCapacityDecision(
        boolean allowed,
        String reasonCode,
        long occupiedSlots,
        int effectiveMaxSlots,
        BigDecimal walletBalance,
        BigDecimal availableBalance,
        BigDecimal requiredReservedCapital
) {
}
