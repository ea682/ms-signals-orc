package com.apunto.engine.service.copy.account;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record MicroLiveCapacitySnapshot(
        UUID executionAccountId,
        String asset,
        BigDecimal authoritativeEquityUsd,
        BigDecimal availableBalanceUsd,
        BigDecimal safetyBufferUsd,
        BigDecimal eligibleCapitalUsd,
        BigDecimal budgetPerAllocationUsd,
        int theoreticalCapacity,
        int effectiveCapacity,
        int configuredMax,
        int reservedRecertificationSlots,
        OffsetDateTime observedAt,
        OffsetDateTime validUntil
) {
}
