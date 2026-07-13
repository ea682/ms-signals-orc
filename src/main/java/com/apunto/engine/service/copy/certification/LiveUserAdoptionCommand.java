package com.apunto.engine.service.copy.certification;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record LiveUserAdoptionCommand(
        UUID certificationId,
        UUID userId,
        Long allocationId,
        BigDecimal balanceUsd,
        BigDecimal assignedCapitalUsd,
        BigDecimal targetLeverage,
        String quoteAsset,
        String observedMarginMode,
        String requiredMarginMode,
        boolean apiPermissionsValid,
        boolean manualPositionsValid,
        boolean riskPolicyValid,
        OffsetDateTime observedAt,
        OffsetDateTime expiresAt
) {
}
