package com.apunto.engine.service.copy.certification;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record UserAdoptionValidationRequest(
        UUID certificationId,
        UUID userId,
        Long allocationId,
        LiveCertificationIdentity certificationIdentity,
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
