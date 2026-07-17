package com.apunto.engine.service.copy.certification;

import java.time.OffsetDateTime;
import java.util.UUID;

public record LiveAllocationActivationAudit(
        String activationKey,
        Long allocationId,
        UUID certificationId,
        UUID userId,
        String priorMode,
        String nextMode,
        String actor,
        String reason,
        OffsetDateTime occurredAt
) {
}
