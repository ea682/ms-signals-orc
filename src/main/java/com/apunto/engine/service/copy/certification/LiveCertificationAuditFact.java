package com.apunto.engine.service.copy.certification;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public record LiveCertificationAuditFact(
        UUID certificationId,
        String transitionKey,
        LiveCertificationStatus priorStatus,
        LiveCertificationStatus nextStatus,
        long priorVersion,
        long nextVersion,
        String actor,
        String reason,
        Map<String, Object> evidenceSnapshot,
        OffsetDateTime occurredAt
) {
}
