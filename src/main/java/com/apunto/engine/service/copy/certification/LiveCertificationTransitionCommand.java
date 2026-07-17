package com.apunto.engine.service.copy.certification;

import java.util.Map;
import java.util.UUID;

public record LiveCertificationTransitionCommand(
        UUID certificationId,
        long expectedVersion,
        LiveCertificationStatus expectedPriorStatus,
        LiveCertificationStatus nextStatus,
        boolean automatic,
        String actor,
        String reason,
        Map<String, Object> evidenceSnapshot,
        String transitionKey
) {
}
