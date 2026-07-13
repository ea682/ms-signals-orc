package com.apunto.engine.service.copy.certification;

import java.util.Map;

public record LiveCertificationCreateCommand(
        LiveCertificationIdentity identity,
        LiveEvidenceLevel evidenceLevel,
        LiveCertificationStatus initialStatus,
        String actor,
        String reason,
        Map<String, Object> evidenceSnapshot,
        String creationKey
) {
}
