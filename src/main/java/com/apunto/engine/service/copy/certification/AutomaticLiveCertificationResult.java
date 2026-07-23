package com.apunto.engine.service.copy.certification;

import java.util.UUID;

public record AutomaticLiveCertificationResult(
        boolean approved,
        String reasonCode,
        UUID certificationId,
        boolean idempotent
) {
    static AutomaticLiveCertificationResult approved(UUID id, boolean idempotent) {
        return new AutomaticLiveCertificationResult(true,
                idempotent ? "LIVE_CERTIFICATION_ALREADY_APPROVED" : "LIVE_CERTIFICATION_AUTOMATICALLY_APPROVED",
                id, idempotent);
    }

    static AutomaticLiveCertificationResult blocked(String reasonCode, UUID id) {
        return new AutomaticLiveCertificationResult(false, reasonCode, id, false);
    }
}

