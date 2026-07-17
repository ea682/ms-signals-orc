package com.apunto.engine.service.copy.certification;

import java.time.OffsetDateTime;
import java.util.UUID;

public record LiveActivationAuthorization(
        UUID certificationId,
        LiveCertificationStatus certificationStatus,
        String adoptionStatus,
        boolean allChecksValid,
        OffsetDateTime validatedAt,
        OffsetDateTime expiresAt
) {
    public LiveActivationAuthorization withExpiresAt(OffsetDateTime value) {
        return new LiveActivationAuthorization(
                certificationId, certificationStatus, adoptionStatus,
                allChecksValid, validatedAt, value);
    }
}
