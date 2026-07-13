package com.apunto.engine.service.copy.certification;

import java.time.OffsetDateTime;
import java.util.UUID;

public record LiveCertificationAuthorizationRecord(
        UUID certificationId,
        LiveCertificationIdentity identity,
        LiveCertificationStatus certificationStatus,
        UUID adoptionId,
        String adoptionStatus,
        boolean balanceValid,
        boolean capitalBandValid,
        boolean leverageValid,
        boolean quoteAssetValid,
        boolean marginModeValid,
        boolean apiPermissionsValid,
        boolean manualPositionsValid,
        boolean riskPolicyValid,
        OffsetDateTime validatedAt,
        OffsetDateTime expiresAt
) {

    public LiveCertificationAuthorizationRecord withAdoptionWindow(OffsetDateTime validated, OffsetDateTime expires) {
        return new LiveCertificationAuthorizationRecord(
                certificationId, identity, certificationStatus, adoptionId, adoptionStatus,
                balanceValid, capitalBandValid, leverageValid, quoteAssetValid, marginModeValid,
                apiPermissionsValid, manualPositionsValid, riskPolicyValid, validated, expires);
    }

    boolean everyAdoptionCheckValid() {
        return balanceValid && capitalBandValid && leverageValid && quoteAssetValid
                && marginModeValid && apiPermissionsValid && manualPositionsValid && riskPolicyValid;
    }
}
