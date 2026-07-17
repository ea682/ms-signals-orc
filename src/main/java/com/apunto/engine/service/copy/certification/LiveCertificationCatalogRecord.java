package com.apunto.engine.service.copy.certification;

import java.util.UUID;

public record LiveCertificationCatalogRecord(
        UUID id,
        LiveCertificationIdentity identity,
        LiveEvidenceLevel evidenceLevel,
        LiveCertificationStatus status,
        long version
) {
    LiveCertificationSnapshot snapshot() {
        return new LiveCertificationSnapshot(id, status, version);
    }
}
