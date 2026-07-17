package com.apunto.engine.service.copy.certification;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface LiveCertificationCatalogStore {
    Optional<LiveCertificationCatalogRecord> findByCreationKey(String creationKey);
    Optional<LiveCertificationCatalogRecord> findByIdentity(LiveCertificationIdentity identity);
    boolean insert(LiveCertificationCatalogRecord record, String creationKey,
                   Map<String, Object> evidenceSnapshot, String actor, String reason);
    Optional<LiveCertificationIdentity> findIdentityById(UUID certificationId);
}
