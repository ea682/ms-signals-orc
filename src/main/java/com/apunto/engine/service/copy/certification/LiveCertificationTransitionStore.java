package com.apunto.engine.service.copy.certification;

import java.util.Optional;
import java.util.UUID;

public interface LiveCertificationTransitionStore {
    Optional<LiveCertificationSnapshot> lockById(UUID certificationId);
    Optional<LiveCertificationAuditFact> findAuditByTransitionKey(String transitionKey);
    boolean compareAndSet(UUID certificationId, long expectedVersion,
                          LiveCertificationStatus expectedStatus,
                          LiveCertificationStatus nextStatus);
    void appendAudit(LiveCertificationAuditFact audit);
}
