package com.apunto.engine.service.copy.certification;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

public interface LiveAllocationActivationStore {
    Optional<LiveAllocationActivationSnapshot> lockAllocation(Long allocationId);
    Optional<LiveAllocationActivationAudit> findAudit(String activationKey);
    long countOpenOperations(Long allocationId);
    long countNonTerminalIntents(Long allocationId);
    Optional<LiveActivationAuthorization> findAuthorization(Long allocationId, UUID certificationId);
    boolean activate(Long allocationId, String actor, String reason, OffsetDateTime activatedAt);
    default boolean activatePendingLive(Long allocationId, String actor, String reason,
                                        OffsetDateTime activatedAt) {
        return false;
    }
    void appendAudit(LiveAllocationActivationAudit audit);
}
