package com.apunto.engine.service.copy.certification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.OffsetDateTime;

@Service
public class ManualLiveAllocationActivationService {

    private final LiveAllocationActivationStore store;
    private final Clock clock;

    @Autowired
    public ManualLiveAllocationActivationService(LiveAllocationActivationStore store) {
        this(store, Clock.systemUTC());
    }

    ManualLiveAllocationActivationService(LiveAllocationActivationStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    @Transactional
    public LiveAllocationActivationResult activate(LiveAllocationActivationCommand command) {
        if (command == null || command.allocationId() == null || command.allocationId() <= 0
                || command.certificationId() == null || blank(command.actor())
                || blank(command.reason()) || blank(command.activationKey())) {
            return LiveAllocationActivationResult.blocked("LIVE_ACTIVATION_CONTRACT_INVALID",
                    command == null ? null : command.allocationId());
        }
        LiveAllocationActivationSnapshot allocation = store.lockAllocation(command.allocationId()).orElse(null);
        if (allocation == null) {
            return LiveAllocationActivationResult.blocked("LIVE_ACTIVATION_ALLOCATION_MISSING",
                    command.allocationId());
        }
        LiveAllocationActivationAudit existing = store.findAudit(command.activationKey().trim()).orElse(null);
        if (existing != null) {
            if (existing.allocationId().equals(command.allocationId())
                    && existing.certificationId().equals(command.certificationId())) {
                return LiveAllocationActivationResult.activated(command.allocationId(), true);
            }
            return LiveAllocationActivationResult.blocked("LIVE_ACTIVATION_KEY_CONFLICT",
                    command.allocationId());
        }
        boolean pendingLive = "LIVE".equalsIgnoreCase(allocation.executionMode())
                && "PAUSED".equalsIgnoreCase(allocation.status());
        if (!pendingLive) {
            return LiveAllocationActivationResult.blocked("LIVE_ACTIVATION_WRONG_SOURCE_MODE",
                    command.allocationId());
        }
        if (!allocation.active() || allocation.endsAt() != null) {
            return LiveAllocationActivationResult.blocked("LIVE_ACTIVATION_ALLOCATION_NOT_ACTIVE",
                    command.allocationId());
        }
        if (store.countOpenOperations(command.allocationId()) > 0) {
            return LiveAllocationActivationResult.blocked("LIVE_ACTIVATION_OPEN_POSITIONS_EXIST",
                    command.allocationId());
        }
        if (store.countNonTerminalIntents(command.allocationId()) > 0) {
            return LiveAllocationActivationResult.blocked("LIVE_ACTIVATION_PENDING_INTENTS_EXIST",
                    command.allocationId());
        }
        LiveActivationAuthorization authorization = store
                .findAuthorization(command.allocationId(), command.certificationId()).orElse(null);
        if (authorization == null) {
            return LiveAllocationActivationResult.blocked("LIVE_ADOPTION_MISSING", command.allocationId());
        }
        if (authorization.certificationStatus() != LiveCertificationStatus.LIVE_APPROVED) {
            return LiveAllocationActivationResult.blocked("LIVE_CERTIFICATION_NOT_APPROVED",
                    command.allocationId());
        }
        if (!"VALID".equalsIgnoreCase(authorization.adoptionStatus())
                || !authorization.allChecksValid()) {
            return LiveAllocationActivationResult.blocked("LIVE_ADOPTION_REJECTED", command.allocationId());
        }
        OffsetDateTime now = OffsetDateTime.now(clock);
        if (authorization.validatedAt() == null || authorization.validatedAt().isAfter(now)) {
            return LiveAllocationActivationResult.blocked("LIVE_ADOPTION_OBSERVED_AT_IN_FUTURE",
                    command.allocationId());
        }
        if (authorization.expiresAt() == null || !authorization.expiresAt().isAfter(now)) {
            return LiveAllocationActivationResult.blocked("LIVE_ADOPTION_EXPIRED", command.allocationId());
        }
        boolean activated = store.activatePendingLive(
                command.allocationId(), command.actor().trim(), command.reason().trim(), now);
        if (!activated) {
            return LiveAllocationActivationResult.blocked("LIVE_ACTIVATION_CONCURRENT_STATE_CHANGE",
                    command.allocationId());
        }
        store.appendAudit(new LiveAllocationActivationAudit(
                command.activationKey().trim(), command.allocationId(), command.certificationId(),
                allocation.userId(), allocation.executionMode(), "LIVE", command.actor().trim(),
                command.reason().trim(), now));
        return LiveAllocationActivationResult.activated(command.allocationId(), false);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
