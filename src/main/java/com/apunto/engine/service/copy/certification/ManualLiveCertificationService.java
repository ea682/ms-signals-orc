package com.apunto.engine.service.copy.certification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Map;

@Service
public class ManualLiveCertificationService {

    private final LiveCertificationTransitionStore store;
    private final LiveCertificationTransitionPolicy policy;
    private final Clock clock;

    @Autowired
    public ManualLiveCertificationService(LiveCertificationTransitionStore store,
                                          LiveCertificationTransitionPolicy policy) {
        this(store, policy, Clock.systemUTC());
    }

    ManualLiveCertificationService(LiveCertificationTransitionStore store,
                                   LiveCertificationTransitionPolicy policy,
                                   Clock clock) {
        this.store = store;
        this.policy = policy;
        this.clock = clock;
    }

    @Transactional
    public LiveCertificationTransitionResult transition(LiveCertificationTransitionCommand command) {
        if (command == null || command.certificationId() == null
                || command.transitionKey() == null || command.transitionKey().isBlank()) {
            return LiveCertificationTransitionResult.blocked(
                    "LIVE_CERTIFICATION_TRANSITION_CONTRACT_INVALID", null);
        }
        LiveCertificationSnapshot current = store.lockById(command.certificationId()).orElse(null);
        if (current == null) {
            return LiveCertificationTransitionResult.blocked("LIVE_CERTIFICATION_MISSING", null);
        }

        LiveCertificationAuditFact existing = store.findAuditByTransitionKey(command.transitionKey()).orElse(null);
        if (existing != null) {
            if (!existing.certificationId().equals(command.certificationId())
                    || existing.priorStatus() != command.expectedPriorStatus()
                    || existing.nextStatus() != command.nextStatus()) {
                return LiveCertificationTransitionResult.blocked(
                        "LIVE_CERTIFICATION_TRANSITION_KEY_CONFLICT", current);
            }
            return LiveCertificationTransitionResult.applied(current, true);
        }
        if (current.version() != command.expectedVersion()) {
            return LiveCertificationTransitionResult.blocked(
                    "LIVE_CERTIFICATION_VERSION_CONFLICT", current);
        }
        if (current.status() != command.expectedPriorStatus()) {
            return LiveCertificationTransitionResult.blocked(
                    "LIVE_CERTIFICATION_PRIOR_STATUS_CONFLICT", current);
        }
        CertificationTransitionDecision policyDecision = policy.evaluate(
                current.status(), command.nextStatus(), command.automatic(), command.actor(),
                command.reason(), command.evidenceSnapshot());
        if (!policyDecision.allowed()) {
            return LiveCertificationTransitionResult.blocked(policyDecision.reasonCode(), current);
        }

        boolean updated = store.compareAndSet(current.id(), current.version(), current.status(), command.nextStatus());
        if (!updated) {
            return LiveCertificationTransitionResult.blocked(
                    "LIVE_CERTIFICATION_VERSION_CONFLICT", current);
        }
        LiveCertificationSnapshot next = new LiveCertificationSnapshot(
                current.id(), command.nextStatus(), current.version() + 1);
        store.appendAudit(new LiveCertificationAuditFact(
                current.id(), command.transitionKey().trim(), current.status(), command.nextStatus(),
                current.version(), next.version(), command.actor().trim(), command.reason().trim(),
                Map.copyOf(command.evidenceSnapshot()), OffsetDateTime.now(clock)));
        return LiveCertificationTransitionResult.applied(next, false);
    }
}
