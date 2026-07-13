package com.apunto.engine.service.copy.certification;

import org.springframework.stereotype.Component;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

@Component
public class LiveCertificationTransitionPolicy {

    private static final Map<LiveCertificationStatus, Set<LiveCertificationStatus>> FORWARD = Map.of(
            LiveCertificationStatus.SOURCE_SHADOW_VALIDATING,
            EnumSet.of(LiveCertificationStatus.EXECUTABLE_SHADOW_VALIDATING),
            LiveCertificationStatus.EXECUTABLE_SHADOW_VALIDATING,
            EnumSet.of(LiveCertificationStatus.MICRO_LIVE_VALIDATING),
            LiveCertificationStatus.MICRO_LIVE_VALIDATING,
            EnumSet.of(LiveCertificationStatus.LIVE_APPROVED),
            LiveCertificationStatus.LIVE_DEGRADED,
            EnumSet.of(LiveCertificationStatus.LIVE_APPROVED),
            LiveCertificationStatus.SUSPENDED,
            EnumSet.of(LiveCertificationStatus.LIVE_APPROVED)
    );

    private static final Set<LiveCertificationStatus> SAFETY = EnumSet.of(
            LiveCertificationStatus.LIVE_DEGRADED,
            LiveCertificationStatus.SUSPENDED,
            LiveCertificationStatus.REVOKED
    );

    public CertificationTransitionDecision evaluate(LiveCertificationStatus current,
                                                    LiveCertificationStatus next,
                                                    boolean automatic,
                                                    String actor,
                                                    String reason,
                                                    Map<String, ?> evidenceSnapshot) {
        if (current == null || next == null) {
            return CertificationTransitionDecision.block("LIVE_CERTIFICATION_STATUS_REQUIRED");
        }
        if (automatic) {
            return CertificationTransitionDecision.block("LIVE_CERTIFICATION_MANUAL_TRANSITION_REQUIRED");
        }
        if (actor == null || actor.isBlank()) {
            return CertificationTransitionDecision.block("LIVE_CERTIFICATION_MANUAL_ACTOR_REQUIRED");
        }
        if (reason == null || reason.isBlank()) {
            return CertificationTransitionDecision.block("LIVE_CERTIFICATION_REASON_REQUIRED");
        }
        if (evidenceSnapshot == null || evidenceSnapshot.isEmpty()) {
            return CertificationTransitionDecision.block("LIVE_CERTIFICATION_EVIDENCE_REQUIRED");
        }
        if (current == LiveCertificationStatus.REVOKED) {
            return CertificationTransitionDecision.block("LIVE_CERTIFICATION_REVOKED_TERMINAL");
        }
        if (current == next) {
            return CertificationTransitionDecision.block("LIVE_CERTIFICATION_ILLEGAL_TRANSITION");
        }
        boolean forward = FORWARD.getOrDefault(current, Set.of()).contains(next);
        if (!forward && !SAFETY.contains(next)) {
            return CertificationTransitionDecision.block("LIVE_CERTIFICATION_ILLEGAL_TRANSITION");
        }
        return CertificationTransitionDecision.permit();
    }
}
