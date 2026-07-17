package com.apunto.engine.service.copy.certification;

public record LiveCertificationTransitionResult(
        boolean applied,
        boolean idempotent,
        String reasonCode,
        LiveCertificationSnapshot certification
) {
    static LiveCertificationTransitionResult applied(LiveCertificationSnapshot certification, boolean idempotent) {
        return new LiveCertificationTransitionResult(true, idempotent,
                idempotent ? "LIVE_CERTIFICATION_TRANSITION_ALREADY_APPLIED"
                        : "LIVE_CERTIFICATION_TRANSITION_APPLIED",
                certification);
    }

    static LiveCertificationTransitionResult blocked(String reason, LiveCertificationSnapshot certification) {
        return new LiveCertificationTransitionResult(false, false, reason, certification);
    }
}
