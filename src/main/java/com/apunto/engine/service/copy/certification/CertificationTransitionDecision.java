package com.apunto.engine.service.copy.certification;

public record CertificationTransitionDecision(boolean allowed, String reasonCode) {

    static CertificationTransitionDecision permit() {
        return new CertificationTransitionDecision(true, "LIVE_CERTIFICATION_TRANSITION_ALLOWED");
    }

    static CertificationTransitionDecision block(String reasonCode) {
        return new CertificationTransitionDecision(false, reasonCode);
    }
}
