package com.apunto.engine.service.copy.certification;

public record LiveUserAdoptionResult(
        boolean persisted,
        String reasonCode,
        UserAdoptionValidationDecision decision
) {
    static LiveUserAdoptionResult persisted(UserAdoptionValidationDecision decision) {
        return new LiveUserAdoptionResult(true,
                decision.valid() ? "LIVE_ADOPTION_VALID" : "LIVE_ADOPTION_REJECTED", decision);
    }

    static LiveUserAdoptionResult blocked(String reason) {
        return new LiveUserAdoptionResult(false, reason, null);
    }
}
