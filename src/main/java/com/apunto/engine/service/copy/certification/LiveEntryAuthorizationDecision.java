package com.apunto.engine.service.copy.certification;

import java.util.UUID;

public record LiveEntryAuthorizationDecision(boolean allowed, String reasonCode, UUID certificationId) {

    public static LiveEntryAuthorizationDecision allowed(String reasonCode, UUID certificationId) {
        return new LiveEntryAuthorizationDecision(true, reasonCode, certificationId);
    }

    public static LiveEntryAuthorizationDecision blocked(String reasonCode) {
        return new LiveEntryAuthorizationDecision(false, reasonCode, null);
    }
}
