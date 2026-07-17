package com.apunto.engine.service.copy.certification;

public record LiveEntryAuthorizationContext(
        boolean valid,
        String reasonCode,
        LiveEntryAuthorizationRequest request
) {
    static LiveEntryAuthorizationContext valid(LiveEntryAuthorizationRequest request) {
        return new LiveEntryAuthorizationContext(true, "LIVE_CERTIFICATION_RUNTIME_CONTEXT_VALID", request);
    }

    static LiveEntryAuthorizationContext invalid(String reasonCode) {
        return new LiveEntryAuthorizationContext(false, reasonCode, null);
    }
}
