package com.apunto.engine.service.copy.certification;

public record LiveCertificationCreateResult(
        boolean created,
        boolean idempotent,
        String reasonCode,
        LiveCertificationSnapshot certification
) {
    static LiveCertificationCreateResult created(LiveCertificationSnapshot snapshot, boolean idempotent) {
        return new LiveCertificationCreateResult(true, idempotent,
                idempotent ? "LIVE_CERTIFICATION_ALREADY_CREATED" : "LIVE_CERTIFICATION_CREATED",
                snapshot);
    }

    static LiveCertificationCreateResult blocked(String reason) {
        return new LiveCertificationCreateResult(false, false, reason, null);
    }
}
