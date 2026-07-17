package com.apunto.engine.service.copy.certification;

public enum LiveCertificationStatus {
    SOURCE_SHADOW_VALIDATING,
    EXECUTABLE_SHADOW_VALIDATING,
    MICRO_LIVE_VALIDATING,
    LIVE_APPROVED,
    LIVE_DEGRADED,
    SUSPENDED,
    REVOKED
}
