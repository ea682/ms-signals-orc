package com.apunto.engine.service.copy.readiness;

import java.util.List;

public record ShadowLiveReadinessDecision(
        ShadowLiveReadinessStatus status,
        List<String> reasonCodes
) {
    public ShadowLiveReadinessDecision {
        reasonCodes = reasonCodes == null ? List.of() : List.copyOf(reasonCodes);
    }

    public boolean approvedForLive() {
        return status == ShadowLiveReadinessStatus.APPROVED_FOR_LIVE;
    }

    public String primaryReasonCode() {
        return reasonCodes.isEmpty() ? "NA" : reasonCodes.get(0);
    }
}
