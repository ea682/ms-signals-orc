package com.apunto.engine.service.copy.readiness;

import java.math.BigDecimal;
import java.util.List;

public record MicroLiveReadinessDecision(
        boolean allowed,
        List<String> reasons,
        BigDecimal calendarProgressPct,
        BigDecimal executionEvidencePct,
        BigDecimal reconciliationPct,
        BigDecimal finalReadinessPct
) {
    public MicroLiveReadinessDecision {
        reasons = reasons == null ? List.of() : List.copyOf(reasons);
    }

    public String primaryReason() {
        return reasons.isEmpty() ? "MICRO_LIVE_EXECUTION_EVIDENCE_READY" : reasons.getFirst();
    }
}
