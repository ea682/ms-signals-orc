package com.apunto.engine.service.copy.coverage;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record ShadowCoverageSnapshot(
        long historicalSimulatedEvents,
        long historicalSkippedEvents,
        long historicalErrorEvents,
        long historicalEvaluableEvents,
        BigDecimal historicalCoveragePct,
        long rollingSimulatedEvents,
        long rollingRecordedEvents,
        long rollingSkippedEvents,
        long rollingErrorEvents,
        long rollingEvaluableEvents,
        BigDecimal rollingCoveragePct,
        int rollingWindowDays,
        int rollingMaxEvents,
        int rollingMinEvents,
        OffsetDateTime rollingWindowStart,
        OffsetDateTime rollingWindowEnd,
        BigDecimal coverageThresholdPct,
        BigDecimal rollingThresholdPct,
        ShadowCoverageSource coverageSourceUsed,
        ShadowCoverageDecision coverageDecision,
        ShadowCoverageReasonCode coverageReasonCode,
        ShadowCoverageDecision rollingCoverageDecision,
        ShadowCoverageReasonCode rollingCoverageReasonCode,
        boolean queryFailed
) {
}
