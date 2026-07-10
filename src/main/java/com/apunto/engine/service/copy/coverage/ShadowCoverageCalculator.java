package com.apunto.engine.service.copy.coverage;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Component
public class ShadowCoverageCalculator {

    private static final BigDecimal HUNDRED = new BigDecimal("100");
    private static final int PERCENT_SCALE = 6;

    public ShadowCoverageSnapshot calculate(
            long historicalSimulatedEvents,
            long historicalSkippedEvents,
            long historicalErrorEvents,
            ShadowCoverageCounts rollingCounts,
            boolean queryFailed,
            OffsetDateTime windowEnd,
            ShadowCoverageWindowProperties properties,
            BigDecimal legacyThresholdPct
    ) {
        properties.validate();
        OffsetDateTime endUtc = (windowEnd == null ? OffsetDateTime.now(ZoneOffset.UTC) : windowEnd)
                .withOffsetSameInstant(ZoneOffset.UTC);
        OffsetDateTime startUtc = endUtc.minusDays(properties.getWindowDays());
        ShadowCoverageCounts counts = rollingCounts == null ? ShadowCoverageCounts.empty(null) : rollingCounts;

        long historicalSuccess = nonNegative(historicalSimulatedEvents);
        long historicalSkipped = nonNegative(historicalSkippedEvents);
        long historicalErrors = nonNegative(historicalErrorEvents);
        long historicalEvaluable = safeAdd(historicalSuccess, historicalSkipped, historicalErrors);
        BigDecimal historicalPct = percentage(historicalSuccess, historicalEvaluable);

        long rollingSuccess = Math.max(nonNegative(counts.simulatedEvents()), nonNegative(counts.recordedEvents()));
        long rollingSkipped = nonNegative(counts.skippedEvents());
        long rollingErrors = nonNegative(counts.errorEvents());
        long rollingEvaluable = safeAdd(rollingSuccess, rollingSkipped, rollingErrors);
        BigDecimal rollingPct = percentage(rollingSuccess, rollingEvaluable);

        CoverageResult rollingResult = rollingResult(
                queryFailed || counts.queryFailed(),
                rollingEvaluable,
                rollingPct,
                properties
        );
        ShadowCoverageMode mode = properties.effectiveMode();
        BigDecimal legacyThreshold = normalizedThreshold(legacyThresholdPct);
        boolean historicalReady = legacyThreshold.compareTo(BigDecimal.ZERO) <= 0
                || historicalEvaluable > 0 && historicalPct.compareTo(legacyThreshold) >= 0;
        ShadowCoverageDecision historicalDecision = historicalReady
                ? ShadowCoverageDecision.COVERAGE_READY
                : ShadowCoverageDecision.COVERAGE_NOT_READY;
        boolean usesRolling = mode == ShadowCoverageMode.ROLLING;

        return new ShadowCoverageSnapshot(
                historicalSuccess,
                historicalSkipped,
                historicalErrors,
                historicalEvaluable,
                historicalPct,
                rollingSuccess,
                nonNegative(counts.recordedEvents()),
                rollingSkipped,
                rollingErrors,
                rollingEvaluable,
                rollingPct,
                properties.getWindowDays(),
                properties.getMaxEvents(),
                properties.getMinEvaluableEvents(),
                startUtc,
                endUtc,
                usesRolling ? properties.getMinimumPercent() : legacyThreshold,
                properties.getMinimumPercent(),
                usesRolling ? ShadowCoverageSource.ROLLING : ShadowCoverageSource.HISTORICAL,
                usesRolling ? rollingResult.decision() : historicalDecision,
                usesRolling ? rollingResult.reasonCode() : ShadowCoverageReasonCode.SHADOW_COVERAGE_LEGACY_USED,
                rollingResult.decision(),
                rollingResult.reasonCode(),
                queryFailed || counts.queryFailed()
        );
    }

    private static CoverageResult rollingResult(
            boolean queryFailed,
            long evaluable,
            BigDecimal percentage,
            ShadowCoverageWindowProperties properties
    ) {
        if (queryFailed) {
            return new CoverageResult(ShadowCoverageDecision.COVERAGE_NOT_READY,
                    ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_QUERY_FAILED);
        }
        if (evaluable == 0) {
            return new CoverageResult(ShadowCoverageDecision.NEEDS_MORE_DATA,
                    ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_NO_EVENTS);
        }
        if (evaluable < properties.getMinEvaluableEvents()) {
            return new CoverageResult(ShadowCoverageDecision.NEEDS_MORE_DATA,
                    ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_INSUFFICIENT_SAMPLE);
        }
        if (percentage.compareTo(properties.getMinimumPercent()) < 0) {
            return new CoverageResult(ShadowCoverageDecision.COVERAGE_NOT_READY,
                    ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_BELOW_THRESHOLD);
        }
        return new CoverageResult(ShadowCoverageDecision.COVERAGE_READY,
                ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_READY);
    }

    private static BigDecimal percentage(long numerator, long denominator) {
        if (denominator <= 0) {
            return BigDecimal.ZERO.setScale(PERCENT_SCALE, RoundingMode.HALF_UP);
        }
        return BigDecimal.valueOf(numerator)
                .multiply(HUNDRED)
                .divide(BigDecimal.valueOf(denominator), PERCENT_SCALE, RoundingMode.HALF_UP);
    }

    private static BigDecimal normalizedThreshold(BigDecimal value) {
        if (value == null) return BigDecimal.ZERO;
        return value.max(BigDecimal.ZERO).min(HUNDRED);
    }

    private static long nonNegative(long value) {
        return Math.max(0L, value);
    }

    private static long safeAdd(long first, long second, long third) {
        return Math.addExact(Math.addExact(first, second), third);
    }

    private record CoverageResult(ShadowCoverageDecision decision, ShadowCoverageReasonCode reasonCode) {
    }
}
