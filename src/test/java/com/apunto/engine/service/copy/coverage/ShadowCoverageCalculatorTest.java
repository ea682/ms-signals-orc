package com.apunto.engine.service.copy.coverage;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowCoverageCalculatorTest {

    private static final OffsetDateTime NOW = OffsetDateTime.of(2026, 7, 10, 12, 0, 0, 0, ZoneOffset.UTC);
    private final ShadowCoverageCalculator calculator = new ShadowCoverageCalculator();

    @Test
    void historicalBadButRecentGoodPassesRollingCoverage() {
        ShadowCoverageSnapshot result = calculate(423, 226, 0, counts(111, 0, 3, 0));

        assertEquals(new BigDecimal("65.177196"), result.historicalCoveragePct());
        assertEquals(new BigDecimal("97.368421"), result.rollingCoveragePct());
        assertEquals(ShadowCoverageDecision.COVERAGE_READY, result.coverageDecision());
        assertEquals(ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_READY, result.coverageReasonCode());
        assertEquals(ShadowCoverageSource.ROLLING, result.coverageSourceUsed());
    }

    @Test
    void historicalGoodButRecentBadBlocksPromotion() {
        ShadowCoverageSnapshot result = calculate(1_000, 0, 0, counts(90, 0, 10, 0));

        assertEquals(new BigDecimal("100.000000"), result.historicalCoveragePct());
        assertEquals(new BigDecimal("90.000000"), result.rollingCoveragePct());
        assertEquals(ShadowCoverageDecision.COVERAGE_NOT_READY, result.coverageDecision());
        assertEquals(ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_BELOW_THRESHOLD, result.coverageReasonCode());
    }

    @Test
    void insufficientRecentSampleNeedsMoreData() {
        ShadowCoverageSnapshot result = calculate(1_000, 0, 0, counts(99, 0, 0, 0));

        assertEquals(new BigDecimal("100.000000"), result.rollingCoveragePct());
        assertEquals(99, result.rollingEvaluableEvents());
        assertEquals(ShadowCoverageDecision.NEEDS_MORE_DATA, result.coverageDecision());
        assertEquals(ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_INSUFFICIENT_SAMPLE, result.coverageReasonCode());
    }

    @Test
    void exactlyMinimumSampleCanBeEvaluated() {
        ShadowCoverageSnapshot result = calculate(1_000, 0, 0, counts(95, 0, 5, 0));

        assertEquals(100, result.rollingEvaluableEvents());
        assertEquals(new BigDecimal("95.000000"), result.rollingCoveragePct());
        assertEquals(ShadowCoverageDecision.COVERAGE_READY, result.coverageDecision());
    }

    @Test
    void skippedAndErrorsRemainInDenominator() {
        ShadowCoverageSnapshot result = calculate(100, 0, 0, counts(95, 0, 3, 2));

        assertEquals(100, result.rollingEvaluableEvents());
        assertEquals(3, result.rollingSkippedEvents());
        assertEquals(2, result.rollingErrorEvents());
        assertEquals(new BigDecimal("95.000000"), result.rollingCoveragePct());
    }

    @Test
    void recordedRemainsLegacySuccessAliasWithoutDoubleCounting() {
        ShadowCoverageSnapshot result = calculate(100, 0, 0, counts(100, 100, 0, 0));

        assertEquals(100, result.rollingSimulatedEvents());
        assertEquals(100, result.rollingEvaluableEvents());
        assertEquals(new BigDecimal("100.000000"), result.rollingCoveragePct());
    }

    @Test
    void noEventsFailsClosed() {
        ShadowCoverageSnapshot result = calculate(1_000, 0, 0, ShadowCoverageCounts.empty(12L));

        assertEquals(0, result.rollingEvaluableEvents());
        assertEquals(ShadowCoverageDecision.NEEDS_MORE_DATA, result.coverageDecision());
        assertEquals(ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_NO_EVENTS, result.coverageReasonCode());
    }

    @Test
    void queryFailureFailsClosed() {
        ShadowCoverageWindowProperties properties = rollingProperties();

        ShadowCoverageSnapshot result = calculator.calculate(
                1_000, 0, 0, ShadowCoverageCounts.empty(12L), true, NOW, properties, new BigDecimal("95")
        );

        assertTrue(result.queryFailed());
        assertEquals(ShadowCoverageDecision.COVERAGE_NOT_READY, result.coverageDecision());
        assertEquals(ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_QUERY_FAILED, result.coverageReasonCode());
    }

    @Test
    void legacyFeatureFlagPreservesPreviousBehavior() {
        ShadowCoverageWindowProperties properties = rollingProperties();
        properties.setRollingEnabled(false);

        ShadowCoverageSnapshot result = calculator.calculate(
                90, 10, 0, counts(100, 0, 0, 0), false, NOW, properties, new BigDecimal("95")
        );

        assertEquals(ShadowCoverageSource.HISTORICAL, result.coverageSourceUsed());
        assertEquals(ShadowCoverageDecision.COVERAGE_NOT_READY, result.coverageDecision());
        assertEquals(ShadowCoverageReasonCode.SHADOW_COVERAGE_LEGACY_USED, result.coverageReasonCode());
        assertEquals(new BigDecimal("90.000000"), result.historicalCoveragePct());
        assertEquals(new BigDecimal("100.000000"), result.rollingCoveragePct());
    }

    @Test
    void auditModeCalculatesRollingButDecidesWithHistorical() {
        ShadowCoverageWindowProperties properties = rollingProperties();
        properties.setMode(ShadowCoverageMode.AUDIT);

        ShadowCoverageSnapshot result = calculator.calculate(
                90, 10, 0, counts(100, 0, 0, 0), false, NOW, properties, new BigDecimal("95")
        );

        assertEquals(ShadowCoverageSource.HISTORICAL, result.coverageSourceUsed());
        assertEquals(ShadowCoverageDecision.COVERAGE_NOT_READY, result.coverageDecision());
        assertEquals(ShadowCoverageDecision.COVERAGE_READY, result.rollingCoverageDecision());
        assertEquals(ShadowCoverageReasonCode.SHADOW_COVERAGE_ROLLING_READY, result.rollingCoverageReasonCode());
    }

    @Test
    void historicalCoverageRemainsAvailableForAudit() {
        ShadowCoverageSnapshot result = calculate(423, 226, 0, counts(100, 0, 0, 0));

        assertEquals(423, result.historicalSimulatedEvents());
        assertEquals(226, result.historicalSkippedEvents());
        assertEquals(new BigDecimal("65.177196"), result.historicalCoveragePct());
    }

    @Test
    void windowIsUtcAndLowerBoundaryIsInclusiveByContract() {
        ShadowCoverageSnapshot result = calculate(100, 0, 0, counts(100, 0, 0, 0));

        assertEquals(ZoneOffset.UTC, result.rollingWindowStart().getOffset());
        assertEquals(ZoneOffset.UTC, result.rollingWindowEnd().getOffset());
        assertEquals(NOW.minusDays(14), result.rollingWindowStart());
        assertEquals(NOW, result.rollingWindowEnd());
    }

    @Test
    void onlyLatestMaxEventsCanReachCalculator() {
        ShadowCoverageSnapshot result = calculate(1_000, 0, 0, counts(475, 0, 25, 0));

        assertEquals(500, result.rollingEvaluableEvents());
        assertEquals(500, result.rollingMaxEvents());
        assertEquals(new BigDecimal("95.000000"), result.rollingCoveragePct());
    }

    @Test
    void calculatorNeverMutatesInputCounts() {
        ShadowCoverageCounts counts = counts(95, 0, 5, 0);

        calculate(100, 0, 0, counts);

        assertEquals(95, counts.simulatedEvents());
        assertEquals(5, counts.skippedEvents());
        assertFalse(counts.queryFailed());
    }

    private ShadowCoverageSnapshot calculate(
            long historicalSimulated,
            long historicalSkipped,
            long historicalErrors,
            ShadowCoverageCounts rollingCounts
    ) {
        return calculator.calculate(
                historicalSimulated,
                historicalSkipped,
                historicalErrors,
                rollingCounts,
                false,
                NOW,
                rollingProperties(),
                new BigDecimal("95")
        );
    }

    private static ShadowCoverageWindowProperties rollingProperties() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();
        properties.setRollingEnabled(true);
        properties.setMode(ShadowCoverageMode.ROLLING);
        properties.setWindowDays(14);
        properties.setMaxEvents(500);
        properties.setMinEvaluableEvents(100);
        properties.setMinimumPercent(new BigDecimal("95"));
        return properties;
    }

    private static ShadowCoverageCounts counts(long simulated, long recorded, long skipped, long errors) {
        return new ShadowCoverageCounts(
                12L,
                simulated,
                recorded,
                skipped,
                errors,
                NOW.minusHours(4),
                NOW,
                false
        );
    }
}
