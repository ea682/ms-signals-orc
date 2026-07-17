package com.apunto.engine.service.copy.calibration;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionCalibrationEngineTest {

    private static final OffsetDateTime NOW = OffsetDateTime.of(
            2026, 7, 14, 12, 0, 0, 0, ZoneOffset.UTC);

    private final ExecutionCalibrationEngine engine = new ExecutionCalibrationEngine(
            10, 30, Duration.ofDays(14));

    @Test
    void calculatesP50P75P90RobustMeanAndConfidence() {
        List<CalibrationObservation> observations = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 1000)
                .stream()
                .map(value -> observation("BTCUSDT", "LONG", "OPEN", "SMALL", value, NOW.minusDays(1)))
                .toList();

        CalibrationEstimate estimate = engine.estimate(
                new CalibrationSegment("BTCUSDT", "LONG", "OPEN", "SMALL"), observations, NOW);

        assertEquals(0, estimate.p50().compareTo(new BigDecimal("5.5")));
        assertEquals(0, estimate.p75().compareTo(new BigDecimal("7.75")));
        assertEquals(0, estimate.p90().compareTo(new BigDecimal("108.1")));
        assertTrue(estimate.robustMean().compareTo(new BigDecimal("100")) < 0);
        assertEquals(CalibrationConfidence.MEDIUM, estimate.confidence());
        assertEquals("EXACT", estimate.fallbackLevel());
    }

    @Test
    void usesHierarchicalFallbackAndMakesItObservable() {
        List<CalibrationObservation> observations = List.of(
                observation("ETHUSDT", "LONG", "OPEN", "SMALL", 4, NOW.minusHours(2)),
                observation("ETHUSDT", "LONG", "OPEN", "SMALL", 6, NOW.minusHours(1)));

        CalibrationEstimate estimate = engine.estimate(
                new CalibrationSegment("BTCUSDT", "LONG", "OPEN", "SMALL"), observations, NOW);

        assertEquals("ACTION_NOTIONAL", estimate.fallbackLevel());
        assertTrue(estimate.reasonCodes().contains("CALIBRATION_FALLBACK_ACTION_NOTIONAL"));
        assertEquals(CalibrationConfidence.LOW, estimate.confidence());
    }

    @Test
    void staleEvidenceIsUnavailableInsteadOfSilentlyZero() {
        CalibrationEstimate estimate = engine.estimate(
                new CalibrationSegment("BTCUSDT", "LONG", "OPEN", "SMALL"),
                List.of(observation("BTCUSDT", "LONG", "OPEN", "SMALL", 0, NOW.minusDays(30))), NOW);

        assertFalse(estimate.available());
        assertEquals(null, estimate.p50());
        assertTrue(estimate.reasonCodes().contains("CALIBRATION_STALE"));
    }

    @Test
    void zeroIsRealOnlyWhenBackedByFreshEvidence() {
        CalibrationEstimate estimate = engine.estimate(
                new CalibrationSegment("BTCUSDT", "LONG", "OPEN", "SMALL"),
                List.of(observation("BTCUSDT", "LONG", "OPEN", "SMALL", 0, NOW.minusHours(1))), NOW);

        assertTrue(estimate.available());
        assertEquals(0, estimate.p50().compareTo(BigDecimal.ZERO));
        assertTrue(estimate.reasonCodes().contains("ZERO_REAL"));
    }

    @Test
    void usesWindowSampleCountAsStatisticalWeight() {
        CalibrationSegment segment = new CalibrationSegment("BTCUSDT", "LONG", "OPEN", "SMALL");
        CalibrationEstimate estimate = engine.estimate(segment, List.of(
                new CalibrationObservation(segment, BigDecimal.ONE, NOW.minusHours(2), "v1", 100),
                new CalibrationObservation(segment, new BigDecimal("100"), NOW.minusHours(1), "v1", 1)
        ), NOW);

        assertEquals(101, estimate.sampleCount());
        assertEquals(2, estimate.effectiveSampleCount());
        assertEquals(0, BigDecimal.ONE.compareTo(estimate.p50()));
        assertEquals(CalibrationConfidence.LOW, estimate.confidence());
    }

    private static CalibrationObservation observation(String symbol, String side, String action,
                                                      String notionalBand, double value,
                                                      OffsetDateTime observedAt) {
        return new CalibrationObservation(
                new CalibrationSegment(symbol, side, action, notionalBand),
                BigDecimal.valueOf(value), observedAt, "calibration-v1");
    }
}
