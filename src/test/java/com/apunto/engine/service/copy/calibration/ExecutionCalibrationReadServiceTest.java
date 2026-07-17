package com.apunto.engine.service.copy.calibration;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionCalibrationReadServiceTest {

    private static final String STRATEGY_KEY = "0xa445|MOVEMENT_ALL|ALL|ALL";
    private static final String GENERATION_ID = "e3293fec-6f42-48cf-ae57-99a31d173f01";
    private static final OffsetDateTime NOW = OffsetDateTime.of(
            2026, 7, 14, 12, 0, 0, 0, ZoneOffset.UTC);

    @Test
    void aggregatesOnlyExactGenerationEvidenceAndNeverAuthorizesMoney() {
        FakeStore store = new FakeStore();
        CalibrationSegment segment = new CalibrationSegment("BTCUSDT", "LONG", "OPEN", "SMALL");
        store.values = List.of(
                        new CalibrationObservation(segment, BigDecimal.ONE, NOW.minusHours(2), "v3", 20),
                        new CalibrationObservation(segment, new BigDecimal("3"), NOW.minusHours(1), "v3", 20)
                );
        ExecutionCalibrationReadService service = service(store);

        ExecutionCalibrationResponse response = service.estimate(
                STRATEGY_KEY, GENERATION_ID, ExecutionCalibrationMetric.SLIPPAGE_ERROR_BPS,
                segment, NOW);

        assertEquals(STRATEGY_KEY, store.strategyKey);
        assertEquals(GENERATION_ID, store.generationId);
        assertEquals(ExecutionCalibrationMetric.SLIPPAGE_ERROR_BPS, store.metric);
        assertTrue(response.estimate().available());
        assertEquals(40, response.estimate().sampleCount());
        assertFalse(response.decisionFinal());
        assertFalse(response.allowsMoney());
    }

    @Test
    void missingEvidenceStaysUnknownInsteadOfBecomingZero() {
        FakeStore store = new FakeStore();
        ExecutionCalibrationReadService service = service(store);

        ExecutionCalibrationResponse response = service.estimate(
                STRATEGY_KEY, GENERATION_ID, ExecutionCalibrationMetric.FEE_ERROR_USD,
                new CalibrationSegment("BTCUSDT", "LONG", "OPEN", "SMALL"), NOW);

        assertFalse(response.estimate().available());
        assertEquals(null, response.estimate().p50());
        assertTrue(response.reasonCodes().contains("CALIBRATION_EVIDENCE_MISSING"));
        assertFalse(response.allowsMoney());
    }

    @Test
    void realityGaugeUsesKnownPnlCaptureAndUnknownDoesNotOverwriteIt() {
        FakeStore store = new FakeStore();
        CalibrationSegment segment = new CalibrationSegment("ALL", "ALL", "ALL", "ALL");
        store.values = List.of(
                new CalibrationObservation(segment, new BigDecimal("0.80"), NOW.minusHours(1), "v3", 20)
        );
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ExecutionCalibrationReadService service = service(store, registry);

        service.estimate(STRATEGY_KEY, GENERATION_ID,
                ExecutionCalibrationMetric.PNL_CAPTURE_RATIO, segment, NOW);
        assertEquals(80.0, registry.get("signals.shadow.reality_score").gauge().value());

        store.values = List.of();
        service.estimate(STRATEGY_KEY, GENERATION_ID,
                ExecutionCalibrationMetric.PNL_CAPTURE_RATIO, segment, NOW);
        assertEquals(80.0, registry.get("signals.shadow.reality_score").gauge().value());
    }

    private ExecutionCalibrationReadService service(ExecutionCalibrationEvidenceStore store) {
        return service(store, new SimpleMeterRegistry());
    }

    private ExecutionCalibrationReadService service(ExecutionCalibrationEvidenceStore store,
                                                    SimpleMeterRegistry registry) {
        ExecutionCalibrationProperties properties = new ExecutionCalibrationProperties();
        properties.setMediumSampleSize(10);
        properties.setHighSampleSize(30);
        properties.setMaximumAge(Duration.ofDays(14));
        return new ExecutionCalibrationReadService(store, properties, registry);
    }

    private static final class FakeStore implements ExecutionCalibrationEvidenceStore {
        private List<CalibrationObservation> values = List.of();
        private String strategyKey;
        private String generationId;
        private ExecutionCalibrationMetric metric;

        @Override
        public List<CalibrationObservation> find(String strategyKey,
                                                 String generationId,
                                                 ExecutionCalibrationMetric metric) {
            this.strategyKey = strategyKey;
            this.generationId = generationId;
            this.metric = metric;
            return values;
        }
    }
}
