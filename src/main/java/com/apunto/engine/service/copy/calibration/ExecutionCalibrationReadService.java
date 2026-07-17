package com.apunto.engine.service.copy.calibration;

import com.apunto.engine.shared.metric.MetricStrategyIdentity;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class ExecutionCalibrationReadService {

    private final ExecutionCalibrationEvidenceStore store;
    private final ExecutionCalibrationEngine engine;
    private final MeterRegistry meterRegistry;
    private final AtomicReference<Double> shadowRealityScore = new AtomicReference<>(Double.NaN);

    public ExecutionCalibrationReadService(ExecutionCalibrationEvidenceStore store,
                                           ExecutionCalibrationProperties properties,
                                           MeterRegistry meterRegistry) {
        this.store = store;
        this.engine = new ExecutionCalibrationEngine(
                properties.getMediumSampleSize(),
                properties.getHighSampleSize(),
                properties.getMaximumAge());
        this.meterRegistry = meterRegistry;
        meterRegistry.gauge("signals.shadow.reality_score", shadowRealityScore,
                value -> value.get());
    }

    public ExecutionCalibrationResponse estimate(String strategyKey,
                                                  String generationId,
                                                  ExecutionCalibrationMetric metric,
                                                  CalibrationSegment segment,
                                                  OffsetDateTime now) {
        String canonicalKey = requireCanonicalKey(strategyKey);
        String generation = requireText(generationId, "CALIBRATION_GENERATION_REQUIRED");
        CalibrationSegment requested = segment == null
                ? new CalibrationSegment("ALL", "ALL", "ALL", "ALL")
                : segment;
        OffsetDateTime calculatedAt = now == null ? OffsetDateTime.now() : now;
        Timer.Sample timer = Timer.start(meterRegistry);
        CalibrationEstimate estimate;
        try {
            estimate = engine.estimate(requested,
                    store.find(canonicalKey, generation, metric), calculatedAt);
        } catch (DataAccessException error) {
            String reason = schemaReason(error);
            estimate = new CalibrationEstimate(false, null, null, null, null, null,
                    0, 0, null, null, CalibrationConfidence.LOW, "NONE", "UNKNOWN", List.of(reason));
        }
        List<String> reasons = new ArrayList<>(estimate.reasonCodes());
        if (!estimate.available() && reasons.isEmpty()) reasons.add("CALIBRATION_EVIDENCE_MISSING");
        updateShadowRealityScore(metric, estimate);
        meterRegistry.counter("signals.shadow.calibration.total",
                "metric", metric.name(),
                "available", Boolean.toString(estimate.available())).increment();
        meterRegistry.counter("signals.shadow.calibration.sample.total",
                "metric", metric.name()).increment(estimate.sampleCount());
        timer.stop(meterRegistry.timer("signals.shadow.calibration.duration",
                "metric", metric.name()));
        log.info("event=signals.shadow.calibration strategyKey={} generationId={} metric={} fallback={} samples={} effectiveSamples={} confidence={} available={} reasonCodes={} elapsedRecorded=true allowsMoney=false",
                canonicalKey, generation, metric, estimate.fallbackLevel(), estimate.sampleCount(),
                estimate.effectiveSampleCount(), estimate.confidence(), estimate.available(), reasons);
        return new ExecutionCalibrationResponse(
                canonicalKey, generation, metric, metric.unit(), requested, estimate,
                false, false, List.copyOf(reasons), calculatedAt);
    }

    private void updateShadowRealityScore(ExecutionCalibrationMetric metric,
                                          CalibrationEstimate estimate) {
        if (metric != ExecutionCalibrationMetric.PNL_CAPTURE_RATIO
                || !estimate.available() || estimate.p50() == null) {
            return;
        }
        double score = estimate.p50().movePointRight(2).doubleValue();
        if (Double.isFinite(score)) {
            shadowRealityScore.set(Math.max(0.0d, Math.min(100.0d, score)));
        }
    }

    private String requireCanonicalKey(String value) {
        String key = requireText(value, "CALIBRATION_STRATEGY_KEY_REQUIRED");
        String[] parts = key.split("\\|", -1);
        if (parts.length != 4) throw new IllegalArgumentException("CALIBRATION_STRATEGY_KEY_INVALID");
        String canonical = MetricStrategyIdentity.canonicalKey(parts[0], parts[1], parts[2], parts[3]);
        if (!canonical.equals(key)) throw new IllegalArgumentException("CALIBRATION_STRATEGY_KEY_NOT_CANONICAL");
        return canonical;
    }

    private String requireText(String value, String reason) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(reason);
        return value.trim();
    }

    private String schemaReason(DataAccessException error) {
        Throwable current = error;
        while (current != null) {
            String message = String.valueOf(current.getMessage());
            if (message.contains("copy_micro_live_calibration_v3")) {
                return "CALIBRATION_READ_MODEL_UNAVAILABLE";
            }
            current = current.getCause();
        }
        return "CALIBRATION_READ_FAILED";
    }
}
