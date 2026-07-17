package com.apunto.engine.service.copy.calibration;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

public record CalibrationEstimate(
        boolean available,
        BigDecimal p50,
        BigDecimal p75,
        BigDecimal p90,
        BigDecimal robustMean,
        BigDecimal sampleStddev,
        int sampleCount,
        int effectiveSampleCount,
        OffsetDateTime oldestSampleAt,
        OffsetDateTime newestSampleAt,
        CalibrationConfidence confidence,
        String fallbackLevel,
        String modelVersion,
        List<String> reasonCodes
) {
}

