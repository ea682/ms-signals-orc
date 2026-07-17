package com.apunto.engine.service.copy.calibration;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record CalibrationObservation(
        CalibrationSegment segment,
        BigDecimal value,
        OffsetDateTime observedAt,
        String modelVersion,
        int sampleCount
) {
    public CalibrationObservation {
        sampleCount = Math.max(1, sampleCount);
    }

    public CalibrationObservation(CalibrationSegment segment,
                                  BigDecimal value,
                                  OffsetDateTime observedAt,
                                  String modelVersion) {
        this(segment, value, observedAt, modelVersion, 1);
    }
}
