package com.apunto.engine.service.copy.calibration;

import java.time.OffsetDateTime;
import java.util.List;

public record ExecutionCalibrationResponse(
        String strategyKey,
        String generationId,
        ExecutionCalibrationMetric metric,
        String unit,
        CalibrationSegment requestedSegment,
        CalibrationEstimate estimate,
        boolean decisionFinal,
        boolean allowsMoney,
        List<String> reasonCodes,
        OffsetDateTime computedAt
) {
}
