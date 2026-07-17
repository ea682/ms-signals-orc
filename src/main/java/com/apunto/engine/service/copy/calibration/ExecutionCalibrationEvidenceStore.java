package com.apunto.engine.service.copy.calibration;

import java.util.List;

public interface ExecutionCalibrationEvidenceStore {
    List<CalibrationObservation> find(String strategyKey,
                                      String generationId,
                                      ExecutionCalibrationMetric metric);
}
