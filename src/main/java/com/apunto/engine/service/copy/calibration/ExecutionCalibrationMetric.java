package com.apunto.engine.service.copy.calibration;

import java.util.Locale;

public enum ExecutionCalibrationMetric {
    PNL_CAPTURE_RATIO("pnl_capture_ratio", "ratio"),
    FILL_ERROR_BPS("fill_error_bps", "bps"),
    FEE_ERROR_USD("fee_error_usd", "USD"),
    SLIPPAGE_ERROR_BPS("slippage_error_bps", "bps"),
    LATENCY_ERROR_MS("latency_error_ms", "ms"),
    PNL_ERROR_USD("pnl_error_usd", "USD");

    private final String columnName;
    private final String unit;

    ExecutionCalibrationMetric(String columnName, String unit) {
        this.columnName = columnName;
        this.unit = unit;
    }

    public String columnName() {
        return columnName;
    }

    public String unit() {
        return unit;
    }

    public static ExecutionCalibrationMetric parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("CALIBRATION_METRIC_REQUIRED");
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException("CALIBRATION_METRIC_UNSUPPORTED");
        }
    }
}
