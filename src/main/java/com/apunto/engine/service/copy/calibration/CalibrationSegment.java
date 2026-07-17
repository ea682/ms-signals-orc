package com.apunto.engine.service.copy.calibration;

import java.util.Locale;

public record CalibrationSegment(String symbol, String side, String action, String notionalBand) {
    public CalibrationSegment {
        symbol = normalize(symbol);
        side = normalize(side);
        action = normalize(action);
        notionalBand = normalize(notionalBand);
    }

    private static String normalize(String value) {
        return value == null ? "ALL" : value.trim().toUpperCase(Locale.ROOT);
    }
}

