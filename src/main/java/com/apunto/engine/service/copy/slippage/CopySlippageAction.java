package com.apunto.engine.service.copy.slippage;

import java.util.Locale;

public enum CopySlippageAction {
    OPEN,
    INCREASE,
    REDUCE,
    CLOSE,
    PANIC_CLOSE,
    FLIP,
    UNKNOWN;

    public static CopySlippageAction fromEventType(String eventType) {
        if (eventType == null || eventType.isBlank()) {
            return UNKNOWN;
        }
        return switch (eventType.trim().toUpperCase(Locale.ROOT)) {
            case "OPEN" -> OPEN;
            case "INCREASE" -> INCREASE;
            case "REDUCE" -> REDUCE;
            case "CLOSE" -> CLOSE;
            case "PANIC_CLOSE" -> PANIC_CLOSE;
            case "FLIP" -> FLIP;
            default -> UNKNOWN;
        };
    }
}
