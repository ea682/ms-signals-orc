package com.apunto.engine.hyperliquid.model;

import java.util.Locale;

public enum HyperliquidDeltaType {
    OPEN,
    RESIZE,
    UPDATE,
    CLOSE,
    FLIP,
    NO_CHANGE,
    UNKNOWN;

    public static HyperliquidDeltaType from(String value) {
        if (value == null || value.isBlank()) {
            return UNKNOWN;
        }
        try {
            return HyperliquidDeltaType.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return UNKNOWN;
        }
    }

    public boolean isOpen() {
        return this == OPEN;
    }

    public boolean isClose() {
        return this == CLOSE;
    }

    public boolean isAdjustment() {
        return this == RESIZE || this == FLIP;
    }

    public boolean canStartCopyLifecycle() {
        return this == OPEN;
    }

    public boolean canAdjustExistingCopy() {
        return this == RESIZE || this == FLIP;
    }
}
