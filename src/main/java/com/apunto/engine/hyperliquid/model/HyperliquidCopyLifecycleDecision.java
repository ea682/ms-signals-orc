package com.apunto.engine.hyperliquid.model;

public record HyperliquidCopyLifecycleDecision(
        boolean allowed,
        String reasonCode,
        boolean cacheActive
) {
    public static HyperliquidCopyLifecycleDecision allow(boolean cacheActive) {
        return new HyperliquidCopyLifecycleDecision(true, "allowed", cacheActive);
    }

    public static HyperliquidCopyLifecycleDecision skip(String reasonCode, boolean cacheActive) {
        String code = reasonCode == null || reasonCode.isBlank() ? "copy_lifecycle_skip" : reasonCode;
        return new HyperliquidCopyLifecycleDecision(false, code, cacheActive);
    }
}
