package com.apunto.engine.shared.metric;

import java.util.Locale;

public final class MetricStrategyIdentity {

    public static final String DEFAULT_STRATEGY = "MOVEMENT_ALL";

    private MetricStrategyIdentity() {
    }

    public static String canonicalKey(
            String walletId,
            String strategyCode,
            String scopeType,
            String scopeValue
    ) {
        String strategy = strategyCode(strategyCode);
        return walletId(walletId)
                + "|" + strategy
                + "|" + scopeType(scopeType, strategy)
                + "|" + scopeValue(scopeValue, strategy);
    }

    public static String walletId(String value) {
        return normalize(value).toLowerCase(Locale.ROOT);
    }

    public static String strategyCode(String value) {
        String normalized = token(value);
        return normalized.isEmpty() ? DEFAULT_STRATEGY : normalized;
    }

    public static String scopeType(String value, String strategyCode) {
        String strategy = strategyCode(strategyCode);
        String normalized = token(value);
        if (!normalized.isEmpty() && !"STRATEGY".equals(normalized) && !"DEFAULT".equals(normalized)) {
            return normalized;
        }
        if ("LONG_ONLY".equals(strategy) || "SHORT_ONLY".equals(strategy)) return "DIRECTION";
        if ("SYMBOL_SPECIALIST".equals(strategy)) return "SYMBOL";
        if ("LOW_LEVERAGE_ONLY".equals(strategy)) return "LEVERAGE_RANGE";
        return "ALL";
    }

    public static String scopeValue(String value, String strategyCode) {
        String strategy = strategyCode(strategyCode);
        String normalized = token(value);
        if (isLegacyDefault(normalized, strategy)) {
            if ("LONG_ONLY".equals(strategy)) return "LONG";
            if ("SHORT_ONLY".equals(strategy)) return "SHORT";
            return "ALL";
        }
        return normalized;
    }

    private static boolean isLegacyDefault(String value, String strategy) {
        return value.isEmpty()
                || "DEFAULT".equals(value)
                || "STRATEGY".equals(value)
                || value.equals(strategy);
    }

    private static String token(String value) {
        return normalize(value).toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }
}
