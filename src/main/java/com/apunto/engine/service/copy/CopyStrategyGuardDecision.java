package com.apunto.engine.service.copy;

public record CopyStrategyGuardDecision(
        boolean allowed,
        String reason,
        String detail,
        String statusWhenBlocked
) {
    public static CopyStrategyGuardDecision allow() {
        return new CopyStrategyGuardDecision(true, "OK", "", null);
    }

    public static CopyStrategyGuardDecision blocked(String reason, String detail) {
        return new CopyStrategyGuardDecision(false, reason, detail == null ? "" : detail, statusFor(reason));
    }

    private static String statusFor(String reason) {
        if (reason == null) return "EXIT_ONLY";
        String r = reason.trim().toUpperCase(java.util.Locale.ROOT);
        if (r.contains("NEGATIVE")) return "PAUSED_BY_NEGATIVE_PNL";
        if (r.contains("STALE") || r.contains("MISSING")) return "PAUSED_BY_STALE_METRIC";
        if (r.contains("RISK") || r.contains("COVERAGE")) return "PAUSED_BY_RISK";
        return "EXIT_ONLY";
    }
}
