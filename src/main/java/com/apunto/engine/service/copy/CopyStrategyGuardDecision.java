package com.apunto.engine.service.copy;

import java.time.OffsetDateTime;

public record CopyStrategyGuardDecision(
        boolean allowed,
        String reason,
        String detail,
        String statusWhenBlocked,
        String action,
        double capitalMultiplier,
        String targetExecutionMode,
        String decisionSource,
        String decisionVersion,
        OffsetDateTime computedAt,
        OffsetDateTime expiresAt,
        boolean decisionFinal,
        String materializationStatus,
        String inputFingerprint
) {
    public CopyStrategyGuardDecision(boolean allowed,
                                     String reason,
                                     String detail,
                                     String statusWhenBlocked,
                                     String action,
                                     double capitalMultiplier,
                                     String targetExecutionMode) {
        this(allowed, reason, detail, statusWhenBlocked, action, capitalMultiplier, targetExecutionMode,
                "UNSPECIFIED", null, null, null, false, "NOT_MATERIALIZED", null);
    }

    public CopyStrategyGuardDecision withMetadata(String decisionSource,
                                                  String decisionVersion,
                                                  OffsetDateTime computedAt,
                                                  OffsetDateTime expiresAt,
                                                  boolean decisionFinal,
                                                  String materializationStatus,
                                                  String inputFingerprint) {
        return new CopyStrategyGuardDecision(
                allowed, reason, detail, statusWhenBlocked, action, capitalMultiplier, targetExecutionMode,
                decisionSource, decisionVersion, computedAt, expiresAt, decisionFinal,
                materializationStatus, inputFingerprint
        );
    }

    public static CopyStrategyGuardDecision allow() {
        return new CopyStrategyGuardDecision(true, "OK", "", null, "ALLOW", 1.0, "KEEP");
    }

    public static CopyStrategyGuardDecision warn(String reason, String detail, double capitalMultiplier) {
        return new CopyStrategyGuardDecision(true, reason, detail == null ? "" : detail, null, "WARNING", clampMultiplier(capitalMultiplier), "KEEP");
    }

    public static CopyStrategyGuardDecision info(String reason, String detail) {
        return new CopyStrategyGuardDecision(true, reason, detail == null ? "" : detail, null, "INFO", 1.0, "KEEP");
    }

    public static CopyStrategyGuardDecision reduce(String reason, String detail, double capitalMultiplier) {
        return new CopyStrategyGuardDecision(true, reason, detail == null ? "" : detail, null, "REDUCE_CAPITAL", clampMultiplier(capitalMultiplier), "KEEP");
    }

    public static CopyStrategyGuardDecision shadowOnly(String reason, String detail, double capitalMultiplier) {
        return new CopyStrategyGuardDecision(true, reason, detail == null ? "" : detail, null, "SHADOW_ONLY", clampMultiplier(capitalMultiplier), "SHADOW");
    }

    public static CopyStrategyGuardDecision watchlistShadow(String reason, String detail) {
        return new CopyStrategyGuardDecision(false, reason, detail == null ? "" : detail, "OBSERVATION_SHADOW", "WATCHLIST_SHADOW", 0.0, "SHADOW");
    }

    public static CopyStrategyGuardDecision shadowRevalidation(String reason, String detail) {
        return new CopyStrategyGuardDecision(false, reason, detail == null ? "" : detail, "SHADOW_REVALIDATION", "SHADOW_REVALIDATION", 0.0, "SHADOW");
    }

    public static CopyStrategyGuardDecision microLiveRequiredReentry(String reason, String detail) {
        return new CopyStrategyGuardDecision(false, reason, detail == null ? "" : detail, "MICRO_LIVE_REQUIRED_REENTRY", "MICRO_LIVE_REQUIRED_REENTRY", 0.0, "MICRO_LIVE");
    }

    public static CopyStrategyGuardDecision manualReview(String reason, String detail) {
        return new CopyStrategyGuardDecision(false, reason, detail == null ? "" : detail, "MANUAL_REVIEW", "MANUAL_REVIEW", 0.0, "KEEP");
    }

    public static CopyStrategyGuardDecision blocked(String reason, String detail) {
        return new CopyStrategyGuardDecision(false, reason, detail == null ? "" : detail, statusFor(reason), "PAUSE_OPEN", 0.0, "KEEP");
    }

    public static CopyStrategyGuardDecision disabled(String reason, String detail) {
        return new CopyStrategyGuardDecision(false, reason, detail == null ? "" : detail, "PAUSED_BY_RISK", "DISABLED", 0.0, "KEEP");
    }

    private static String statusFor(String reason) {
        if (reason == null) return "EXIT_ONLY";
        String r = reason.trim().toUpperCase(java.util.Locale.ROOT);
        if (r.contains("NEGATIVE")) return "PAUSED_BY_NEGATIVE_PNL";
        if (r.contains("STALE") || r.contains("MISSING")) return "PAUSED_BY_STALE_METRIC";
        if (r.contains("RISK") || r.contains("COVERAGE") || r.contains("PAUSE") || r.contains("GUARD")) return "PAUSED_BY_RISK";
        return "EXIT_ONLY";
    }

    private static double clampMultiplier(double value) {
        if (!Double.isFinite(value)) return 1.0;
        return Math.max(0.0, Math.min(1.0, value));
    }
}
