package com.apunto.engine.service.copy.readiness;

import java.math.BigDecimal;
import java.util.List;

public record ShadowLiveReadinessDecision(
        ShadowLiveReadinessStatus status,
        List<String> reasonCodes,
        String recommendedExecutionMode,
        String riskClass,
        BigDecimal evidenceScore,
        List<String> hardBlockers,
        List<String> softWarnings,
        List<String> positiveReasons,
        List<String> dataWarnings,
        List<String> capitalWarnings,
        List<String> flags,
        CapitalDecision capitalDecision
) {
    public ShadowLiveReadinessDecision(
            ShadowLiveReadinessStatus status,
            List<String> reasonCodes,
            String recommendedExecutionMode,
            String riskClass,
            BigDecimal evidenceScore,
            List<String> hardBlockers,
            List<String> softWarnings,
            CapitalDecision capitalDecision
    ) {
        this(
                status,
                reasonCodes,
                recommendedExecutionMode,
                riskClass,
                evidenceScore,
                hardBlockers,
                softWarnings,
                List.of(),
                classifyDataWarnings(softWarnings),
                classifyCapitalWarnings(softWarnings),
                List.of(),
                capitalDecision
        );
    }

    public ShadowLiveReadinessDecision(ShadowLiveReadinessStatus status, List<String> reasonCodes) {
        this(
                status,
                reasonCodes,
                defaultExecutionMode(status),
                "B",
                null,
                status == ShadowLiveReadinessStatus.BLOCKED || status == ShadowLiveReadinessStatus.NOT_READY
                        ? reasonCodes
                        : List.of(),
                status == ShadowLiveReadinessStatus.NEEDS_MORE_DATA ? reasonCodes : List.of(),
                List.of(),
                status == ShadowLiveReadinessStatus.NEEDS_MORE_DATA ? reasonCodes : List.of(),
                List.of(),
                List.of(),
                CapitalDecision.forMode(defaultExecutionMode(status), false, reasonCodes)
        );
    }

    public ShadowLiveReadinessDecision {
        reasonCodes = reasonCodes == null ? List.of() : List.copyOf(reasonCodes);
        recommendedExecutionMode = recommendedExecutionMode == null || recommendedExecutionMode.isBlank()
                ? defaultExecutionMode(status)
                : recommendedExecutionMode.trim().toUpperCase().replace('-', '_');
        riskClass = riskClass == null || riskClass.isBlank() ? "B" : riskClass.trim().toUpperCase();
        hardBlockers = hardBlockers == null ? List.of() : List.copyOf(hardBlockers);
        softWarnings = softWarnings == null ? List.of() : List.copyOf(softWarnings);
        positiveReasons = positiveReasons == null ? List.of() : List.copyOf(positiveReasons);
        dataWarnings = dataWarnings == null ? List.of() : List.copyOf(dataWarnings);
        capitalWarnings = capitalWarnings == null ? List.of() : List.copyOf(capitalWarnings);
        flags = flags == null ? List.of() : List.copyOf(flags);
        capitalDecision = capitalDecision == null
                ? CapitalDecision.forMode(recommendedExecutionMode, false, reasonCodes)
                : capitalDecision;
    }

    public boolean approvedForLive() {
        return status == ShadowLiveReadinessStatus.APPROVED_FOR_LIVE;
    }

    public String primaryReasonCode() {
        return reasonCodes.isEmpty() ? "NA" : reasonCodes.get(0);
    }

    private static String defaultExecutionMode(ShadowLiveReadinessStatus status) {
        if (status == ShadowLiveReadinessStatus.APPROVED_FOR_LIVE) return "LIVE";
        if (status == ShadowLiveReadinessStatus.MICRO_LIVE_READY) return "MICRO_LIVE";
        return "SHADOW";
    }

    private static List<String> classifyDataWarnings(List<String> reasons) {
        if (reasons == null || reasons.isEmpty()) return List.of();
        return reasons.stream()
                .filter(ShadowLiveReadinessDecision::isDataWarning)
                .distinct()
                .toList();
    }

    private static List<String> classifyCapitalWarnings(List<String> reasons) {
        if (reasons == null || reasons.isEmpty()) return List.of();
        return reasons.stream()
                .filter(reason -> reason != null && (
                        reason.contains("CAPITAL")
                                || reason.contains("EXPOSURE")
                                || reason.contains("SCALE")
                ))
                .distinct()
                .toList();
    }

    private static boolean isDataWarning(String reason) {
        return reason != null && (
                reason.contains("DATA")
                        || reason.contains("SAMPLE")
                        || reason.contains("POSITIONS")
                        || reason.contains("SLIPPAGE")
                        || reason.contains("LATENCY")
                        || reason.contains("STALE")
        );
    }

    public record CapitalDecision(
            String action,
            BigDecimal baseCapitalUSDT,
            BigDecimal maxCapitalUSDT,
            BigDecimal capitalMultiplier,
            boolean canScale,
            List<String> reasons
    ) {
        public CapitalDecision {
            action = action == null || action.isBlank() ? "NO_ALLOCATE" : action.trim().toUpperCase();
            baseCapitalUSDT = baseCapitalUSDT == null ? BigDecimal.ZERO : baseCapitalUSDT;
            maxCapitalUSDT = maxCapitalUSDT == null ? BigDecimal.ZERO : maxCapitalUSDT;
            capitalMultiplier = capitalMultiplier == null ? BigDecimal.ZERO : capitalMultiplier;
            reasons = reasons == null ? List.of() : List.copyOf(reasons);
        }

        static CapitalDecision forMode(String mode, boolean canScale, List<String> reasons) {
            if ("LIVE".equals(mode)) {
                return new CapitalDecision(
                        "ALLOCATE",
                        new BigDecimal("100"),
                        new BigDecimal("100"),
                        BigDecimal.ONE,
                        canScale,
                        reasons
                );
            }
            if ("MICRO_LIVE".equals(mode)) {
                return new CapitalDecision(
                        "MICRO_ALLOCATE",
                        new BigDecimal("100"),
                        new BigDecimal("100"),
                        BigDecimal.ONE,
                        false,
                        reasons
                );
            }
            return new CapitalDecision("NO_ALLOCATE", BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO, false, reasons);
        }

        public BigDecimal baseCapitalAmount() {
            return baseCapitalUSDT;
        }

        public BigDecimal maxCapitalAmount() {
            return maxCapitalUSDT;
        }

        public BigDecimal targetCapitalAmount() {
            return maxCapitalUSDT;
        }

        public String capitalCurrency() {
            return "AUTO";
        }

        public String quoteAsset() {
            return "AUTO";
        }

        public String collateralAsset() {
            return "AUTO";
        }
    }
}
