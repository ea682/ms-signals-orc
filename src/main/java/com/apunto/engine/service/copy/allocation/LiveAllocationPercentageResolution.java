package com.apunto.engine.service.copy.allocation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.Locale;
import java.util.UUID;

public record LiveAllocationPercentageResolution(
        boolean resolved,
        BigDecimal percentage,
        BigDecimal strategyPercentage,
        BigDecimal walletTotalPercentage,
        String source,
        UUID sourceId,
        OffsetDateTime calculatedAt,
        OffsetDateTime validUntil,
        String reasonCode
) {
    private static final int SCALE = 6;

    public LiveAllocationPercentageResolution {
        rejectExcessScale(percentage, "percentage");
        rejectExcessScale(strategyPercentage, "strategyPercentage");
        rejectExcessScale(walletTotalPercentage, "walletTotalPercentage");
        percentage = scale(percentage);
        strategyPercentage = scale(strategyPercentage);
        walletTotalPercentage = scale(walletTotalPercentage);
        source = normalize(source);
        reasonCode = normalizeReason(reasonCode);
    }

    public static LiveAllocationPercentageResolution resolved(
            BigDecimal strategyPercentage,
            BigDecimal walletTotalPercentage,
            String source,
            UUID sourceId,
            OffsetDateTime calculatedAt,
            OffsetDateTime validUntil
    ) {
        return new LiveAllocationPercentageResolution(
                true,
                strategyPercentage,
                strategyPercentage,
                walletTotalPercentage,
                source,
                sourceId,
                calculatedAt,
                validUntil,
                "LIVE_ALLOCATION_PCT_RESOLVED"
        );
    }

    public static LiveAllocationPercentageResolution rejected(String reasonCode) {
        return new LiveAllocationPercentageResolution(
                false, null, null, null, null, null, null, null, reasonCode);
    }

    public boolean validForLive() {
        return resolved
                && percentage != null
                && percentage.signum() > 0
                && percentage.compareTo(BigDecimal.ONE) <= 0
                && strategyPercentage != null
                && walletTotalPercentage != null
                && walletTotalPercentage.signum() > 0
                && walletTotalPercentage.compareTo(BigDecimal.ONE) <= 0
                && strategyPercentage.compareTo(walletTotalPercentage) <= 0
                && source != null
                && !"FIXED_MICRO_BUDGET".equals(source)
                && !"LEGACY_MICRO_LIVE_SENTINEL".equals(source)
                && !"LEGACY_MICRO_PCT_IGNORED".equals(source)
                && sourceId != null
                && calculatedAt != null
                && validUntil != null;
    }

    private static BigDecimal scale(BigDecimal value) {
        return value == null ? null : value.setScale(SCALE, RoundingMode.HALF_UP);
    }

    private static void rejectExcessScale(BigDecimal value, String field) {
        if (value != null && value.scale() > SCALE) {
            throw new IllegalArgumentException(field + " scale must be <= " + SCALE);
        }
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizeReason(String value) {
        String normalized = normalize(value);
        return normalized == null ? "LIVE_DISTRIBUTION_NOT_AVAILABLE" : normalized.replace('-', '_');
    }
}
