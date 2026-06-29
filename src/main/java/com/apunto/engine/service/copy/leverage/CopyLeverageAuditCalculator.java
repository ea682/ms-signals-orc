package com.apunto.engine.service.copy.leverage;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

public final class CopyLeverageAuditCalculator {

    private static final int SCALE = 18;
    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private CopyLeverageAuditCalculator() {
    }

    public static CopyLeverageSnapshot evaluateOpen(CopyLeverageAuditInput input) {
        CopyLeverageAuditInput safe = input == null ? CopyLeverageAuditInput.builder().build() : input;

        BigDecimal sourceLeverage = positiveOrNull(safe.sourceLeverageX());
        BigDecimal sourceEffective = firstPositive(
                safe.sourceEffectiveLeverageX(),
                effectiveLeverage(safe.sourceNotionalUsd(), safe.sourceMarginUsd())
        );
        BigDecimal cap = positiveOrNull(safe.leverageCapX());
        BigDecimal shadowRequested = positiveOrNull(safe.shadowRequestedLeverageX());
        BigDecimal shadowApplied = positiveOrNull(safe.shadowAppliedLeverageX());
        BigDecimal liveRequested = positiveOrNull(safe.liveRequestedLeverageX());
        BigDecimal liveExchange = positiveOrNull(safe.liveExchangeLeverageX());
        BigDecimal liveEffective = firstPositive(
                safe.liveEffectiveLeverageX(),
                effectiveLeverage(safe.liveNotionalUsd(), safe.liveRequiredMarginUsd())
        );

        boolean invalidExplicitLeverage = invalidLeverage(
                safe.sourceLeverageX(),
                safe.shadowRequestedLeverageX(),
                safe.shadowAppliedLeverageX(),
                safe.liveRequestedLeverageX(),
                safe.liveExchangeLeverageX(),
                safe.liveEffectiveLeverageX()
        );

        boolean capped = false;
        String capReason = null;
        BigDecimal uncappedTarget = firstPositive(shadowApplied, shadowRequested, liveRequested, sourceLeverage);
        if (sourceLeverage != null && cap != null && sourceLeverage.compareTo(cap) > 0) {
            capped = true;
            capReason = "LEVERAGE_CAPPED_BY_POLICY";
            if (shadowRequested == null) {
                shadowRequested = sourceLeverage;
            }
            if (shadowApplied == null) {
                shadowApplied = cap;
            }
            if (liveRequested == null) {
                liveRequested = shadowApplied;
            }
        } else if (shadowApplied == null) {
            shadowApplied = firstPositive(uncappedTarget, cap);
        }

        CopyLeverageStatus status = CopyLeverageStatus.OK;
        if (invalidExplicitLeverage) {
            status = CopyLeverageStatus.INVALID_LEVERAGE;
        } else if (sourceLeverage == null) {
            status = CopyLeverageStatus.MISSING_SOURCE_LEVERAGE;
            if (shadowApplied == null && cap != null) {
                shadowApplied = cap;
            }
        } else if (safe.requireMarginModeMatch() && marginModeMismatch(safe.sourceMarginMode(), safe.liveMarginMode())) {
            status = CopyLeverageStatus.MARGIN_MODE_MISMATCH;
        } else if (safe.notionalMismatch()) {
            status = CopyLeverageStatus.NOTIONAL_MISMATCH;
        } else if (safe.requireLiveExchangeLeverage() && liveRequested != null && liveExchange == null) {
            status = CopyLeverageStatus.MISSING_LIVE_EXCHANGE_LEVERAGE;
        } else if (liveRequested != null && liveExchange != null && liveRequested.compareTo(liveExchange) != 0) {
            status = CopyLeverageStatus.LEVERAGE_MISMATCH;
        } else if (capped) {
            status = CopyLeverageStatus.LEVERAGE_CAPPED;
        }

        return new CopyLeverageSnapshot(
                sourceLeverage,
                sourceEffective,
                shadowRequested,
                shadowApplied,
                liveRequested,
                liveExchange,
                liveEffective,
                positiveOrNull(safe.sourceNotionalUsd()),
                positiveOrNull(safe.sourceMarginUsd()),
                positiveOrNull(safe.shadowNotionalUsd()),
                positiveOrNull(safe.shadowRequiredMarginUsd()),
                positiveOrNull(safe.liveNotionalUsd()),
                positiveOrNull(safe.liveRequiredMarginUsd()),
                safe.sourceMarginMode(),
                safe.liveMarginMode(),
                cap,
                capped,
                capReason,
                firstText(safe.leverageSource(), sourceLeverage == null ? "missing" : "hyperliquid"),
                status
        );
    }

    public static CopyLeverageSnapshot inheritedForReduction(BigDecimal inheritedLeverageX,
                                                             BigDecimal liveNotionalUsd,
                                                             BigDecimal liveRequiredMarginUsd,
                                                             String liveMarginMode,
                                                             String leverageSource) {
        BigDecimal inherited = positiveOrNull(inheritedLeverageX);
        BigDecimal effective = effectiveLeverage(liveNotionalUsd, liveRequiredMarginUsd);
        CopyLeverageStatus status = inherited == null
                ? CopyLeverageStatus.LEVERAGE_NOT_APPLICABLE_FOR_REDUCTION
                : CopyLeverageStatus.OK;
        return new CopyLeverageSnapshot(
                inherited,
                null,
                null,
                inherited,
                null,
                inherited,
                effective,
                null,
                null,
                null,
                null,
                positiveOrNull(liveNotionalUsd),
                positiveOrNull(liveRequiredMarginUsd),
                null,
                liveMarginMode,
                null,
                false,
                null,
                firstText(leverageSource, "live_position"),
                status
        );
    }

    static BigDecimal effectiveLeverage(BigDecimal notional, BigDecimal margin) {
        BigDecimal n = positiveOrNull(notional);
        BigDecimal m = positiveOrNull(margin);
        if (n == null || m == null) {
            return null;
        }
        return n.divide(m, SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
    }

    private static boolean invalidLeverage(BigDecimal... values) {
        if (values == null) {
            return false;
        }
        for (BigDecimal value : values) {
            if (value != null && value.compareTo(ZERO) <= 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean marginModeMismatch(String source, String live) {
        if (source == null || source.isBlank() || live == null || live.isBlank()) {
            return false;
        }
        return !source.trim().equalsIgnoreCase(live.trim());
    }

    private static BigDecimal firstPositive(BigDecimal... values) {
        if (values == null) {
            return null;
        }
        for (BigDecimal value : values) {
            BigDecimal positive = positiveOrNull(value);
            if (positive != null) {
                return positive;
            }
        }
        return null;
    }

    private static BigDecimal positiveOrNull(BigDecimal value) {
        return value == null || value.compareTo(ZERO) <= 0 ? null : value.stripTrailingZeros();
    }

    private static String firstText(String first, String fallback) {
        return first == null || first.isBlank() ? Objects.toString(fallback, "unknown") : first.trim();
    }
}
