package com.apunto.engine.service.copy.leverage;

import java.math.BigDecimal;
import java.util.Locale;

public record CopyLeverageSnapshot(
        BigDecimal sourceLeverageX,
        BigDecimal sourceEffectiveLeverageX,
        BigDecimal shadowRequestedLeverageX,
        BigDecimal shadowAppliedLeverageX,
        BigDecimal liveRequestedLeverageX,
        BigDecimal liveExchangeLeverageX,
        BigDecimal liveEffectiveLeverageX,
        BigDecimal sourceNotionalUsd,
        BigDecimal sourceMarginUsd,
        BigDecimal shadowNotionalUsd,
        BigDecimal shadowRequiredMarginUsd,
        BigDecimal liveNotionalUsd,
        BigDecimal liveRequiredMarginUsd,
        String sourceMarginMode,
        String liveMarginMode,
        BigDecimal leverageCapX,
        boolean leverageWasCapped,
        String leverageCapReason,
        String leverageSource,
        CopyLeverageStatus leverageStatus
) {
    public static CopyLeverageSnapshot unknown() {
        return new CopyLeverageSnapshot(
                null, null, null, null, null, null, null,
                null, null, null, null, null, null,
                null, null, null, false, null, "unknown", CopyLeverageStatus.UNKNOWN
        );
    }

    public String statusCode() {
        return leverageStatus == null ? CopyLeverageStatus.UNKNOWN.name() : leverageStatus.name();
    }

    public String logFields() {
        return "sourceLeverageX=" + plain(sourceLeverageX)
                + " sourceEffectiveLeverageX=" + plain(sourceEffectiveLeverageX)
                + " shadowAppliedLeverageX=" + plain(shadowAppliedLeverageX)
                + " liveRequestedLeverageX=" + plain(liveRequestedLeverageX)
                + " liveExchangeLeverageX=" + plain(liveExchangeLeverageX)
                + " liveEffectiveLeverageX=" + plain(liveEffectiveLeverageX)
                + " leverageStatus=" + statusCode()
                + " leverageWasCapped=" + leverageWasCapped
                + " leverageCapX=" + plain(leverageCapX)
                + " leverageCapReason=" + text(leverageCapReason)
                + " leverageSource=" + text(leverageSource)
                + " sourceNotionalUsd=" + plain(sourceNotionalUsd)
                + " shadowNotionalUsd=" + plain(shadowNotionalUsd)
                + " liveNotionalUsd=" + plain(liveNotionalUsd)
                + " shadowRequiredMarginUsd=" + plain(shadowRequiredMarginUsd)
                + " liveRequiredMarginUsd=" + plain(liveRequiredMarginUsd)
                + " sourceMarginMode=" + text(sourceMarginMode)
                + " liveMarginMode=" + text(liveMarginMode);
    }

    private static String plain(BigDecimal value) {
        return value == null ? "NA" : value.stripTrailingZeros().toPlainString();
    }

    private static String text(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        return value.trim()
                .replace('\n', '_')
                .replace('\r', '_')
                .replace('\t', '_')
                .replace(' ', '_')
                .replace('"', '\'')
                .toUpperCase(Locale.ROOT);
    }
}
