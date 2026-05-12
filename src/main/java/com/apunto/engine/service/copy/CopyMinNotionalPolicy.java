package com.apunto.engine.service.copy;

import com.apunto.engine.shared.enums.CopyMinNotionalMode;

import java.math.BigDecimal;

/**
 * Politica efectiva, ya resuelta, para decidir si una orden bajo minimo Binance puede elevarse.
 */
public record CopyMinNotionalPolicy(
        CopyMinNotionalMode mode,
        Long allocationId,
        Integer score,
        Integer minScore,
        BigDecimal historyDays,
        Integer minHistoryDays,
        Integer operationsCount,
        Integer minOperations,
        BigDecimal maxNotionalUsdt
) {
    private static final BigDecimal ZERO = BigDecimal.ZERO;

    public CopyMinNotionalPolicy {
        mode = mode == null ? CopyMinNotionalMode.SKIP : mode;
        minScore = normalizePositiveInteger(minScore);
        minHistoryDays = normalizePositiveInteger(minHistoryDays);
        minOperations = normalizePositiveInteger(minOperations);
        maxNotionalUsdt = normalizePositive(maxNotionalUsdt);
        historyDays = normalizePositive(historyDays);
    }

    public static CopyMinNotionalPolicy skip() {
        return new CopyMinNotionalPolicy(CopyMinNotionalMode.SKIP, null, null, null, null, null, null, null, null);
    }

    public boolean isCopyByBinanceMinEnabled() {
        return mode == CopyMinNotionalMode.COPY_BY_BINANCE_MIN;
    }

    private static Integer normalizePositiveInteger(Integer value) {
        if (value == null || value <= 0) {
            return null;
        }
        return value;
    }

    private static BigDecimal normalizePositive(BigDecimal value) {
        if (value == null || value.compareTo(ZERO) <= 0) {
            return null;
        }
        return value;
    }
}
