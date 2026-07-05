package com.apunto.engine.service.copy.capital;

import com.apunto.engine.shared.enums.FuturesCapitalAsset;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class CopyGlobalCapitalAllocator {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final int SCALE = 12;

    private CopyGlobalCapitalAllocator() {
    }

    public static CopyGlobalCapitalAllocation evaluate(
            BigDecimal availableBalanceAmount,
            BigDecimal usedMarginAmount,
            BigDecimal safetyReservePct,
            BigDecimal requiredMarginAmount,
            String capitalCurrency
    ) {
        final BigDecimal available = positiveOrZero(availableBalanceAmount);
        final BigDecimal used = positiveOrZero(usedMarginAmount);
        final BigDecimal safetyPct = clampPct(safetyReservePct);
        final BigDecimal required = positiveOrZero(requiredMarginAmount);
        final BigDecimal safetyReserve = available.multiply(safetyPct).setScale(SCALE, RoundingMode.HALF_UP);
        final BigDecimal availableAfterRequired = available
                .subtract(safetyReserve)
                .subtract(used)
                .subtract(required)
                .setScale(SCALE, RoundingMode.HALF_UP);
        final boolean overbooked = availableAfterRequired.compareTo(ZERO) < 0;
        final String currency = FuturesCapitalAsset.fromNullable(capitalCurrency).name();
        return new CopyGlobalCapitalAllocation(
                available.setScale(SCALE, RoundingMode.HALF_UP),
                used.setScale(SCALE, RoundingMode.HALF_UP),
                safetyReserve,
                required.setScale(SCALE, RoundingMode.HALF_UP),
                availableAfterRequired,
                currency,
                overbooked,
                !overbooked,
                overbooked ? "PRE_FLIGHT_BLOCKED_CAPITAL" : "OK"
        );
    }

    private static BigDecimal positiveOrZero(BigDecimal value) {
        return value == null || value.compareTo(ZERO) <= 0 ? ZERO : value;
    }

    private static BigDecimal clampPct(BigDecimal value) {
        if (value == null || value.compareTo(ZERO) <= 0) {
            return ZERO;
        }
        if (value.compareTo(BigDecimal.ONE) > 0) {
            return BigDecimal.ONE;
        }
        return value;
    }
}
