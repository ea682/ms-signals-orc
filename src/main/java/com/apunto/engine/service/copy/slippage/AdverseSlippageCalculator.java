package com.apunto.engine.service.copy.slippage;

import com.apunto.engine.shared.enums.Side;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class AdverseSlippageCalculator {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal BPS = new BigDecimal("10000");
    private static final int SCALE = 12;

    private AdverseSlippageCalculator() {
    }

    public static AdverseSlippageResult calculateAdverseSlippage(
            Side side,
            CopySlippageAction action,
            BigDecimal expectedPrice,
            BigDecimal executedPrice,
            BigDecimal executedQty
    ) {
        CopySlippageAction safeAction = action == null ? CopySlippageAction.UNKNOWN : action;
        if (side == null) {
            return missing(side, safeAction, expectedPrice, executedPrice, executedQty, AdverseSlippageStatus.SIDE_MISSING);
        }
        if (expectedPrice == null || expectedPrice.compareTo(ZERO) <= 0) {
            return missing(side, safeAction, expectedPrice, executedPrice, executedQty, AdverseSlippageStatus.PRICE_MISSING);
        }
        if (executedPrice == null || executedPrice.compareTo(ZERO) <= 0) {
            return missing(side, safeAction, expectedPrice, executedPrice, executedQty, AdverseSlippageStatus.EXECUTION_PRICE_MISSING);
        }

        BigDecimal rawDiff = executedPrice.subtract(expectedPrice);
        BigDecimal rawBps = rawDiff.multiply(BPS).divide(expectedPrice, SCALE, RoundingMode.HALF_UP);
        BigDecimal adverseFactor = side == Side.BUY ? BigDecimal.ONE : BigDecimal.ONE.negate();
        BigDecimal signedAdverseBps = rawBps.multiply(adverseFactor);
        BigDecimal adverseBps = maxZero(signedAdverseBps);

        if (executedQty == null || executedQty.compareTo(ZERO) <= 0) {
            return new AdverseSlippageResult(
                    side,
                    safeAction,
                    expectedPrice,
                    executedPrice,
                    executedQty,
                    normalize(rawBps),
                    normalize(adverseBps),
                    null,
                    null,
                    AdverseSlippageStatus.QTY_MISSING
            );
        }

        BigDecimal rawUsd = rawDiff.multiply(executedQty);
        BigDecimal adverseUsd = maxZero(rawUsd.multiply(adverseFactor));
        return new AdverseSlippageResult(
                side,
                safeAction,
                expectedPrice,
                executedPrice,
                executedQty,
                normalize(rawBps),
                normalize(adverseBps),
                normalize(rawUsd),
                normalize(adverseUsd),
                AdverseSlippageStatus.OK
        );
    }

    private static AdverseSlippageResult missing(
            Side side,
            CopySlippageAction action,
            BigDecimal expectedPrice,
            BigDecimal executedPrice,
            BigDecimal executedQty,
            AdverseSlippageStatus status
    ) {
        return new AdverseSlippageResult(side, action, expectedPrice, executedPrice, executedQty, null, null, null, null, status);
    }

    private static BigDecimal maxZero(BigDecimal value) {
        return value == null || value.compareTo(ZERO) < 0 ? ZERO : value;
    }

    private static BigDecimal normalize(BigDecimal value) {
        if (value == null) {
            return null;
        }
        BigDecimal normalized = value.setScale(SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
        return normalized.scale() < 0 ? normalized.setScale(0) : normalized;
    }
}
