package com.apunto.copytarget;

import java.math.BigDecimal;
import java.math.RoundingMode;

final class DecimalSupport {
    static final BigDecimal ZERO = BigDecimal.ZERO;
    static final BigDecimal ONE = BigDecimal.ONE;
    static final int SCALE = 18;

    private DecimalSupport() {}

    static BigDecimal normalize(BigDecimal value) {
        if (value == null || value.compareTo(ZERO) == 0) {
            return ZERO;
        }
        return value.stripTrailingZeros();
    }

    static BigDecimal nullable(BigDecimal value) {
        return value == null ? null : normalize(value);
    }

    static BigDecimal nonNegative(BigDecimal value, String field) {
        if (value == null || value.compareTo(ZERO) < 0) {
            throw new IllegalArgumentException(field + " must be non-negative");
        }
        return normalize(value);
    }

    static BigDecimal nonNegativeOrZero(BigDecimal value) {
        if (value == null) {
            return ZERO;
        }
        if (value.compareTo(ZERO) < 0) {
            throw new IllegalArgumentException("value must be non-negative");
        }
        return normalize(value);
    }

    static BigDecimal divideDown(BigDecimal numerator, BigDecimal denominator) {
        if (numerator == null || denominator == null || denominator.compareTo(ZERO) <= 0) {
            return ZERO;
        }
        return normalize(numerator.divide(denominator, SCALE, RoundingMode.DOWN));
    }

    static BigDecimal floorToStep(BigDecimal value, BigDecimal step) {
        if (value == null || value.compareTo(ZERO) <= 0) {
            return ZERO;
        }
        if (step == null || step.compareTo(ZERO) <= 0) {
            return normalize(value);
        }
        BigDecimal units = value.divide(step, 0, RoundingMode.DOWN);
        return normalize(units.multiply(step));
    }

    static BigDecimal ratio(BigDecimal numerator, BigDecimal denominator) {
        return divideDown(numerator, denominator);
    }

    static BigDecimal min(BigDecimal first, BigDecimal second) {
        return normalize(first.min(second));
    }
}
