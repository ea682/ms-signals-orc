package com.apunto.engine.service.copy.accounting;

import java.math.BigDecimal;

public final class PositionDeltaClassifier {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    public PositionDeltaType classify(BigDecimal previousQty, BigDecimal resultingQty) {
        BigDecimal previous = nonNegative(previousQty);
        BigDecimal resulting = nonNegative(resultingQty);
        int resultingVsZero = resulting.compareTo(ZERO);
        if (previous.compareTo(ZERO) == 0 && resultingVsZero > 0) {
            return PositionDeltaType.OPEN;
        }
        int compare = resulting.compareTo(previous);
        if (compare > 0) {
            return PositionDeltaType.INCREASE;
        }
        if (compare < 0 && resultingVsZero > 0) {
            return PositionDeltaType.REDUCE;
        }
        if (resultingVsZero == 0 && previous.compareTo(ZERO) > 0) {
            return PositionDeltaType.CLOSE_FULL;
        }
        return PositionDeltaType.NOOP;
    }

    private BigDecimal nonNegative(BigDecimal value) {
        return value == null || value.compareTo(ZERO) <= 0 ? ZERO : value;
    }
}
