package com.apunto.engine.service.copy.accounting;

import java.math.BigDecimal;

public record PositionDeltaClassification(
        PositionDeltaType computedDeltaType,
        boolean shouldRealizePnl,
        BigDecimal qtyToRealize,
        String reasonCode,
        String warningCode
) {
    public boolean corrected() {
        return "EVENT_TYPE_CONTRADICTS_POSITION_MATH".equals(warningCode);
    }
}
