package com.apunto.engine.service.copy.accounting;

import java.math.BigDecimal;

public record PositionDeltaClassificationInput(
        String originalEventType,
        String originalDeltaType,
        String previousSide,
        String resultingSide,
        BigDecimal previousQty,
        BigDecimal resultingQty,
        BigDecimal deltaQty,
        BigDecimal executionPrice,
        BigDecimal avgEntryPrice,
        String symbol,
        String walletId,
        String profileKey,
        String flow
) {
}
