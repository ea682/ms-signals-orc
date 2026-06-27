package com.apunto.engine.service.copy.accounting;

import java.math.BigDecimal;

public record CopyAccountingResult(
        boolean accepted,
        PositionDeltaType deltaType,
        BigDecimal previousQty,
        BigDecimal resultingQty,
        BigDecimal deltaAddedQty,
        BigDecimal deltaClosedQty,
        BigDecimal deltaNotionalUsd,
        BigDecimal previousAvgEntryPrice,
        BigDecimal newAvgEntryPrice,
        BigDecimal grossRealizedPnlUsd,
        BigDecimal netRealizedPnlUsd,
        BigDecimal feeUsd,
        BigDecimal slippageUsd,
        boolean positionRemainsOpen,
        boolean positionClosed,
        String reasonCode
) {
    public static CopyAccountingResult rejected(
            PositionDeltaType deltaType,
            BigDecimal previousQty,
            BigDecimal resultingQty,
            BigDecimal previousAvgEntryPrice,
            BigDecimal feeUsd,
            BigDecimal slippageUsd,
            String reasonCode
    ) {
        return new CopyAccountingResult(
                false,
                deltaType,
                previousQty,
                resultingQty,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                previousAvgEntryPrice,
                previousAvgEntryPrice,
                null,
                null,
                feeUsd == null ? BigDecimal.ZERO : feeUsd,
                slippageUsd == null ? BigDecimal.ZERO : slippageUsd,
                false,
                false,
                reasonCode
        );
    }
}
