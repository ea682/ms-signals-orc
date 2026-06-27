package com.apunto.engine.service.copy.accounting;

import com.apunto.engine.shared.enums.PositionSide;

import java.math.BigDecimal;
import java.time.Instant;

public record CopyAccountingInput(
        String symbol,
        PositionSide side,
        BigDecimal previousQty,
        BigDecimal resultingQty,
        BigDecimal currentAvgEntryPrice,
        BigDecimal executionPrice,
        BigDecimal feeUsd,
        BigDecimal slippageUsd,
        Instant eventTime,
        AccountingMode mode
) {
}
