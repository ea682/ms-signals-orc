package com.apunto.engine.service.copy.accounting;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.shared.enums.PositionSide;

import java.math.BigDecimal;
import java.time.Instant;

public record LiveCopyAccountingInput(
        String symbol,
        PositionSide side,
        BigDecimal previousQty,
        BigDecimal resultingQty,
        BigDecimal currentAvgEntryPrice,
        BinanceFuturesOrderClientResponse order,
        BigDecimal fallbackExecutionPrice,
        BigDecimal commissionUsd,
        BigDecimal slippageUsd,
        Instant eventTime
) {
}
