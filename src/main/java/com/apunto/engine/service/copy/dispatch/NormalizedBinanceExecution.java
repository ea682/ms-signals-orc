package com.apunto.engine.service.copy.dispatch;

import java.math.BigDecimal;

public record NormalizedBinanceExecution(
        boolean accepted,
        CopyExecutionState executionState,
        Long orderId,
        String clientOrderId,
        String status,
        BigDecimal executedQty,
        BigDecimal averagePrice,
        BigDecimal cumulativeQuoteQty,
        AveragePriceStatus averagePriceStatus,
        boolean requiresReconciliation,
        boolean safeToRetrySend
) {
}
