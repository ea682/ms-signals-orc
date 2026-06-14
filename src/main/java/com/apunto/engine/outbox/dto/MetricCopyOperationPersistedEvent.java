package com.apunto.engine.outbox.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public record MetricCopyOperationPersistedEvent(
        String eventVersion,
        UUID idEvent,
        UUID idOperation,
        String idOrderOrigin,
        String idUser,
        String wallet,
        String symbol,
        String typeOperation,
        String eventType,
        String copyIntent,
        String binanceOrderId,
        String clientOrderId,
        String side,
        String positionSide,
        BigDecimal qtyRequested,
        BigDecimal qtyExecuted,
        BigDecimal price,
        BigDecimal notionalUsd,
        BigDecimal previousQty,
        BigDecimal resultingQty,
        BigDecimal realizedPnlUsd,
        BigDecimal feeUsd,
        String traceId,
        String source,
        String reasonCode,
        OffsetDateTime eventTime,
        OffsetDateTime dateCreation
) {
}
