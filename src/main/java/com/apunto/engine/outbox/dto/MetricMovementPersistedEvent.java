package com.apunto.engine.outbox.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record MetricMovementPersistedEvent(
        String eventVersion,
        String movementKey,
        String movementHash,
        String positionKey,
        String wallet,
        String symbol,
        String side,
        String eventType,
        String deltaType,
        String status,
        OffsetDateTime eventTime,
        OffsetDateTime ingestedAt,
        BigDecimal previousSizeQty,
        BigDecimal resultingSizeQty,
        BigDecimal deltaSizeQty,
        BigDecimal sizeQty,
        BigDecimal notionalUsd,
        BigDecimal marginUsedUsd,
        BigDecimal entryPrice,
        BigDecimal exitPrice,
        BigDecimal markPrice,
        BigDecimal realizedPnlUsd,
        BigDecimal leverage,
        BigDecimal rawNotionalUsd,
        BigDecimal positionNotionalUsd,
        BigDecimal closedNotionalUsd,
        BigDecimal closedMarginUsedUsd,
        BigDecimal effectiveCloseQty,
        BigDecimal effectiveEntryPrice,
        BigDecimal effectiveExitPrice,
        BigDecimal effectiveRealizedPnlUsd,
        String normalizationStatus,
        String normalizationReason,
        Integer copySubmittedTasks,
        Integer copyBusinessSkipped,
        Integer copyFallbackJobs,
        Boolean copyFallbackUsed
) {
}
