package com.apunto.engine.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;

import java.math.BigDecimal;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record HyperliquidDeltaRequest(
        String eventId,
        String idempotencyKey,
        String eventType,
        @NotBlank(message = "deltaType is required")
        String deltaType,
        String platform,
        @NotBlank(message = "wallet is required")
        String wallet,
        String accountId,
        @NotBlank(message = "symbol is required")
        String symbol,
        @NotBlank(message = "side is required")
        String side,
        String status,
        BigDecimal sizeQty,
        BigDecimal signedSizeQty,
        BigDecimal notionalUsd,
        BigDecimal marginUsedUsd,
        BigDecimal entryPrice,
        BigDecimal markPrice,
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
        Long sourceTs,
        Instant detectedAt,
        Instant publishedAt,
        Long walletVersion,
        Long snapshotVersion,
        String externalId,
        String rawReference,
        Boolean estimated
) {
}
