package com.apunto.engine.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

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
        Boolean estimated,
        String economicEventKind,
        Integer economicEventVersion,
        String sourceEventId,
        Long sourceSequence,
        BigDecimal sourceFeeUsd,
        BigDecimal fundingPnlUsd,
        String executionPriceBasis,
        String notionalBasis,
        List<String> lifecycleQualityFlags,
        Boolean sourceEstimated
) {
    public HyperliquidDeltaRequest(
            String eventId,
            String idempotencyKey,
            String eventType,
            String deltaType,
            String platform,
            String wallet,
            String accountId,
            String symbol,
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
        this(
                eventId, idempotencyKey, eventType, deltaType, platform, wallet, accountId, symbol,
                side, status, sizeQty, signedSizeQty, notionalUsd, marginUsedUsd, entryPrice, markPrice,
                leverage, rawNotionalUsd, positionNotionalUsd, closedNotionalUsd, closedMarginUsedUsd,
                effectiveCloseQty, effectiveEntryPrice, effectiveExitPrice, effectiveRealizedPnlUsd,
                normalizationStatus, normalizationReason, sourceTs, detectedAt, publishedAt, walletVersion,
                snapshotVersion, externalId, rawReference, estimated, null, null, null, null, null, null,
                null, null, List.of(), null
        );
    }

    public HyperliquidDeltaRequest {
        lifecycleQualityFlags = lifecycleQualityFlags == null ? List.of() : List.copyOf(lifecycleQualityFlags);
    }
}
