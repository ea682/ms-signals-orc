package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;

import java.math.BigDecimal;

public record CopyDispatchRequest(
        String idempotencyKey,
        CopyDispatchIdentity identity,
        OperationDto operation,
        String walletId,
        String symbol,
        String side,
        String positionSide,
        boolean reduceOnly,
        BigDecimal requestedQty,
        BigDecimal requestedMarginUsd,
        BigDecimal requestedNotionalUsd,
        BigDecimal referencePrice,
        Integer requestedLeverage,
        Integer userMaxConcurrentPositions,
        boolean reservePosition,
        String sourceEventType,
        String requestHash,
        String traceId,
        String notionalBand
) {
    public CopyDispatchRequest(String idempotencyKey,
                               CopyDispatchIdentity identity,
                               OperationDto operation,
                               String walletId,
                               String symbol,
                               String side,
                               String positionSide,
                               boolean reduceOnly,
                               BigDecimal requestedQty,
                               BigDecimal requestedMarginUsd,
                               BigDecimal requestedNotionalUsd,
                               BigDecimal referencePrice,
                               Integer requestedLeverage,
                               Integer userMaxConcurrentPositions,
                               boolean reservePosition,
                               String sourceEventType,
                               String requestHash,
                               String traceId) {
        this(idempotencyKey, identity, operation, walletId, symbol, side, positionSide,
                reduceOnly, requestedQty, requestedMarginUsd, requestedNotionalUsd,
                referencePrice, requestedLeverage, userMaxConcurrentPositions,
                reservePosition, sourceEventType, requestHash, traceId, null);
    }
}
