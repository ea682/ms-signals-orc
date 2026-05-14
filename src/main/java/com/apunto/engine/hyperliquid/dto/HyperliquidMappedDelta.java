package com.apunto.engine.hyperliquid.dto;

import com.apunto.engine.events.OperacionEvent;

public record HyperliquidMappedDelta(
        String idempotencyKey,
        String positionKey,
        String wallet,
        String symbol,
        String side,
        String deltaType,
        OperacionEvent event,
        HyperliquidDeltaRequest request
) {
}
