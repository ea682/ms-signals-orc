package com.apunto.engine.hyperliquid.dto;

import java.time.Instant;

public record HyperliquidDeltaAcceptedResponse(
        String status,
        String idempotencyKey,
        String positionKey,
        String wallet,
        String symbol,
        String side,
        String deltaType,
        boolean duplicate,
        int queueDepth,
        Instant acceptedAt
) {
    public static HyperliquidDeltaAcceptedResponse accepted(
            String idempotencyKey,
            String positionKey,
            String wallet,
            String symbol,
            String side,
            String deltaType,
            boolean duplicate,
            int queueDepth
    ) {
        return new HyperliquidDeltaAcceptedResponse(
                "ACCEPTED",
                idempotencyKey,
                positionKey,
                wallet,
                symbol,
                side,
                deltaType,
                duplicate,
                queueDepth,
                Instant.now()
        );
    }
}
