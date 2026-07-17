package com.apunto.copytarget;

import java.math.BigDecimal;
import java.util.Objects;

public record LiquiditySimulationRequest(
        OrderBookSnapshot orderBook,
        SourceSide side,
        BigDecimal requestedNotionalUsd,
        LiquiditySimulationAssumptions assumptions,
        String modelVersion
) {
    public LiquiditySimulationRequest {
        orderBook = Objects.requireNonNull(orderBook, "orderBook");
        side = Objects.requireNonNull(side, "side");
        requestedNotionalUsd = DecimalSupport.nonNegative(requestedNotionalUsd, "requestedNotionalUsd");
        assumptions = Objects.requireNonNull(assumptions, "assumptions");
        Objects.requireNonNull(modelVersion, "modelVersion");
        modelVersion = modelVersion.trim();
        if (requestedNotionalUsd.signum() == 0 || modelVersion.isEmpty()) {
            throw new IllegalArgumentException("requestedNotionalUsd and modelVersion must be positive/non-blank");
        }
    }
}
