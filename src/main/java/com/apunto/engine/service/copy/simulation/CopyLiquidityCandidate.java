package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.SourceSide;

import java.math.BigDecimal;
import java.util.UUID;

public record CopyLiquidityCandidate(
        UUID id,
        long capitalScenarioId,
        String symbol,
        SourceSide side,
        BigDecimal requestedNotionalUsd,
        int attempt
) {
}
