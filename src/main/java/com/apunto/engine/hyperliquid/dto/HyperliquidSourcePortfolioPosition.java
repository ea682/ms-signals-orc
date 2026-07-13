package com.apunto.engine.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public record HyperliquidSourcePortfolioPosition(
        String sourcePositionKey,
        String sourceSymbol,
        String sourceSide,
        BigDecimal sourcePositionQuantity,
        BigDecimal sourcePositionNotionalUsd,
        BigDecimal sourceMarkPrice,
        BigDecimal sourceEntryPrice,
        BigDecimal sourceLeverage,
        Long sourceSnapshotVersion,
        Long sourceTs,
        Boolean estimated
) { }
