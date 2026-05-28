package com.apunto.engine.service.futures;

import com.apunto.engine.shared.enums.FuturesCapitalAsset;

import java.math.BigDecimal;

public record FuturesBnbConversionDecision(
        boolean shouldConvert,
        String reasonCode,
        FuturesCapitalAsset fromAsset,
        BigDecimal availableBalance,
        BigDecimal bnbBalance,
        BigDecimal bnbValue,
        BigDecimal bnbMinimumValue,
        BigDecimal conversionAmount
) {
}
