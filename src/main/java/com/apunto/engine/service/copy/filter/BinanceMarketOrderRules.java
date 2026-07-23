package com.apunto.engine.service.copy.filter;

import java.math.BigDecimal;

public record BinanceMarketOrderRules(
        String symbol,
        boolean trading,
        boolean marketOrderSupported,
        String quoteAsset,
        String marginAsset,
        String quantityFilterSource,
        BigDecimal stepSize,
        BigDecimal minQty,
        BigDecimal maxQty,
        int quantityPrecision,
        BigDecimal minNotional,
        BigDecimal maxNotional
) {
}
