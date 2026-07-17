package com.apunto.engine.service.copy.certification;

import java.math.BigDecimal;
import java.util.UUID;

final class TestCertificationFixtures {

    private TestCertificationFixtures() {
    }

    static LiveCertificationIdentity identity(BigDecimal bandMin, BigDecimal bandMax, BigDecimal leverage) {
        return new LiveCertificationIdentity(
                "0xabc", "MOVEMENT_ALL", "copy-strategy-v3", "ALL", "ALL",
                bandMin, bandMax, leverage, "BINANCE", "USDC",
                "proportional-portfolio-v3", "binance-symbol-map-v3", "binance-fee-v3",
                "binance-funding-v3", "binance-slippage-v3", "order-book-liquidity-v3");
    }

    static LiveEntryAuthorizationRequest request(UUID userId, Long allocationId,
                                                 BigDecimal capital, BigDecimal leverage) {
        return new LiveEntryAuthorizationRequest(
                userId, allocationId, "0xabc", "MOVEMENT_ALL", "copy-strategy-v3", "ALL", "ALL",
                capital, leverage, "BINANCE", "USDC", "proportional-portfolio-v3",
                "binance-symbol-map-v3", "binance-fee-v3", "binance-funding-v3",
                "binance-slippage-v3", "order-book-liquidity-v3");
    }
}
