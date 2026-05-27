package com.apunto.engine.shared.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopySymbolIdentityTest {

    @Test
    void sameBaseAssetMatchesHyperliquidUsdWithBinanceStableQuotes() {
        assertTrue(CopySymbolIdentity.sameBaseAsset("BTCUSD", "BTCUSDT"));
        assertTrue(CopySymbolIdentity.sameBaseAsset("BTCUSD", "BTCUSDC"));
        assertTrue(CopySymbolIdentity.sameBaseAsset("ETH-USDC", "ETH/USDT"));
    }

    @Test
    void sameBaseAssetDoesNotMatchDifferentAssets() {
        assertFalse(CopySymbolIdentity.sameBaseAsset("BTCUSD", "ETHUSDT"));
        assertFalse(CopySymbolIdentity.sameBaseAsset(null, "ETHUSDT"));
        assertFalse(CopySymbolIdentity.sameBaseAsset("", "ETHUSDT"));
    }

    @Test
    void baseAssetStripsVersionAndLeadingMultiplierCandidates() {
        assertEquals("XAUT", CopySymbolIdentity.primaryBaseAsset("XAUTV2USD"));
        assertTrue(CopySymbolIdentity.baseAssetCandidates("1000PEPEUSDT").contains("PEPE"));
        assertTrue(CopySymbolIdentity.sameBaseAsset("PEPEUSD", "1000PEPEUSDT"));
    }
}
