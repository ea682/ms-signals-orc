package com.apunto.engine.shared.metric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricStrategyIdentityTest {

    @Test
    void canonicalizesLegacyDefaultsWithoutCollapsingIndependentScopes() {
        assertEquals(
                "0xabc|MOVEMENT_ALL|ALL|ALL",
                MetricStrategyIdentity.canonicalKey(" 0xAbC ", "movement-all", "strategy", "MOVEMENT_ALL")
        );
        assertEquals(
                "0xabc|LONG_ONLY|DIRECTION|LONG",
                MetricStrategyIdentity.canonicalKey("0xABC", "long_only", "default", "LONG_ONLY")
        );
        assertEquals(
                "0xabc|SHORT_ONLY|DIRECTION|SHORT",
                MetricStrategyIdentity.canonicalKey("0xABC", "short_only", null, null)
        );
        assertEquals(
                "0xabc|SYMBOL_SPECIALIST|SYMBOL|BTCUSDT",
                MetricStrategyIdentity.canonicalKey("0xABC", "symbol_specialist", "symbol", "btcusdt")
        );
        assertEquals(
                "0xabc|SYMBOL_SPECIALIST|SYMBOL|ETHUSDT",
                MetricStrategyIdentity.canonicalKey("0xABC", "symbol_specialist", "symbol", "ethusdt")
        );
    }

    @Test
    void keepsExplicitStrategyScopesCanonical() {
        assertEquals("DYNAMIC_SYMBOL_SET", MetricStrategyIdentity.scopeType("dynamic_symbol_set", "TOP_SYMBOLS_ONLY"));
        assertEquals("BTCUSDT,ETHUSDT", MetricStrategyIdentity.scopeValue("btcusdt,ethusdt", "TOP_SYMBOLS_ONLY"));
        assertEquals("LEVERAGE_RANGE", MetricStrategyIdentity.scopeType("strategy", "LOW_LEVERAGE_ONLY"));
    }
}
