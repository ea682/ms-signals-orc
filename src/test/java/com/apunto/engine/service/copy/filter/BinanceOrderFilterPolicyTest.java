package com.apunto.engine.service.copy.filter;

import com.apunto.engine.dto.client.BinanceFuturesSymbolFilterDto;
import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinanceOrderFilterPolicyTest {

    private final BinanceOrderFilterPolicy policy = new BinanceOrderFilterPolicy();

    @Test
    void marketLotSizeHasPriorityWhenItDiffersFromLotSize() {
        BinanceMarketOrderRules rules = policy.resolveMarketRules(symbol(
                filter("LOT_SIZE", "0.001", "0.001", "100"),
                filter("MARKET_LOT_SIZE", "0.010", "0.010", "10"),
                notional("5")));

        assertEquals("MARKET_LOT_SIZE", rules.quantityFilterSource());
        assertEquals(new BigDecimal("0.010"), rules.stepSize());
        assertEquals(new BigDecimal("0.010"), rules.minQty());
        assertEquals(new BigDecimal("10"), rules.maxQty());
    }

    @Test
    void newExposureNeverInflatesAnOrderToReachMinimums() {
        BinanceOrderFilterDecision decision = policy.evaluate(
                BinanceOrderIntent.INCREASE, symbol(
                        filter("MARKET_LOT_SIZE", "0.001", "0.001", "10"),
                        notional("5")),
                new BigDecimal("0.0049"), BigDecimal.ZERO, BigDecimal.ZERO,
                new BigDecimal("1000"));

        assertFalse(decision.sendOrder());
        assertEquals(0, new BigDecimal("0.004").compareTo(decision.roundedQty()));
        assertEquals("INCREASE_BELOW_MIN_NOTIONAL_NOOP", decision.reasonCode());
    }

    @Test
    void unknownRulesBlockAllNewExposureFailClosed() {
        BinanceOrderFilterDecision decision = policy.evaluate(
                BinanceOrderIntent.OPEN, null, BigDecimal.ONE, BigDecimal.ZERO,
                BigDecimal.ZERO, BigDecimal.TEN);
        assertFalse(decision.sendOrder());
        assertEquals("BINANCE_SYMBOL_RULES_UNAVAILABLE", decision.reasonCode());
    }

    @Test
    void reduceUsesMinimumOfRequestedOwnedAndActualAndRoundsDown() {
        BinanceOrderFilterDecision decision = policy.evaluate(
                BinanceOrderIntent.REDUCE, symbol(
                        filter("MARKET_LOT_SIZE", "0.01", "0.01", "10"),
                        notional("1")),
                new BigDecimal("0.19"), new BigDecimal("0.12"),
                new BigDecimal("0.105"), new BigDecimal("100"));

        assertTrue(decision.sendOrder());
        assertEquals(0, new BigDecimal("0.10").compareTo(decision.roundedQty()));
        assertEquals("REDUCE_FILTERS_OK", decision.reasonCode());
    }

    @Test
    void closeBelowMarketMinimumDoesNotRoundUpAndRequiresDustReconciliation() {
        BinanceOrderFilterDecision decision = policy.evaluate(
                BinanceOrderIntent.CLOSE, symbol(
                        filter("MARKET_LOT_SIZE", "0.01", "0.01", "10"),
                        notional("1")),
                new BigDecimal("0.009"), new BigDecimal("0.009"),
                new BigDecimal("0.009"), new BigDecimal("100"));

        assertFalse(decision.sendOrder());
        assertTrue(decision.reconciliationRequired());
        assertEquals(0, BigDecimal.ZERO.compareTo(decision.roundedQty()));
        assertEquals("CLOSE_DUST_RECONCILIATION_REQUIRED", decision.reasonCode());
    }

    private static BinanceFuturesSymbolInfoClientDto symbol(BinanceFuturesSymbolFilterDto... filters) {
        BinanceFuturesSymbolInfoClientDto symbol = new BinanceFuturesSymbolInfoClientDto();
        symbol.setSymbol("BTCUSDC");
        symbol.setStatus("TRADING");
        symbol.setQuoteAsset("USDC");
        symbol.setMarginAsset("USDC");
        symbol.setQuantityPrecision(3);
        symbol.setOrderTypes(List.of("MARKET"));
        symbol.setFilters(List.of(filters));
        return symbol;
    }

    private static BinanceFuturesSymbolFilterDto filter(String type, String step, String min, String max) {
        BinanceFuturesSymbolFilterDto filter = new BinanceFuturesSymbolFilterDto();
        filter.setFilterType(type);
        filter.setStepSize(step);
        filter.setMinQty(min);
        filter.setMaxQty(max);
        return filter;
    }

    private static BinanceFuturesSymbolFilterDto notional(String minimum) {
        BinanceFuturesSymbolFilterDto filter = new BinanceFuturesSymbolFilterDto();
        filter.setFilterType("MIN_NOTIONAL");
        filter.setNotional(minimum);
        return filter;
    }
}
