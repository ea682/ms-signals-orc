package com.apunto.engine.service.impl;

import org.junit.jupiter.api.Test;

import com.apunto.engine.dto.client.BinanceFuturesSymbolInfoClientDto;
import com.apunto.engine.service.impl.BinanceEngineServiceImpl.StrategyMarginBudget;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import com.apunto.engine.shared.exception.SkipExecutionException;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BinanceEngineServiceCapitalPolicyTest {

    @Test
    void microLiveBudgetUsesStrategyCapWithoutMultiplyingCapitalShare() {
        BigDecimal budget = BinanceEngineServiceImpl.resolveStrategyMarginBudget(
                1_000,
                0.25,
                "MICRO_LIVE"
        );

        assertEquals(new BigDecimal("100.000000000000"), budget);
    }

    @Test
    void microLiveBudgetDoesNotRequirePositiveCapitalShare() {
        BigDecimal budget = BinanceEngineServiceImpl.resolveStrategyMarginBudget(
                1_000,
                0.0,
                "MICRO_LIVE"
        );

        assertEquals(new BigDecimal("100.000000000000"), budget);
    }

    @Test
    void microLiveTargetDoesNotShrinkWhenUserCapitalIsBelowTarget() {
        BigDecimal budget = BinanceEngineServiceImpl.resolveStrategyMarginBudget(
                60,
                0.25,
                "MICRO_LIVE"
        );

        assertEquals(new BigDecimal("100.000000000000"), budget);
    }

    @Test
    void microLiveBudgetPreservesUsdcCapitalCurrency() {
        StrategyMarginBudget budget = BinanceEngineServiceImpl.resolveStrategyMarginBudget(
                1_000,
                0.25,
                "MICRO_LIVE",
                "USDC"
        );

        assertEquals(new BigDecimal("100.000000000000"), budget.amount());
        assertEquals("USDC", budget.capitalCurrency());
        assertEquals("USDC", budget.quoteAsset());
        assertEquals("USDC", budget.collateralAsset());
    }

    @Test
    void liveBudgetStillUsesCapitalShare() {
        BigDecimal budget = BinanceEngineServiceImpl.resolveStrategyMarginBudget(
                1_000,
                0.25,
                "LIVE"
        );

        assertEquals(new BigDecimal("250.000000000000"), budget);
    }

    @Test
    void usdcAllocationCannotUseUsdtSymbolContract() {
        BinanceFuturesSymbolInfoClientDto symbol = new BinanceFuturesSymbolInfoClientDto();
        symbol.setSymbol("BTCUSDT");
        symbol.setQuoteAsset("USDT");
        symbol.setMarginAsset("USDT");

        SkipExecutionException ex = assertThrows(
                SkipExecutionException.class,
                () -> BinanceEngineServiceImpl.validateSymbolCapitalAsset(symbol, FuturesCapitalAsset.USDC)
        );

        assertEquals("PRE_FLIGHT_BLOCKED_QUOTE_ASSET", ex.getReasonCode());
    }

    @Test
    void usdcAllocationAcceptsUsdcSymbolContract() {
        BinanceFuturesSymbolInfoClientDto symbol = new BinanceFuturesSymbolInfoClientDto();
        symbol.setSymbol("BTCUSDC");
        symbol.setQuoteAsset("USDC");
        symbol.setMarginAsset("USDC");

        BinanceEngineServiceImpl.validateSymbolCapitalAsset(symbol, FuturesCapitalAsset.USDC);
    }
}
