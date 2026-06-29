package com.apunto.engine.service.copy.slippage;

import com.apunto.engine.shared.enums.Side;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdverseSlippageCalculatorTest {

    @Test
    void buyOrderTreatsHigherExecutionPriceAsAdverse() {
        AdverseSlippageResult result = AdverseSlippageCalculator.calculateAdverseSlippage(
                Side.BUY,
                CopySlippageAction.OPEN,
                bd("100"),
                bd("101"),
                bd("2")
        );

        assertTrue(result.available());
        assertEquals(bd("100"), result.rawSlippageBps());
        assertEquals(bd("100"), result.adverseSlippageBps());
        assertEquals(bd("2"), result.rawSlippageUsd());
        assertEquals(bd("2"), result.adverseSlippageUsd());
    }

    @Test
    void sellOrderTreatsLowerExecutionPriceAsAdverse() {
        AdverseSlippageResult result = AdverseSlippageCalculator.calculateAdverseSlippage(
                Side.SELL,
                CopySlippageAction.CLOSE,
                bd("100"),
                bd("99"),
                bd("3")
        );

        assertTrue(result.available());
        assertEquals(bd("-100"), result.rawSlippageBps());
        assertEquals(bd("100"), result.adverseSlippageBps());
        assertEquals(bd("-3"), result.rawSlippageUsd());
        assertEquals(bd("3"), result.adverseSlippageUsd());
    }

    @Test
    void favorableExecutionHasZeroAdverseSlippage() {
        AdverseSlippageResult result = AdverseSlippageCalculator.calculateAdverseSlippage(
                Side.BUY,
                CopySlippageAction.INCREASE,
                bd("100"),
                bd("99.5"),
                bd("4")
        );

        assertTrue(result.available());
        assertEquals(bd("-50"), result.rawSlippageBps());
        assertEquals(BigDecimal.ZERO, result.adverseSlippageBps());
        assertEquals(bd("-2"), result.rawSlippageUsd());
        assertEquals(BigDecimal.ZERO, result.adverseSlippageUsd());
    }

    @Test
    void missingPricesAreExplicitlyNotAvailable() {
        AdverseSlippageResult result = AdverseSlippageCalculator.calculateAdverseSlippage(
                Side.BUY,
                CopySlippageAction.OPEN,
                null,
                bd("100"),
                bd("1")
        );

        assertEquals(AdverseSlippageStatus.PRICE_MISSING, result.status());
        assertEquals(null, result.adverseSlippageBps());
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
