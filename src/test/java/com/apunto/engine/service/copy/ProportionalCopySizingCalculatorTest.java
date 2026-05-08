package com.apunto.engine.service.copy;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProportionalCopySizingCalculatorTest {

    private final ProportionalCopySizingCalculator calculator = new ProportionalCopySizingCalculator();

    @Test
    void computesEthLikeFractionWithoutCap() {
        BigDecimal fraction = calculator.computeFraction(
                new BigDecimal("36756.80"),
                new BigDecimal("376230.39"),
                new BigDecimal("0.10")
        );

        assertEquals(new BigDecimal("0.097697583653462975"), fraction);
    }

    @Test
    void capsSolLikeOversizedPositionAtTenPercent() {
        BigDecimal fraction = calculator.computeFraction(
                new BigDecimal("191731.05"),
                new BigDecimal("376230.39"),
                new BigDecimal("0.10")
        );

        assertEquals(new BigDecimal("0.10"), fraction);
    }

    @Test
    void computesTargetMarginFromWalletBudget() {
        BigDecimal targetMargin = calculator.computeTargetMargin(
                new BigDecimal("134.99985"),
                new BigDecimal("191731.05"),
                new BigDecimal("376230.39"),
                new BigDecimal("0.10")
        );

        assertEquals(new BigDecimal("13.4999850"), targetMargin);
    }

    @Test
    void returnsZeroForInvalidInputs() {
        assertEquals(BigDecimal.ZERO, calculator.computeFraction(null, new BigDecimal("100"), new BigDecimal("0.10")));
        assertEquals(BigDecimal.ZERO, calculator.computeFraction(new BigDecimal("10"), BigDecimal.ZERO, new BigDecimal("0.10")));
        assertEquals(BigDecimal.ZERO, calculator.computeTargetMargin(BigDecimal.ZERO, new BigDecimal("10"), new BigDecimal("100"), new BigDecimal("0.10")));
    }
}
