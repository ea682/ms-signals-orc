package com.apunto.engine.service.copy;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Centraliza la matemática de sizing proporcional para copy trading.
 *
 * Regla de negocio:
 * - El margen de la copia debe representar la misma fracción que la posición origen
 *   sobre el capital de referencia de la wallet original.
 * - La fracción se limita por operación para no copiar exposiciones extremas.
 */
@Component
public class ProportionalCopySizingCalculator {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final int SCALE = 18;

    public BigDecimal computeFraction(BigDecimal sourceMargin,
                                      BigDecimal capitalReference,
                                      BigDecimal maxFractionPerTrade) {
        if (sourceMargin == null || capitalReference == null || sourceMargin.compareTo(ZERO) <= 0 || capitalReference.compareTo(ZERO) <= 0) {
            return ZERO;
        }

        BigDecimal fraction = sourceMargin.divide(capitalReference, SCALE, RoundingMode.HALF_UP);

        if (fraction.compareTo(BigDecimal.ONE) > 0) {
            fraction = BigDecimal.ONE;
        }

        if (maxFractionPerTrade != null && maxFractionPerTrade.compareTo(ZERO) > 0) {
            fraction = fraction.min(maxFractionPerTrade);
        }

        if (fraction.compareTo(ZERO) < 0) {
            return ZERO;
        }

        return fraction;
    }

    public BigDecimal computeTargetMargin(BigDecimal walletBudget,
                                          BigDecimal sourceMargin,
                                          BigDecimal capitalReference,
                                          BigDecimal maxFractionPerTrade) {
        if (walletBudget == null || walletBudget.compareTo(ZERO) <= 0) {
            return ZERO;
        }

        BigDecimal fraction = computeFraction(sourceMargin, capitalReference, maxFractionPerTrade);
        if (fraction.compareTo(ZERO) <= 0) {
            return ZERO;
        }

        return walletBudget.multiply(fraction);
    }
}
