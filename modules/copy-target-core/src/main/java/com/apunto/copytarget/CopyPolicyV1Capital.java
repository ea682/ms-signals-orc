package com.apunto.copytarget;

import java.math.BigDecimal;

/** Capital envelope shared by every Copy Policy V1 execution mode. */
public final class CopyPolicyV1Capital {

    public static final BigDecimal INITIAL_CAPITAL_USD = new BigDecimal("100");
    public static final BigDecimal RESERVE_REMAINDER_RATIO = new BigDecimal("0.90");
    public static final BigDecimal DEPLOYMENT_RATIO = new BigDecimal("0.90");

    private CopyPolicyV1Capital() {
    }

    public static BigDecimal defensiveSizingEquity(BigDecimal currentMtmEquityUsd) {
        return defensiveSizingEquity(currentMtmEquityUsd, INITIAL_CAPITAL_USD);
    }

    public static BigDecimal defensiveSizingEquity(BigDecimal currentMtmEquityUsd,
                                                    BigDecimal initialAccountEquityUsd) {
        if (currentMtmEquityUsd == null) {
            throw new IllegalArgumentException("currentMtmEquityUsd must not be null");
        }
        if (initialAccountEquityUsd == null || initialAccountEquityUsd.signum() <= 0) {
            throw new IllegalArgumentException("initialAccountEquityUsd must be positive");
        }
        BigDecimal nonNegativeEquity = currentMtmEquityUsd.max(BigDecimal.ZERO);
        return DecimalSupport.normalize(nonNegativeEquity.min(initialAccountEquityUsd));
    }

    public static BigDecimal authorizedSizingCapital(BigDecimal currentMtmEquityUsd) {
        return authorizedSizingCapital(currentMtmEquityUsd, INITIAL_CAPITAL_USD);
    }

    public static BigDecimal authorizedSizingCapital(BigDecimal currentMtmEquityUsd,
                                                     BigDecimal initialAccountEquityUsd) {
        return DecimalSupport.normalize(defensiveSizingEquity(currentMtmEquityUsd, initialAccountEquityUsd)
                .multiply(RESERVE_REMAINDER_RATIO)
                .multiply(DEPLOYMENT_RATIO));
    }
}
