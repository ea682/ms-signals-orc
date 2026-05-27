package com.apunto.engine.service.futures;

import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
public class FuturesBnbConversionPolicy {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final int AMOUNT_SCALE = 8;

    public FuturesBnbConversionDecision decide(FuturesCapitalAsset asset,
                                               BigDecimal availableBalance,
                                               BigDecimal bnbBalance,
                                               BigDecimal bnbPrice,
                                               BigDecimal minimumAvailableToConvert,
                                               BigDecimal minimumBnbRatio,
                                               BigDecimal conversionRatio) {
        BigDecimal safeAvailable = positive(availableBalance);
        BigDecimal safeBnbBalance = positive(bnbBalance);
        BigDecimal safeBnbPrice = positive(bnbPrice);
        BigDecimal bnbValue = safeBnbBalance.multiply(safeBnbPrice);
        BigDecimal minimumBnbValue = safeAvailable.multiply(positive(minimumBnbRatio));

        if (safeAvailable.compareTo(positive(minimumAvailableToConvert)) < 0) {
            return new FuturesBnbConversionDecision(false, "available_balance_below_minimum", asset, safeAvailable,
                    safeBnbBalance, bnbValue, minimumBnbValue, ZERO);
        }

        if (safeBnbPrice.compareTo(ZERO) <= 0) {
            return new FuturesBnbConversionDecision(false, "bnb_price_missing", asset, safeAvailable,
                    safeBnbBalance, bnbValue, minimumBnbValue, ZERO);
        }

        if (bnbValue.compareTo(minimumBnbValue) >= 0) {
            return new FuturesBnbConversionDecision(false, "bnb_balance_is_enough", asset, safeAvailable,
                    safeBnbBalance, bnbValue, minimumBnbValue, ZERO);
        }

        BigDecimal amount = safeAvailable
                .multiply(positive(conversionRatio))
                .setScale(AMOUNT_SCALE, RoundingMode.DOWN)
                .stripTrailingZeros();
        if (amount.compareTo(ZERO) <= 0) {
            return new FuturesBnbConversionDecision(false, "conversion_amount_zero", asset, safeAvailable,
                    safeBnbBalance, bnbValue, minimumBnbValue, ZERO);
        }

        return new FuturesBnbConversionDecision(true, "bnb_balance_below_minimum", asset, safeAvailable,
                safeBnbBalance, bnbValue, minimumBnbValue, amount);
    }

    private BigDecimal positive(BigDecimal value) {
        if (value == null || value.compareTo(ZERO) <= 0) {
            return ZERO;
        }
        return value;
    }
}
