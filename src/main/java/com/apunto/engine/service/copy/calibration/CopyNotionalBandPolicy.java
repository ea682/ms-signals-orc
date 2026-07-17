package com.apunto.engine.service.copy.calibration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class CopyNotionalBandPolicy {

    private final BigDecimal smallMaximumUsd;
    private final BigDecimal mediumMaximumUsd;
    private final BigDecimal largeMaximumUsd;

    public CopyNotionalBandPolicy(
            @Value("${copy.calibration.notional-band.small-max-usd:100}") BigDecimal smallMaximumUsd,
            @Value("${copy.calibration.notional-band.medium-max-usd:1000}") BigDecimal mediumMaximumUsd,
            @Value("${copy.calibration.notional-band.large-max-usd:10000}") BigDecimal largeMaximumUsd
    ) {
        if (!positive(smallMaximumUsd)
                || mediumMaximumUsd == null || mediumMaximumUsd.compareTo(smallMaximumUsd) <= 0
                || largeMaximumUsd == null || largeMaximumUsd.compareTo(mediumMaximumUsd) <= 0) {
            throw new IllegalArgumentException("calibration notional bands must be positive and strictly ascending");
        }
        this.smallMaximumUsd = smallMaximumUsd;
        this.mediumMaximumUsd = mediumMaximumUsd;
        this.largeMaximumUsd = largeMaximumUsd;
    }

    public String band(BigDecimal notionalUsd) {
        if (!positive(notionalUsd)) return null;
        BigDecimal value = notionalUsd.abs();
        if (value.compareTo(smallMaximumUsd) <= 0) return "SMALL";
        if (value.compareTo(mediumMaximumUsd) <= 0) return "MEDIUM";
        if (value.compareTo(largeMaximumUsd) <= 0) return "LARGE";
        return "XLARGE";
    }

    private static boolean positive(BigDecimal value) {
        return value != null && value.signum() > 0;
    }
}
