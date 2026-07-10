package com.apunto.engine.service.copy.coverage;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

@Validated
@ConfigurationProperties(prefix = "copy.promotion.coverage")
public class ShadowCoverageWindowProperties implements InitializingBean {

    private boolean rollingEnabled = true;

    @NotNull
    private ShadowCoverageMode mode = ShadowCoverageMode.ROLLING;

    @Min(1)
    private int windowDays = 14;

    @Min(1)
    private int maxEvents = 500;

    @Min(1)
    private int minEvaluableEvents = 100;

    @NotNull
    @DecimalMin("0")
    @DecimalMax("100")
    private BigDecimal minimumPercent = new BigDecimal("95");

    public ShadowCoverageMode effectiveMode() {
        return rollingEnabled ? mode : ShadowCoverageMode.LEGACY;
    }

    public void validate() {
        if (windowDays <= 0) {
            throw new IllegalArgumentException("copy.promotion.coverage.window-days must be greater than zero");
        }
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("copy.promotion.coverage.max-events must be greater than zero");
        }
        if (minEvaluableEvents <= 0) {
            throw new IllegalArgumentException("copy.promotion.coverage.min-evaluable-events must be greater than zero");
        }
        if (minEvaluableEvents > maxEvents) {
            throw new IllegalArgumentException("copy.promotion.coverage.min-evaluable-events cannot exceed max-events");
        }
        if (minimumPercent == null
                || minimumPercent.compareTo(BigDecimal.ZERO) < 0
                || minimumPercent.compareTo(new BigDecimal("100")) > 0) {
            throw new IllegalArgumentException("copy.promotion.coverage.minimum-percent must be between 0 and 100");
        }
        if (mode == null) {
            throw new IllegalArgumentException("copy.promotion.coverage.mode is required");
        }
    }

    @Override
    public void afterPropertiesSet() {
        validate();
    }

    public boolean isRollingEnabled() {
        return rollingEnabled;
    }

    public void setRollingEnabled(boolean rollingEnabled) {
        this.rollingEnabled = rollingEnabled;
    }

    public ShadowCoverageMode getMode() {
        return mode;
    }

    public void setMode(ShadowCoverageMode mode) {
        this.mode = mode;
    }

    public int getWindowDays() {
        return windowDays;
    }

    public void setWindowDays(int windowDays) {
        this.windowDays = windowDays;
    }

    public int getMaxEvents() {
        return maxEvents;
    }

    public void setMaxEvents(int maxEvents) {
        this.maxEvents = maxEvents;
    }

    public int getMinEvaluableEvents() {
        return minEvaluableEvents;
    }

    public void setMinEvaluableEvents(int minEvaluableEvents) {
        this.minEvaluableEvents = minEvaluableEvents;
    }

    public BigDecimal getMinimumPercent() {
        return minimumPercent;
    }

    public void setMinimumPercent(BigDecimal minimumPercent) {
        this.minimumPercent = minimumPercent;
    }
}
