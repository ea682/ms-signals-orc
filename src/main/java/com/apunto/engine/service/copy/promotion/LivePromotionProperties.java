package com.apunto.engine.service.copy.promotion;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "copy.live-promotion")
public class LivePromotionProperties {

    private boolean enabled = true;
    private int candidateLimit = 100;
    private long minMicroDays = 7;
    private long minMicroOrders = 10;
    private BigDecimal maxErrorRatePct = new BigDecimal("5");
    private boolean requirePositiveNetPnl = false;
    private BigDecimal minNetPnlUsd = BigDecimal.ZERO;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getCandidateLimit() {
        return candidateLimit;
    }

    public void setCandidateLimit(int candidateLimit) {
        this.candidateLimit = candidateLimit;
    }

    public long getMinMicroDays() {
        return minMicroDays;
    }

    public void setMinMicroDays(long minMicroDays) {
        this.minMicroDays = minMicroDays;
    }

    public long getMinMicroOrders() {
        return minMicroOrders;
    }

    public void setMinMicroOrders(long minMicroOrders) {
        this.minMicroOrders = minMicroOrders;
    }

    public BigDecimal getMaxErrorRatePct() {
        return maxErrorRatePct;
    }

    public void setMaxErrorRatePct(BigDecimal maxErrorRatePct) {
        this.maxErrorRatePct = maxErrorRatePct;
    }

    public boolean isRequirePositiveNetPnl() {
        return requirePositiveNetPnl;
    }

    public void setRequirePositiveNetPnl(boolean requirePositiveNetPnl) {
        this.requirePositiveNetPnl = requirePositiveNetPnl;
    }

    public BigDecimal getMinNetPnlUsd() {
        return minNetPnlUsd;
    }

    public void setMinNetPnlUsd(BigDecimal minNetPnlUsd) {
        this.minNetPnlUsd = minNetPnlUsd;
    }
}
