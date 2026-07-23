package com.apunto.engine.service.copy.promotion;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "copy.live-promotion")
public class LivePromotionProperties {

    private boolean enabled = false;
    private boolean manualCertificationRequired = true;
    private int candidateLimit = 100;
    private long minMicroDays = 7;
    private long minMicroOrders = 10;
    private long minSubmittedOrders = 10;
    private long minAcknowledgedOrders = 10;
    private long minFilledOrders = 10;
    private long minClosedOperations = 3;
    private long maxDispatchErrors = 0;
    private long maxReconciliationPending = 0;
    private long maxDuplicateCount = 0;
    private long maxUnresolvedAmbiguousTimeouts = 0;
    private BigDecimal maxDrawdownUsd = new BigDecimal("20");
    private BigDecimal maxErrorRatePct = new BigDecimal("5");
    private boolean requirePositiveNetPnl = false;
    private BigDecimal minNetPnlUsd = BigDecimal.ZERO;
    private boolean requireFullDecisionForLive = true;
    private int copyDecisionMinHistoryDays = 30;
    private int copyDecisionSimulationLookbackDays = 60;
    private int copyDecisionMaxFactsPerUnit = 50000;
    private int copyDecisionTimeoutMs = 55000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isManualCertificationRequired() {
        return manualCertificationRequired;
    }

    public void setManualCertificationRequired(boolean manualCertificationRequired) {
        this.manualCertificationRequired = manualCertificationRequired;
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

    public long getMinSubmittedOrders() { return minSubmittedOrders; }
    public void setMinSubmittedOrders(long value) { this.minSubmittedOrders = value; }
    public long getMinAcknowledgedOrders() { return minAcknowledgedOrders; }
    public void setMinAcknowledgedOrders(long value) { this.minAcknowledgedOrders = value; }
    public long getMinFilledOrders() { return minFilledOrders; }
    public void setMinFilledOrders(long value) { this.minFilledOrders = value; }
    public long getMinClosedOperations() { return minClosedOperations; }
    public void setMinClosedOperations(long value) { this.minClosedOperations = value; }
    public long getMaxDispatchErrors() { return maxDispatchErrors; }
    public void setMaxDispatchErrors(long value) { this.maxDispatchErrors = value; }
    public long getMaxReconciliationPending() { return maxReconciliationPending; }
    public void setMaxReconciliationPending(long value) { this.maxReconciliationPending = value; }
    public long getMaxDuplicateCount() { return maxDuplicateCount; }
    public void setMaxDuplicateCount(long value) { this.maxDuplicateCount = value; }
    public long getMaxUnresolvedAmbiguousTimeouts() { return maxUnresolvedAmbiguousTimeouts; }
    public void setMaxUnresolvedAmbiguousTimeouts(long value) { this.maxUnresolvedAmbiguousTimeouts = value; }
    public BigDecimal getMaxDrawdownUsd() { return maxDrawdownUsd; }
    public void setMaxDrawdownUsd(BigDecimal value) { this.maxDrawdownUsd = value; }

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

    public boolean isRequireFullDecisionForLive() {
        return requireFullDecisionForLive;
    }

    public void setRequireFullDecisionForLive(boolean requireFullDecisionForLive) {
        this.requireFullDecisionForLive = requireFullDecisionForLive;
    }

    public int getCopyDecisionMinHistoryDays() {
        return copyDecisionMinHistoryDays;
    }

    public void setCopyDecisionMinHistoryDays(int copyDecisionMinHistoryDays) {
        this.copyDecisionMinHistoryDays = copyDecisionMinHistoryDays;
    }

    public int getCopyDecisionSimulationLookbackDays() {
        return copyDecisionSimulationLookbackDays;
    }

    public void setCopyDecisionSimulationLookbackDays(int copyDecisionSimulationLookbackDays) {
        this.copyDecisionSimulationLookbackDays = copyDecisionSimulationLookbackDays;
    }

    public int getCopyDecisionMaxFactsPerUnit() {
        return copyDecisionMaxFactsPerUnit;
    }

    public void setCopyDecisionMaxFactsPerUnit(int copyDecisionMaxFactsPerUnit) {
        this.copyDecisionMaxFactsPerUnit = copyDecisionMaxFactsPerUnit;
    }

    public int getCopyDecisionTimeoutMs() {
        return copyDecisionTimeoutMs;
    }

    public void setCopyDecisionTimeoutMs(int copyDecisionTimeoutMs) {
        this.copyDecisionTimeoutMs = copyDecisionTimeoutMs;
    }
}
