package com.apunto.engine.service.copy.promotion;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "copy.promotion")
public class ShadowPromotionProperties {

    private boolean enabled = true;
    private boolean fromShadowEnabled = true;
    private int candidateLimit = 100;
    private long minShadowDays = 3;
    private long minShadowEvents = 10;
    private long minShadowClosedPositions = 3;
    private BigDecimal minShadowCoveragePct = new BigDecimal("95");
    private boolean requireActiveWallet = true;
    private long maxInactiveDays = 3;
    private boolean requirePositiveShadowPnl = false;
    private BigDecimal minShadowNetPnlUsd = BigDecimal.ZERO;
    private BigDecimal maxShadowDrawdownPct = new BigDecimal("20");
    private boolean requireCopyGuardOpen = true;
    private boolean allowShadowOnlySummary = false;
    private String defaultTargetMode = "MICRO_LIVE";
    private BigDecimal microLiveInitialCapitalUsd = new BigDecimal("100");
    private BigDecimal microLiveMaxCapitalUsd = new BigDecimal("100");

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isFromShadowEnabled() {
        return fromShadowEnabled;
    }

    public void setFromShadowEnabled(boolean fromShadowEnabled) {
        this.fromShadowEnabled = fromShadowEnabled;
    }

    public int getCandidateLimit() {
        return candidateLimit;
    }

    public void setCandidateLimit(int candidateLimit) {
        this.candidateLimit = candidateLimit;
    }

    public long getMinShadowDays() {
        return minShadowDays;
    }

    public void setMinShadowDays(long minShadowDays) {
        this.minShadowDays = minShadowDays;
    }

    public long getMinShadowEvents() {
        return minShadowEvents;
    }

    public void setMinShadowEvents(long minShadowEvents) {
        this.minShadowEvents = minShadowEvents;
    }

    public long getMinShadowClosedPositions() {
        return minShadowClosedPositions;
    }

    public void setMinShadowClosedPositions(long minShadowClosedPositions) {
        this.minShadowClosedPositions = minShadowClosedPositions;
    }

    public BigDecimal getMinShadowCoveragePct() {
        return minShadowCoveragePct;
    }

    public void setMinShadowCoveragePct(BigDecimal minShadowCoveragePct) {
        this.minShadowCoveragePct = minShadowCoveragePct;
    }

    public boolean isRequireActiveWallet() {
        return requireActiveWallet;
    }

    public void setRequireActiveWallet(boolean requireActiveWallet) {
        this.requireActiveWallet = requireActiveWallet;
    }

    public long getMaxInactiveDays() {
        return maxInactiveDays;
    }

    public void setMaxInactiveDays(long maxInactiveDays) {
        this.maxInactiveDays = maxInactiveDays;
    }

    public boolean isRequirePositiveShadowPnl() {
        return requirePositiveShadowPnl;
    }

    public void setRequirePositiveShadowPnl(boolean requirePositiveShadowPnl) {
        this.requirePositiveShadowPnl = requirePositiveShadowPnl;
    }

    public BigDecimal getMinShadowNetPnlUsd() {
        return minShadowNetPnlUsd;
    }

    public void setMinShadowNetPnlUsd(BigDecimal minShadowNetPnlUsd) {
        this.minShadowNetPnlUsd = minShadowNetPnlUsd;
    }

    public BigDecimal getMaxShadowDrawdownPct() {
        return maxShadowDrawdownPct;
    }

    public void setMaxShadowDrawdownPct(BigDecimal maxShadowDrawdownPct) {
        this.maxShadowDrawdownPct = maxShadowDrawdownPct;
    }

    public boolean isRequireCopyGuardOpen() {
        return requireCopyGuardOpen;
    }

    public void setRequireCopyGuardOpen(boolean requireCopyGuardOpen) {
        this.requireCopyGuardOpen = requireCopyGuardOpen;
    }

    public boolean isAllowShadowOnlySummary() {
        return allowShadowOnlySummary;
    }

    public void setAllowShadowOnlySummary(boolean allowShadowOnlySummary) {
        this.allowShadowOnlySummary = allowShadowOnlySummary;
    }

    public String getDefaultTargetMode() {
        return defaultTargetMode;
    }

    public void setDefaultTargetMode(String defaultTargetMode) {
        this.defaultTargetMode = defaultTargetMode;
    }

    public BigDecimal getMicroLiveInitialCapitalUsd() {
        return microLiveInitialCapitalUsd;
    }

    public void setMicroLiveInitialCapitalUsd(BigDecimal microLiveInitialCapitalUsd) {
        this.microLiveInitialCapitalUsd = microLiveInitialCapitalUsd;
    }

    public BigDecimal getMicroLiveMaxCapitalUsd() {
        return microLiveMaxCapitalUsd;
    }

    public void setMicroLiveMaxCapitalUsd(BigDecimal microLiveMaxCapitalUsd) {
        this.microLiveMaxCapitalUsd = microLiveMaxCapitalUsd;
    }
}
