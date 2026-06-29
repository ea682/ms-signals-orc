package com.apunto.engine.service.copy.readiness;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "copy.live-readiness")
public class ShadowLiveReadinessProperties {

    private long minClosedPositions = 50;
    private BigDecimal minProfitFactor = new BigDecimal("1.3");
    private boolean requirePositiveExpectancy = true;
    private boolean requirePositiveNetPnl = true;
    private BigDecimal maxTop1Concentration = new BigDecimal("0.35");
    private long minStableHoursAfterDeploy = 24;
    private long maxShadowP95EndToEndMs = 500;
    private long maxLiveMockP95EndToEndMs = 1000;
    private long maxBinanceP95HttpMs = 800;
    private BigDecimal maxAdverseSlippageBpsP95 = new BigDecimal("10");
    private BigDecimal maxAdverseSlippageUsdPerOrder = new BigDecimal("1.00");
    private long requireSlippageSampleSize = 20;
    private BigDecimal maxLeverageX = new BigDecimal("5");
    private boolean requireSourceLeverage = true;
    private boolean requireLiveLeverageConfirmed = true;
    private boolean blockOnLeverageMismatch = true;
    private boolean blockOnMarginModeMismatch = true;
    private BigDecimal maxEffectiveLeverageX = new BigDecimal("5");
    private BigDecimal maxLiveNotionalUsdPerOrder = new BigDecimal("10");
    private BigDecimal maxLiveRequiredMarginUsdPerOrder = new BigDecimal("10");

    public long getMinClosedPositions() {
        return minClosedPositions;
    }

    public void setMinClosedPositions(long minClosedPositions) {
        this.minClosedPositions = minClosedPositions;
    }

    public BigDecimal getMinProfitFactor() {
        return minProfitFactor;
    }

    public void setMinProfitFactor(BigDecimal minProfitFactor) {
        this.minProfitFactor = minProfitFactor;
    }

    public boolean isRequirePositiveExpectancy() {
        return requirePositiveExpectancy;
    }

    public void setRequirePositiveExpectancy(boolean requirePositiveExpectancy) {
        this.requirePositiveExpectancy = requirePositiveExpectancy;
    }

    public boolean isRequirePositiveNetPnl() {
        return requirePositiveNetPnl;
    }

    public void setRequirePositiveNetPnl(boolean requirePositiveNetPnl) {
        this.requirePositiveNetPnl = requirePositiveNetPnl;
    }

    public BigDecimal getMaxTop1Concentration() {
        return maxTop1Concentration;
    }

    public void setMaxTop1Concentration(BigDecimal maxTop1Concentration) {
        this.maxTop1Concentration = maxTop1Concentration;
    }

    public long getMinStableHoursAfterDeploy() {
        return minStableHoursAfterDeploy;
    }

    public void setMinStableHoursAfterDeploy(long minStableHoursAfterDeploy) {
        this.minStableHoursAfterDeploy = minStableHoursAfterDeploy;
    }

    public long getMaxShadowP95EndToEndMs() {
        return maxShadowP95EndToEndMs;
    }

    public void setMaxShadowP95EndToEndMs(long maxShadowP95EndToEndMs) {
        this.maxShadowP95EndToEndMs = maxShadowP95EndToEndMs;
    }

    public long getMaxLiveMockP95EndToEndMs() {
        return maxLiveMockP95EndToEndMs;
    }

    public void setMaxLiveMockP95EndToEndMs(long maxLiveMockP95EndToEndMs) {
        this.maxLiveMockP95EndToEndMs = maxLiveMockP95EndToEndMs;
    }

    public long getMaxBinanceP95HttpMs() {
        return maxBinanceP95HttpMs;
    }

    public void setMaxBinanceP95HttpMs(long maxBinanceP95HttpMs) {
        this.maxBinanceP95HttpMs = maxBinanceP95HttpMs;
    }

    public BigDecimal getMaxAdverseSlippageBpsP95() {
        return maxAdverseSlippageBpsP95;
    }

    public void setMaxAdverseSlippageBpsP95(BigDecimal maxAdverseSlippageBpsP95) {
        this.maxAdverseSlippageBpsP95 = maxAdverseSlippageBpsP95;
    }

    public BigDecimal getMaxAdverseSlippageUsdPerOrder() {
        return maxAdverseSlippageUsdPerOrder;
    }

    public void setMaxAdverseSlippageUsdPerOrder(BigDecimal maxAdverseSlippageUsdPerOrder) {
        this.maxAdverseSlippageUsdPerOrder = maxAdverseSlippageUsdPerOrder;
    }

    public long getRequireSlippageSampleSize() {
        return requireSlippageSampleSize;
    }

    public void setRequireSlippageSampleSize(long requireSlippageSampleSize) {
        this.requireSlippageSampleSize = requireSlippageSampleSize;
    }

    public BigDecimal getMaxLeverageX() {
        return maxLeverageX;
    }

    public void setMaxLeverageX(BigDecimal maxLeverageX) {
        this.maxLeverageX = maxLeverageX;
    }

    public boolean isRequireSourceLeverage() {
        return requireSourceLeverage;
    }

    public void setRequireSourceLeverage(boolean requireSourceLeverage) {
        this.requireSourceLeverage = requireSourceLeverage;
    }

    public boolean isRequireLiveLeverageConfirmed() {
        return requireLiveLeverageConfirmed;
    }

    public void setRequireLiveLeverageConfirmed(boolean requireLiveLeverageConfirmed) {
        this.requireLiveLeverageConfirmed = requireLiveLeverageConfirmed;
    }

    public boolean isBlockOnLeverageMismatch() {
        return blockOnLeverageMismatch;
    }

    public void setBlockOnLeverageMismatch(boolean blockOnLeverageMismatch) {
        this.blockOnLeverageMismatch = blockOnLeverageMismatch;
    }

    public boolean isBlockOnMarginModeMismatch() {
        return blockOnMarginModeMismatch;
    }

    public void setBlockOnMarginModeMismatch(boolean blockOnMarginModeMismatch) {
        this.blockOnMarginModeMismatch = blockOnMarginModeMismatch;
    }

    public BigDecimal getMaxEffectiveLeverageX() {
        return maxEffectiveLeverageX;
    }

    public void setMaxEffectiveLeverageX(BigDecimal maxEffectiveLeverageX) {
        this.maxEffectiveLeverageX = maxEffectiveLeverageX;
    }

    public BigDecimal getMaxLiveNotionalUsdPerOrder() {
        return maxLiveNotionalUsdPerOrder;
    }

    public void setMaxLiveNotionalUsdPerOrder(BigDecimal maxLiveNotionalUsdPerOrder) {
        this.maxLiveNotionalUsdPerOrder = maxLiveNotionalUsdPerOrder;
    }

    public BigDecimal getMaxLiveRequiredMarginUsdPerOrder() {
        return maxLiveRequiredMarginUsdPerOrder;
    }

    public void setMaxLiveRequiredMarginUsdPerOrder(BigDecimal maxLiveRequiredMarginUsdPerOrder) {
        this.maxLiveRequiredMarginUsdPerOrder = maxLiveRequiredMarginUsdPerOrder;
    }
}
