package com.apunto.engine.service.copy.dispatch;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.util.UUID;

@ConfigurationProperties(prefix = "copy.b2b-real-money")
public class B2bRealMoneyGuardProperties implements InitializingBean {
    private boolean enabled;
    private String explicitAcknowledgement = "";
    private boolean emergencyStop = true;
    private boolean manualPositionsVerified;
    private String testUserId = "";
    private String allowedSymbols = "BTCUSDC,ETHUSDC";
    private String clientOrderIdPrefix = "codex-b2b-";
    private BigDecimal maxTotalMarginUsd = new BigDecimal("10");
    private BigDecimal maxNotionalUsd = new BigDecimal("50");
    private int maxLeverage = 5;
    private int maxOrders = 8;
    private UUID microLiveExecutionAccountId;
    private UUID liveExecutionAccountId;

    @Override
    public void afterPropertiesSet() {
        if (!enabled) return;
        if (!"I_ACCEPT_MAX_10_USDC_REAL_MARGIN".equals(explicitAcknowledgement)) {
            throw new IllegalArgumentException("B2B real-money acknowledgement is required");
        }
        if (maxTotalMarginUsd == null || maxTotalMarginUsd.signum() <= 0
                || maxTotalMarginUsd.compareTo(new BigDecimal("10")) > 0) {
            throw new IllegalArgumentException("B2B max total margin must be in (0, 10]");
        }
        if (maxNotionalUsd == null || maxNotionalUsd.signum() <= 0
                || maxNotionalUsd.compareTo(new BigDecimal("50")) > 0) {
            throw new IllegalArgumentException("B2B max notional must be in (0, 50]");
        }
        if (maxLeverage < 1 || maxLeverage > 5) {
            throw new IllegalArgumentException("B2B max leverage must be in [1, 5]");
        }
        if (maxOrders < 2) throw new IllegalArgumentException("B2B max orders must reserve a CLOSE slot");
        if (microLiveExecutionAccountId == null || liveExecutionAccountId == null) {
            throw new IllegalArgumentException("B2B execution account ids are required for both purposes");
        }
        if (microLiveExecutionAccountId.equals(liveExecutionAccountId)) {
            throw new IllegalArgumentException("B2B_EXECUTION_ACCOUNTS_NOT_ISOLATED");
        }
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getExplicitAcknowledgement() { return explicitAcknowledgement; }
    public void setExplicitAcknowledgement(String value) { this.explicitAcknowledgement = value; }
    public boolean isEmergencyStop() { return emergencyStop; }
    public void setEmergencyStop(boolean emergencyStop) { this.emergencyStop = emergencyStop; }
    public boolean isManualPositionsVerified() { return manualPositionsVerified; }
    public void setManualPositionsVerified(boolean value) { this.manualPositionsVerified = value; }
    public String getTestUserId() { return testUserId; }
    public void setTestUserId(String value) { this.testUserId = value; }
    public String getAllowedSymbols() { return allowedSymbols; }
    public void setAllowedSymbols(String value) { this.allowedSymbols = value; }
    public String getClientOrderIdPrefix() { return clientOrderIdPrefix; }
    public void setClientOrderIdPrefix(String value) { this.clientOrderIdPrefix = value; }
    public BigDecimal getMaxTotalMarginUsd() { return maxTotalMarginUsd; }
    public void setMaxTotalMarginUsd(BigDecimal value) { this.maxTotalMarginUsd = value; }
    public BigDecimal getMaxNotionalUsd() { return maxNotionalUsd; }
    public void setMaxNotionalUsd(BigDecimal value) { this.maxNotionalUsd = value; }
    public int getMaxLeverage() { return maxLeverage; }
    public void setMaxLeverage(int value) { this.maxLeverage = value; }
    public int getMaxOrders() { return maxOrders; }
    public void setMaxOrders(int value) { this.maxOrders = value; }
    public UUID getMicroLiveExecutionAccountId() { return microLiveExecutionAccountId; }
    public void setMicroLiveExecutionAccountId(UUID value) { this.microLiveExecutionAccountId = value; }
    public UUID getLiveExecutionAccountId() { return liveExecutionAccountId; }
    public void setLiveExecutionAccountId(UUID value) { this.liveExecutionAccountId = value; }
}
