package com.apunto.engine.dto.client;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class CopyDecisionDto {

    private String walletId;
    private String strategyCode;
    private String scopeType;
    private String scopeValue;
    private String strategyKey;
    private String mode;
    private String simulationMode;
    private boolean fullMaterialized;
    private String fullMaterializationStatus;
    private boolean factPayloadLoaded;
    private boolean decisionFinal;
    private boolean requiresFullSimulation;
    private CopyGuardDto copyGuard;
    private boolean canShadow;
    private boolean canMicroLive;
    private boolean canLive;
    private boolean allowNewEntries;
    private String reasonCode;
    private String reasonDetail;
    private Integer minHistoryDays;
    private Integer simulationLookbackDays;
    private Integer maxFactsPerUnit;
    private Integer factsLoaded;
    private Long elapsedMs;
    private BigDecimal capitalRequiredCopy;
    private BigDecimal maxExposureCopy;
    private BigDecimal pnlCopyTotalUSDT;
    private BigDecimal pnlCopyTotalGrossUSDT;
    private BigDecimal pnlCopyTotalNetUSDT;
    private Object pnlCopyWindows;
    private Map<String, Object> copyGuardWindowAudit;
    private Map<String, Object> copySizingAudit;

    public String getWalletId() {
        return walletId;
    }

    public void setWalletId(String walletId) {
        this.walletId = walletId;
    }

    public String getStrategyCode() {
        return strategyCode;
    }

    public void setStrategyCode(String strategyCode) {
        this.strategyCode = strategyCode;
    }

    public String getScopeType() {
        return scopeType;
    }

    public void setScopeType(String scopeType) {
        this.scopeType = scopeType;
    }

    public String getScopeValue() {
        return scopeValue;
    }

    public void setScopeValue(String scopeValue) {
        this.scopeValue = scopeValue;
    }

    public String getStrategyKey() {
        return strategyKey;
    }

    public void setStrategyKey(String strategyKey) {
        this.strategyKey = strategyKey;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getSimulationMode() {
        return simulationMode;
    }

    public void setSimulationMode(String simulationMode) {
        this.simulationMode = simulationMode;
    }

    public boolean isFullMaterialized() {
        return fullMaterialized;
    }

    public void setFullMaterialized(boolean fullMaterialized) {
        this.fullMaterialized = fullMaterialized;
    }

    public String getFullMaterializationStatus() {
        return fullMaterializationStatus;
    }

    public void setFullMaterializationStatus(String fullMaterializationStatus) {
        this.fullMaterializationStatus = fullMaterializationStatus;
    }

    public boolean isFactPayloadLoaded() {
        return factPayloadLoaded;
    }

    public void setFactPayloadLoaded(boolean factPayloadLoaded) {
        this.factPayloadLoaded = factPayloadLoaded;
    }

    public boolean isDecisionFinal() {
        return decisionFinal;
    }

    public void setDecisionFinal(boolean decisionFinal) {
        this.decisionFinal = decisionFinal;
    }

    public boolean isRequiresFullSimulation() {
        return requiresFullSimulation;
    }

    public void setRequiresFullSimulation(boolean requiresFullSimulation) {
        this.requiresFullSimulation = requiresFullSimulation;
    }

    public CopyGuardDto getCopyGuard() {
        return copyGuard;
    }

    public void setCopyGuard(CopyGuardDto copyGuard) {
        this.copyGuard = copyGuard;
    }

    public boolean isCanShadow() {
        return canShadow;
    }

    public void setCanShadow(boolean canShadow) {
        this.canShadow = canShadow;
    }

    public boolean isCanMicroLive() {
        return canMicroLive;
    }

    public void setCanMicroLive(boolean canMicroLive) {
        this.canMicroLive = canMicroLive;
    }

    public boolean isCanLive() {
        return canLive;
    }

    public void setCanLive(boolean canLive) {
        this.canLive = canLive;
    }

    public boolean isAllowNewEntries() {
        return allowNewEntries;
    }

    public void setAllowNewEntries(boolean allowNewEntries) {
        this.allowNewEntries = allowNewEntries;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public void setReasonCode(String reasonCode) {
        this.reasonCode = reasonCode;
    }

    public String getReasonDetail() {
        return reasonDetail;
    }

    public void setReasonDetail(String reasonDetail) {
        this.reasonDetail = reasonDetail;
    }

    public Integer getMinHistoryDays() {
        return minHistoryDays;
    }

    public void setMinHistoryDays(Integer minHistoryDays) {
        this.minHistoryDays = minHistoryDays;
    }

    public Integer getSimulationLookbackDays() {
        return simulationLookbackDays;
    }

    public void setSimulationLookbackDays(Integer simulationLookbackDays) {
        this.simulationLookbackDays = simulationLookbackDays;
    }

    public Integer getMaxFactsPerUnit() {
        return maxFactsPerUnit;
    }

    public void setMaxFactsPerUnit(Integer maxFactsPerUnit) {
        this.maxFactsPerUnit = maxFactsPerUnit;
    }

    public Integer getFactsLoaded() {
        return factsLoaded;
    }

    public void setFactsLoaded(Integer factsLoaded) {
        this.factsLoaded = factsLoaded;
    }

    public Long getElapsedMs() {
        return elapsedMs;
    }

    public void setElapsedMs(Long elapsedMs) {
        this.elapsedMs = elapsedMs;
    }

    public BigDecimal getCapitalRequiredCopy() {
        return capitalRequiredCopy;
    }

    public void setCapitalRequiredCopy(BigDecimal capitalRequiredCopy) {
        this.capitalRequiredCopy = capitalRequiredCopy;
    }

    public BigDecimal getMaxExposureCopy() {
        return maxExposureCopy;
    }

    public void setMaxExposureCopy(BigDecimal maxExposureCopy) {
        this.maxExposureCopy = maxExposureCopy;
    }

    public BigDecimal getPnlCopyTotalUSDT() {
        return pnlCopyTotalUSDT;
    }

    public void setPnlCopyTotalUSDT(BigDecimal pnlCopyTotalUSDT) {
        this.pnlCopyTotalUSDT = pnlCopyTotalUSDT;
    }

    public BigDecimal getPnlCopyTotalGrossUSDT() {
        return pnlCopyTotalGrossUSDT;
    }

    public void setPnlCopyTotalGrossUSDT(BigDecimal pnlCopyTotalGrossUSDT) {
        this.pnlCopyTotalGrossUSDT = pnlCopyTotalGrossUSDT;
    }

    public BigDecimal getPnlCopyTotalNetUSDT() {
        return pnlCopyTotalNetUSDT;
    }

    public void setPnlCopyTotalNetUSDT(BigDecimal pnlCopyTotalNetUSDT) {
        this.pnlCopyTotalNetUSDT = pnlCopyTotalNetUSDT;
    }

    public Object getPnlCopyWindows() {
        return pnlCopyWindows;
    }

    public void setPnlCopyWindows(Object pnlCopyWindows) {
        this.pnlCopyWindows = pnlCopyWindows;
    }

    public Map<String, Object> getCopyGuardWindowAudit() {
        return copyGuardWindowAudit;
    }

    public void setCopyGuardWindowAudit(Map<String, Object> copyGuardWindowAudit) {
        this.copyGuardWindowAudit = copyGuardWindowAudit;
    }

    public Map<String, Object> getCopySizingAudit() {
        return copySizingAudit;
    }

    public void setCopySizingAudit(Map<String, Object> copySizingAudit) {
        this.copySizingAudit = copySizingAudit;
    }

    public static class CopyGuardDto {
        private String status;
        private String action;
        private Boolean allowNewEntries;
        private List<String> reasons;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public Boolean getAllowNewEntries() {
            return allowNewEntries;
        }

        public void setAllowNewEntries(Boolean allowNewEntries) {
            this.allowNewEntries = allowNewEntries;
        }

        public List<String> getReasons() {
            return reasons;
        }

        public void setReasons(List<String> reasons) {
            this.reasons = reasons;
        }
    }
}
