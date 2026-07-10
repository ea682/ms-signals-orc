package com.apunto.engine.dto.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CopyGuardWindowSnapshotDto {

    private String walletId;
    private String copyStrategyCode;
    private String scopeType;
    private String scopeValue;
    private String unitKey;
    private String status;
    private String action;
    private Boolean allowNewEntries;
    private Boolean allowReductions;
    private Boolean allowCloses;
    private Double capitalMultiplier;
    private List<String> reasons;
    private List<String> decisionReasons;
    private List<String> infoReasons;
    private String downgradeType;
    private Boolean requireMicroLiveAgain;
    private String targetExecutionMode;
    private Integer minHistoryDaysForCandidateShadow;
    private Integer minOperationsForCandidateShadow;
    private Double historyDays;
    private Integer operations;
    private List<String> windowsUsed;
    private Map<String, WindowDto> windows;
    private OffsetDateTime computedAt;
    private OffsetDateTime expiresAt;
    private String sourceVersion;
    private String source;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WindowDto {
        private String windowCode;
        private OffsetDateTime windowStartAt;
        private OffsetDateTime windowEndAt;
        private Boolean complete;
        private Boolean mature;
        private Boolean decisionEnabled;
        private Boolean futureWindow;
        private Integer operations;
        private Integer closedOperations;
        private Integer minOperationsForMaturity;
        private Integer minClosedOperationsForMaturity;
        private Double pnlGrossUsd;
        private Double feesUsd;
        private Double slippageUsd;
        private Double pnlNetUsd;
        private Double roiPct;
        private Double maxDrawdownPct;
        private Double winRatePct;
        private Double profitFactor;
        private Double lossPct;
        private Double capitalBaseUsd;
        private Boolean severeLossGuardApplied;
        private String severeLossLevel;
        private String action;
        private String status;
        private String reasonCode;
        private Double capitalMultiplier;
        private String downgradeType;
        private Boolean requireMicroLiveAgain;
        private Boolean decisionParticipates;
        private String infoReason;
        private String suggestedAction;
    }
}
