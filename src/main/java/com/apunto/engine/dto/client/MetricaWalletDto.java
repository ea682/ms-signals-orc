package com.apunto.engine.dto.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricaWalletDto {

    private WalletDto wallet;
    private ActivityDto activity;
    private StrategyDto strategy;
    private RealJewelDto realJewel;
    private ScoringDto scoring;
    private CopySimulationDto copySimulation;
    private ExposureAndCapacityDto exposureAndCapacity;
    private CapitalModelDto capitalModel;
    private TradeStatsDto tradeStats;
    private PerformanceDto performance;
    private RiskDto risk;
    private RiskAdjustedRatiosDto riskAdjustedRatios;
    private ConsistencyAndEdgeDto consistencyAndEdge;
    private DistributionDto distribution;
    private RollingStabilityDto rollingStability;
    private ExecutionAndFrictionDto executionAndFriction;
    private MarketRelationDto marketRelation;
    private DataQualityDto dataQuality;
    private double capitalShare;

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WalletDto {
        private String idWallet;
        private String platform;
        private OffsetDateTime startDate;
        private OffsetDateTime endDate;
        private Double historyDays;
        private Integer countOperation;
        private CountOperationBreakdownDto countOperationBreakdown;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CountOperationBreakdownDto {
        private String strategyCode;
        private String strategyLabel;
        private String scopeType;
        private String scopeValue;
        private Integer metricOperationCount;
        private Integer totalTradeFacts;
        private Integer partialCloseFacts;
        private Integer finalCloseFacts;
        private Integer distinctPositions;
        private Integer wins;
        private Integer losses;
        private Integer breakeven;
        private Integer simulatableTradeFacts;
        private CopySimulationCoverageDto simulationCoverage;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StrategyDto {
        private String strategyCode;
        private String strategySlug;
        private String strategyLabel;
        private String copyMode;
        private String countOperationMeaning;
        private String strategySource;
        private Integer rankWithinStrategy;
        private Integer globalRank;
        private String sourceEndpoint;
        private Double score;
        private CopyabilityDto copyability;
        private CopyGuardDto copyGuard;
        private RealJewelDto riskAdjustedCapitalEfficiency;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RealJewelDto {
        private String basis;
        private String unitKey;
        private String walletId;
        private String strategyCode;
        private String strategyLabel;
        private String scopeType;
        private String scopeValue;
        private Double scoreFinal;
        private Double rankingScore;
        private Double capitalEfficiency;
        private Double capitalEfficiencyPct;
        private Double pnlCopyNetAdjusted;
        private Double capitalUsed;
        private String capitalUsedBasis;
        private Double riskScore;
        private Double dataQualityScore;
        private Double consistencyScore;
        private Double executionScore;
        private Double shadowScore;
        private Double strategyLiftScore;
        private String status;
        private CopyGuardDto copyGuard;
        private List<String> reasons;
        private List<String> hardRejectReasons;
        private List<String> flags;
        private Boolean requiresFullSimulation;
        private Boolean redundantVariant;
        private String redundantOf;
        private Double redundantOverlapPct;
        private Map<String, Object> topTradeConcentration;
        private String explanation;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CopyGuardDto {
        private String status;
        private String action;
        private Boolean allowNewEntries;
        private Boolean allowReductions;
        private Boolean allowCloses;
        private Double capitalMultiplier;
        private String targetExecutionMode;
        private Double severityScore;
        private List<String> reasons;
        private List<String> logs;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CopyabilityDto {
        private String type;
        private Boolean canOpenPosition;
        private Boolean requiresParentStrategy;
        private Boolean supportedByJoyas;
        private String warning;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ActivityDto {
        private Boolean hasOpenPositions;
        private Integer openPositionsCount;
        private OffsetDateTime lastActivityAt;
        private OffsetDateTime lastOpenedAt;
        private OffsetDateTime lastClosedAt;
        private Double inactiveDays;
        private Double inactiveDaysSinceLastOpen;
        private Double inactiveDaysSinceLastClose;
        private OffsetDateTime walletLastActivityAt;
        private OffsetDateTime walletLastOpenedAt;
        private OffsetDateTime walletLastClosedAt;
        private OffsetDateTime strategyLastActivityAt;
        private OffsetDateTime strategyLastOpenedAt;
        private OffsetDateTime strategyLastClosedAt;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ScoringDto {
        private Integer decisionMetric;
        private Integer decisionMetricConservative;
        private Integer decisionMetricSwing;
        private Integer decisionMetricScalping;
        private Integer decisionMetricAggressive;
        private Integer decisionMetricCapacityAware;

        private Boolean passesFilter;
        private Boolean preCopiable;

        private Double profitScorePct;
        private Double reliabilityScorePct;
        private Double executionScorePct;
        private Double penaltyPct;

        private EligibilityDto eligibility;
        private String observations;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class EligibilityDto {
        private ModeEligibilityDto scalping;
        private ModeEligibilityDto swing;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ModeEligibilityDto {
        private Boolean eligible;
        private String reason;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CopySimulationDto {
        private Map<String, Object> policy;

        private Double capitalRequiredCopy;
        private Double maxExposureCopy;

        /**
         * Bucketed simulation PnL maps returned by ms-metrica-cuenta.
         * Examples: 1d, 1w, 2w, 1mo. Net includes fees/friction and is the
         * source of truth for runtime copy guard decisions.
         */
        private Map<String, Double> pnlCopy;
        private Map<String, Double> pnlCopyGross;
        private Map<String, Double> pnlCopyNet;

        private Double pnlCopyDayUSDT;
        private Double pnlCopyWeekUSDT;
        private Double pnlCopyMonthUSDT;
        private Double pnlCopyTotalUSDT;

        private Double pnlCopyTotalGrossUSDT;
        private Double pnlCopyTotalNetUSDT;
        private Double roiAnnualizedCopy;
        private Double roiAnnualizedCopyGross;
        private Double roiAnnualizedCopyNet;
        private CopySizingDto copySizing;
        private CopySimulationCoverageDto copySimulationCoverage;
        private Map<String, Object> liveExposure;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ExposureAndCapacityDto {
        private Double capitalRequired;
        private Double maxExposure;

        private Double timeInMarketPct;
        private Double avgHoldingTimeDays;

        private Integer leverageMedian;
        private Integer leverageP95;

        private Double transactionCostImpactUSDT;
        private Double transactionCostImpactPctOfPnlCopy;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CapitalModelDto {
        private Double sourceTotalCapitalUSDT;
        private Double capitalReferenceUSDT;
        private Double capitalReferenceBufferedUSDT;
        private String capitalReferenceType;
        private String capitalReferenceConfidence;
        private OffsetDateTime referenceAsOf;

        private Double peakConcurrentMarginUSDT;
        private Double currentOpenMarginUSDT;
        private Double maxSingleTradeMarginUSDT;

        private Double currentOpenUtilizationPct;
        private Double maxSingleTradeUtilizationPct;

        private List<String> notes;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class TradeStatsDto {
        private Double tradesPerDay;

        private Double longPct;
        private Double shortPct;

        private Double notionalTradedUSDT;
        private Double avgTradeNotionalUSDT;
        private Double medianTradeNotionalUSDT;
        private Double p95TradeNotionalUSDT;

        private Double holdingTimeP50Days;
        private Double holdingTimeP95Days;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class PerformanceDto {
        private Double roiAnnualized;
        private Double promedioROI;

        private Double winRate;
        private Double winRateAnnual;

        private Double expectancy;
        private Double profitFactor;
        private Double avgWinLossRatio;

        private Double antiMartingaleScore;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RiskDto {
        private Double drawdownMax;
        private Double calcDrawdownPercentile;
        private Double recoveryGainNeededPct;

        private Double averageMaxLoss;
        private Double drawdownFrequency;

        private Double medianDDDurationDays;
        private Double p95DDDurationDays;

        private Double riskOfRuinQuarterPct;

        private Double worstDayPct;
        private Double worstWeekPct;
        private Double maxDDRolling50Pct;

        private Double var95;
        private Double cvar95;
        private Double es95;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RiskAdjustedRatiosDto {
        private Double sharpeRatio;
        private Double sortinoRatio;
        private Double calmarRatio;
        private Double omegaRatio;

        private Double tailRatio;
        private Double expectationVarRatio;

        private Double psr;
        private Double dsr;

        private Double pboEstimate;
        private Double pPfGt1;
        private Double pSortinoGt0;

        private Double kellyFraction;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ConsistencyAndEdgeDto {
        private Double edgeDayPct;
        private Double edgeWeekPct;
        private Double edgeMonthPct;
        private Double edgeQuarterPct;
        private Double edgeSemesterPct;
        private Double edgeYearPct;

        private Double consistencyWeekly;
        private Double consistencyMonthly;

        private RecoveryPeriodsDto recoveryPeriods;
        private RecoveryDaysDto recoveryDays;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RecoveryPeriodsDto {
        private Double day;
        private Double week;
        private Double month;
        private Double quarter;
        private Double semester;
        private Double year;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RecoveryDaysDto {
        private Double fromDayEdge;
        private Double fromWeekEdge;
        private Double fromMonthEdge;
        private Double fromQuarterEdge;
        private Double fromSemesterEdge;
        private Double fromYearEdge;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DistributionDto {
        private Double skewness;
        private Double kurtosis;
        private Double giniPayoff;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class RollingStabilityDto {
        private Double rollingCV20;
        private Double rollingCV50;
        private Double rollingCV100;

        private Integer rollingCollapses;

        private Double sharpeRolling20;
        private Double sharpeRolling50;

        private Double pfRolling20;
        private Double pfRolling50;

        private Integer maxConsecutiveWins;
        private Integer maxConsecutiveLosses;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ExecutionAndFrictionDto {
        private Double breakEvenSlippageBps;
        private Double breakEvenSlippageR;

        private Double implementationShortfallBpsMed;
        private Double implementationShortfallBpsP90;

        private Double pfLag5s;
        private Double pfLag30s;
        private Double pfLag5m;

        private Double fillRatioPct;
        private Double partialFillP50Pct;

        private Double stopEfficiencyPct;

        private Double maeMedianR;
        private Double mfeMedianR;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class MarketRelationDto {
        private Double betaBTC;
        private Double corrBTC;
        private Double corrETH;
        private Double infoRatioVsBTC;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DataQualityDto {
        private Map<String, Object> basisCounts;
        private Map<String, Object> issuesByType;
        private List<String> issuesExamplesUnique;
        private List<String> flags;
    }
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CopySizingDto {
        private Double baseCapitalUSDT;
        private Double targetScale;
        private Double targetCapitalUSDT;
        private Double baselinePeakMarginUSDT;
        private Double appliedScaleFactor;
        private Double appliedScalePct;
        private Double realizedPeakMarginUSDT;
        private Double realizedMaxExposureUSDT;
        private Integer sourceFacts;
        private Integer loadedFacts;
        private Integer excludedFacts;
        private CopySimulationCoverageDto simulationCoverage;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CopySimulationCoverageDto {
        private String coverageBasis;
        private Integer metricOperationCount;
        private Integer loadedFacts;
        private Integer simulatedFacts;
        private Integer rawSimulatedFacts;
        private Integer excludedFacts;
        private Double coveragePct;
        private Boolean isComplete;
        private List<String> exclusionReasons;
        private String reconciliationStatus;
        private Integer reconciliationDelta;
        private Integer reconciliationDeltaAbs;
        private Double reconciliationPct;
        private List<String> reconciliationReasons;
    }
}
