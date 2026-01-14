package com.apunto.engine.dto.client;

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
public class MetricaWalletDto {

    private WalletDto wallet;
    private ActivityDto activity;
    private ScoringDto scoring;
    private CopySimulationDto copySimulation;
    private ExposureAndCapacityDto exposureAndCapacity;
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
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ActivityDto {
        private Boolean hasOpenPositions;
        private Integer openPositionsCount;
        private OffsetDateTime lastActivityAt;
        private OffsetDateTime lastClosedAt;
        private Double inactiveDays;
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

        private Double pnlCopyDayUSDT;
        private Double pnlCopyWeekUSDT;
        private Double pnlCopyMonthUSDT;
        private Double pnlCopyTotalUSDT;

        private Double roiAnnualizedCopy;
        private Double copySizing;
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

        private Integer medianDDDurationDays;
        private Integer p95DDDurationDays;

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
}

