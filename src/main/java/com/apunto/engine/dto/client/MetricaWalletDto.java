package com.apunto.engine.dto.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MetricaWalletDto {

    private String idWallet;
    private LocalDateTime startDate;
    private LocalDateTime endDate;

    private Double capitalRequired;
    private Double roiAnnualized;
    private Double promedioROI;
    private Double winRate;
    private Double winRateAnnual;
    private Double drawdownMax;
    private Double drawdownDuration;
    private Double var95;
    private Double cvar95;
    private Double omegaRatio;
    private Double profitFactor;
    private Double sharpeRatio;
    private Double sortinoRatio;
    private Double calmarRatio;
    private Double expectancy;
    private Double averageMaxLoss;
    private Double expectationVarRatio;
    private Double drawdownFrequency;
    private Double slippageCostRatio;
    private Double correlation;
    private Double maxConsecutiveLosses;
    private Double maxConsecutiveWins;
    private Double recoveryFactor;
    private Double avgHoldingTimeDays;
    private Double avgWinLossRatio;
    private Double consistencyWeekly;
    private Double consistencyMonthly;
    private Double maxExposure;

    private int countOperation;
    private double decisionMetric;

    private Boolean passesFilter;
    private String observations;
    private Integer monthsWithCriticalDD;
    private Double calcDrawdownPercentile;

    private double transactionCostImpact;
    private double skewness;
    private double kurtosis;

    private Double recoveryGainNeededPct;
    private Double edgeDayPct;
    private Double edgeWeekPct;
    private Double edgeMonthPct;
    private Double edgeQuarterPct;
    private Double edgeSemesterPct;
    private Double edgeYearPct;

    private Integer recoveryPeriodsDay;
    private Integer recoveryPeriodsWeek;
    private Integer recoveryPeriodsMonth;
    private Integer recoveryPeriodsQuarter;
    private Integer recoveryPeriodsSemester;
    private Integer recoveryPeriodsYear;

    private Integer recoveryDaysFromDayEdge;
    private Integer recoveryDaysFromWeekEdge;
    private Integer recoveryDaysFromMonthEdge;
    private Integer recoveryDaysFromQuarterEdge;
    private Integer recoveryDaysFromSemesterEdge;
    private Integer recoveryDaysFromYearEdge;

    private Double copySizing;
    private Boolean preCopiable;
    private double capitalShare;
}

