package com.apunto.engine.service.metric;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class MetricStrategyShadowProjectionMapper {

    public MetricaWalletDto toShadowProjection(MetricStrategySnapshotDto source) {
        if (source == null || source.getEvaluationMode() != MetricStrategySnapshotDto.EvaluationMode.FULL) {
            throw new IllegalArgumentException("FULL_METRIC_V2_REQUIRED_FOR_SHADOW_PROJECTION");
        }
        Map<String, Object> simulation = source.getSimulation() == null
                ? Map.of()
                : source.getSimulation();
        boolean eligible = source.isEligibleForShadow();
        MetricaWalletDto.CopyGuardDto copyGuard = MetricaWalletDto.CopyGuardDto.builder()
                .status(eligible ? "OK" : "BLOCKED")
                .action(eligible ? "ALLOW" : "PAUSE_OPEN")
                .allowNewEntries(eligible)
                .allowReductions(true)
                .allowCloses(true)
                .capitalMultiplier(eligible ? 1.0 : 0.0)
                .reasons(source.getReasonCodes())
                .build();

        return MetricaWalletDto.builder()
                .metricVersion(source.getMetricVersion())
                .sourceVersion(source.getSourceVersion())
                .generationId(source.getGenerationId())
                .generationActivatedAt(source.getGenerationActivatedAt())
                .computedAt(source.getComputedAt())
                .dataAsOf(source.getDataAsOf())
                .strategyKey(source.getStrategyKey())
                .evaluationMode(source.getEvaluationMode().name())
                .allowNewEntries(source.isAllowNewEntries())
                .decisionFinal(source.isDecisionFinal())
                .reasonCodes(source.getReasonCodes())
                .qualityFlags(source.getQualityFlags())
                .unknownEconomicFields(source.getUnknownEconomicFields())
                .wallet(MetricaWalletDto.WalletDto.builder()
                        .idWallet(source.getWalletId())
                        .platform("hyperliquid")
                        .historyDays(source.getHistoryDays() == null ? null : source.getHistoryDays().doubleValue())
                        .countOperation(source.getCompleteCycles())
                        .countOperationBreakdown(MetricaWalletDto.CountOperationBreakdownDto.builder()
                                .strategyCode(source.getStrategyCode())
                                .scopeType(source.getScopeType())
                                .scopeValue(source.getScopeValue())
                                .metricOperationCount(source.getCompleteCycles())
                                .build())
                        .build())
                .strategy(MetricaWalletDto.StrategyDto.builder()
                        .strategyCode(source.getStrategyCode())
                        .rankWithinStrategy(source.getRankWithinStrategy())
                        .globalRank(source.getGlobalRank())
                        .sourceEndpoint("/operaciones/metrica/joyas?simulation=full")
                        .copyability(MetricaWalletDto.CopyabilityDto.builder()
                                .type("COPYABLE")
                                .canOpenPosition(eligible)
                                .supportedByJoyas(true)
                                .build())
                        .copyGuard(copyGuard)
                        .build())
                .realJewel(MetricaWalletDto.RealJewelDto.builder()
                        .basis("wallet_metric_economic_v2")
                        .unitKey(source.getStrategyKey())
                        .walletId(source.getWalletId())
                        .strategyCode(source.getStrategyCode())
                        .scopeType(source.getScopeType())
                        .scopeValue(source.getScopeValue())
                        .canShadow(eligible)
                        .canMicroLive(false)
                        .canLive(false)
                        .status(eligible ? "ELIGIBLE_FOR_SHADOW" : "BLOCKED")
                        .hardBlockers(eligible ? List.of() : source.getReasonCodes())
                        .reasons(source.getReasonCodes())
                        .copyGuard(copyGuard)
                        .build())
                .scoring(MetricaWalletDto.ScoringDto.builder()
                        .decisionMetric(score(source))
                        .decisionMetricConservative(score(source))
                        .passesFilter(eligible)
                        .preCopiable(eligible)
                        .build())
                .copySimulation(MetricaWalletDto.CopySimulationDto.builder()
                        .pnlCopyTotalUSDT(number(simulation, "copyNetPnlUsd"))
                        .pnlCopyTotalGrossUSDT(number(simulation, "copyGrossPnlUsd"))
                        .pnlCopyTotalNetUSDT(number(simulation, "copyNetPnlUsd"))
                        .decisionUse(source.getDecisionUse())
                        .decisionFinal(source.isDecisionFinal())
                        .requiresSimulationFull(Boolean.TRUE.equals(source.getRequiresFullSimulation()))
                        .build())
                .dataQuality(MetricaWalletDto.DataQualityDto.builder()
                        .flags(source.getQualityFlags())
                        .build())
                .capitalShare(0.0)
                .build();
    }

    private static int score(MetricStrategySnapshotDto source) {
        int rank = source.getGlobalRank() == null ? 100 : Math.max(1, source.getGlobalRank());
        return Math.max(1, 101 - Math.min(100, rank));
    }

    private static Double number(Map<String, Object> values, String name) {
        Object raw = values.get(name);
        if (raw == null) return null;
        if (raw instanceof Number number) return number.doubleValue();
        try {
            return Double.valueOf(String.valueOf(raw));
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
