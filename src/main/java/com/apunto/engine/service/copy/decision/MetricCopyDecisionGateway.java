package com.apunto.engine.service.copy.decision;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import com.apunto.engine.service.copy.capital.AdaptiveCapitalDecision;
import com.apunto.engine.service.copy.capital.AdaptiveCapitalDecisionEngine;
import com.apunto.engine.service.copy.capital.AdaptiveCapitalInput;
import com.apunto.engine.service.copy.capital.CopyExposureAction;
import com.apunto.engine.service.copy.capital.StrategyOperationalState;
import com.apunto.engine.service.metric.MetricV2SnapshotStore;
import com.apunto.engine.service.metric.MetricWalletReadModeResolver;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class MetricCopyDecisionGateway implements CopyDecisionGateway {

    private final AdaptiveCapitalDecisionEngine adaptiveCapitalEngine = new AdaptiveCapitalDecisionEngine();

    private final MetricWalletsInfoClient metricWalletsInfoClient;
    private final MetricWalletReadModeResolver readModeResolver;
    private final MetricV2SnapshotStore metricV2SnapshotStore;

    public MetricCopyDecisionGateway(
            MetricWalletsInfoClient metricWalletsInfoClient,
            MetricWalletReadModeResolver readModeResolver,
            MetricV2SnapshotStore metricV2SnapshotStore
    ) {
        this.metricWalletsInfoClient = Objects.requireNonNull(metricWalletsInfoClient, "metricWalletsInfoClient");
        this.readModeResolver = Objects.requireNonNull(readModeResolver, "readModeResolver");
        this.metricV2SnapshotStore = Objects.requireNonNull(metricV2SnapshotStore, "metricV2SnapshotStore");
    }

    @Override
    public CopyDecisionDto getFullDecisionExact(CopyDecisionRequest request) {
        if (readModeResolver.readsV2()) {
            MetricStrategySnapshotDto snapshot = metricWalletsInfoClient.metricStrategyDecision(
                    request.walletId(),
                    request.strategyCode(),
                    request.scopeType(),
                    request.scopeValue(),
                    request.mode(),
                    request.simulation(),
                    request.minHistoryDays(),
                    request.simulationLookbackDays(),
                    request.maxFactsPerUnit(),
                    request.timeoutMs(),
                    request.debug()
            );
            return adaptV2(request, snapshot);
        }
        return metricWalletsInfoClient.copyDecision(
                request.walletId(),
                request.strategyCode(),
                request.scopeType(),
                request.scopeValue(),
                request.mode(),
                request.simulation(),
                request.minHistoryDays(),
                request.simulationLookbackDays(),
                request.maxFactsPerUnit(),
                request.timeoutMs(),
                request.debug()
        );
    }

    private CopyDecisionDto adaptV2(CopyDecisionRequest request, MetricStrategySnapshotDto snapshot) {
        String expectedKey = MetricStrategySnapshotDto.canonicalStrategyKey(
                request.walletId(), request.strategyCode(), request.scopeType(), request.scopeValue()
        );
        List<String> errors = new ArrayList<>();
        if (snapshot == null) {
            errors.add("METRIC_V2_EXACT_RESPONSE_MISSING");
        } else {
            errors.addAll(snapshot.contractErrors());
            errors.addAll(snapshot.institutionalFinancialContractErrors());
            if (snapshot.getEvaluationMode() != MetricStrategySnapshotDto.EvaluationMode.FULL) {
                errors.add("METRIC_V2_EXACT_FULL_REQUIRED");
            }
            if (!expectedKey.equals(snapshot.getStrategyKey())) {
                errors.add("METRIC_V2_EXACT_STRATEGY_KEY_MISMATCH");
            }
        }

        CopyStrategyGuardDecision guard = errors.isEmpty()
                ? metricV2SnapshotStore.evaluate(
                        snapshot.getWalletId(),
                        snapshot.getStrategyCode(),
                        snapshot.getScopeType(),
                        snapshot.getScopeValue()
                )
                : CopyStrategyGuardDecision.blocked(errors.getFirst(), String.join(",", errors));
        if (guard.allowed() && !Objects.equals(snapshot.getGenerationId(), guard.decisionVersion())) {
            guard = CopyStrategyGuardDecision.blocked(
                    "METRIC_V2_EXACT_CACHE_GENERATION_MISMATCH",
                    "exactGeneration=" + snapshot.getGenerationId() + ",cacheGeneration=" + guard.decisionVersion()
            );
        }
        AdaptiveCapitalDecision adaptiveDecision = errors.isEmpty()
                ? adaptiveShadowDecision(snapshot)
                : null;
        if (adaptiveDecision != null && !adaptiveDecision.allowAction()) {
            String reason = adaptiveDecision.reasonCodes().isEmpty()
                    ? "ADAPTIVE_CAPITAL_BLOCKED"
                    : adaptiveDecision.reasonCodes().getFirst();
            errors.add(reason);
            guard = CopyStrategyGuardDecision.blocked(reason,
                    String.join(",", adaptiveDecision.reasonCodes()));
        }

        boolean fullMaterialized = errors.isEmpty();
        boolean factPayloadLoaded = fullMaterialized
                && snapshot.getCoverage() != null
                && snapshot.getCoverage().getFactsReturned() != null
                && snapshot.getCoverage().getFactsReturned() > 0;
        boolean shadowAllowed = fullMaterialized && snapshot.isEligibleForShadow() && guard.allowed();
        String reason = reasonCode(snapshot, errors, guard, shadowAllowed);

        CopyDecisionDto result = new CopyDecisionDto();
        result.setWalletId(snapshot == null ? request.walletId() : snapshot.getWalletId());
        result.setStrategyCode(snapshot == null ? request.strategyCode() : snapshot.getStrategyCode());
        result.setScopeType(snapshot == null ? request.scopeType() : snapshot.getScopeType());
        result.setScopeValue(snapshot == null ? request.scopeValue() : snapshot.getScopeValue());
        result.setStrategyKey(snapshot == null ? expectedKey : snapshot.getStrategyKey());
        result.setMode(request.mode());
        result.setSimulationMode(request.simulation());
        result.setFullMaterialized(fullMaterialized);
        result.setFullMaterializationStatus(fullMaterialized ? "METRIC_V2_FULL" : "METRIC_V2_FULL_INVALID");
        result.setFactPayloadLoaded(factPayloadLoaded);
        result.setDecisionFinal(fullMaterialized && snapshot.isDecisionFinal());
        result.setRequiresFullSimulation(!fullMaterialized || Boolean.TRUE.equals(snapshot.getRequiresFullSimulation()));
        result.setCanShadow(shadowAllowed);
        result.setCanMicroLive(false);
        result.setCanLive(false);
        result.setAllowNewEntries(shadowAllowed);
        result.setReasonCode(reason);
        result.setReasonDetail(guard.detail());
        result.setMinHistoryDays(request.minHistoryDays());
        result.setSimulationLookbackDays(request.simulationLookbackDays());
        result.setMaxFactsPerUnit(request.maxFactsPerUnit());
        result.setFactsLoaded(snapshot == null || snapshot.getCoverage() == null
                ? null
                : snapshot.getCoverage().getFactsReturned());

        CopyDecisionDto.CopyGuardDto copyGuard = new CopyDecisionDto.CopyGuardDto();
        copyGuard.setStatus(guard.allowed() ? "OK" : firstNonBlank(guard.statusWhenBlocked(), "BLOCKED"));
        copyGuard.setAction(guard.allowed() ? "ALLOW" : firstNonBlank(guard.action(), "PAUSE_OPEN"));
        copyGuard.setAllowNewEntries(shadowAllowed);
        copyGuard.setReasons(List.of(reason));
        result.setCopyGuard(copyGuard);
        return result;
    }

    private AdaptiveCapitalDecision adaptiveShadowDecision(MetricStrategySnapshotDto snapshot) {
        Map<String, Object> operational = snapshot.getOperationalDecision();
        boolean operationalAllows = booleanValue(operational, "allowNewEntries", false);
        return adaptiveCapitalEngine.evaluate(AdaptiveCapitalInput.builder()
                .strategyKey(snapshot.getStrategyKey())
                .action(CopyExposureAction.OPEN)
                .executionMode("SHADOW")
                .decisionFinal(snapshot.isDecisionFinal())
                .eligibleForShadow(Boolean.TRUE.equals(snapshot.getEligibleForShadow()))
                .metricsFresh(snapshot.getDataFreshnessSeconds() != null
                        && snapshot.getDataFreshnessSeconds() >= 0)
                // The authoritative equity gate remains in copy-target-core at event time.
                .sourceEquityAvailable(true)
                .copyGuardAction(operationalAllows ? "ALLOW" : "PAUSE_OPEN")
                .operationalState(stateValue(operational == null ? null : operational.get("operationalState")))
                .currentCapitalMultiplier(decimalValue(operational, "capitalMultiplier", BigDecimal.ZERO))
                .requestedCapitalUsd(new BigDecimal("100"))
                .capacityUsd(financialValue(snapshot.getFinancialMetrics(), "capacityUsd"))
                .evidenceScore(scoreValue(snapshot.getScores(), "evidenceScore"))
                .healthyConsecutiveEvaluations(0)
                .microLiveEnabled(false)
                .liveEnabled(false)
                .build());
    }

    private static StrategyOperationalState stateValue(Object value) {
        try {
            return StrategyOperationalState.valueOf(String.valueOf(value).trim().toUpperCase());
        } catch (RuntimeException ex) {
            return StrategyOperationalState.CANDIDATE;
        }
    }

    private static boolean booleanValue(Map<String, Object> values, String key, boolean fallback) {
        if (values == null) return fallback;
        Object value = values.get(key);
        return value instanceof Boolean bool ? bool : fallback;
    }

    private static BigDecimal decimalValue(Map<String, Object> values, String key, BigDecimal fallback) {
        if (values == null) return fallback;
        Object value = values.get(key);
        if (value instanceof Number number) return BigDecimal.valueOf(number.doubleValue());
        try {
            return value == null ? fallback : new BigDecimal(String.valueOf(value));
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    @SuppressWarnings("unchecked")
    private static BigDecimal financialValue(Map<String, Object> metrics, String field) {
        if (metrics == null || !(metrics.get(field) instanceof Map<?, ?> metric)) return null;
        Object value = metric.get("value");
        return value instanceof Number number ? BigDecimal.valueOf(number.doubleValue()) : null;
    }

    @SuppressWarnings("unchecked")
    private static BigDecimal scoreValue(Map<String, Object> scores, String field) {
        if (scores == null || !(scores.get(field) instanceof Map<?, ?> score)) return null;
        Object value = score.get("value");
        return value instanceof Number number ? BigDecimal.valueOf(number.doubleValue()) : null;
    }

    private static String reasonCode(
            MetricStrategySnapshotDto snapshot,
            List<String> errors,
            CopyStrategyGuardDecision guard,
            boolean shadowAllowed
    ) {
        if (!errors.isEmpty()) return errors.getFirst();
        if (!snapshot.isEligibleForShadow()) {
            return snapshot.getReasonCodes() == null || snapshot.getReasonCodes().isEmpty()
                    ? "METRIC_V2_FULL_BLOCKED"
                    : firstNonBlank(snapshot.getReasonCodes().getFirst(), "METRIC_V2_FULL_BLOCKED");
        }
        if (!guard.allowed()) return firstNonBlank(guard.reason(), "METRIC_V2_COPY_GUARD_BLOCKED");
        return shadowAllowed ? "V2_MONEY_PROMOTION_DISABLED" : "METRIC_V2_FULL_BLOCKED";
    }

    private static String firstNonBlank(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
