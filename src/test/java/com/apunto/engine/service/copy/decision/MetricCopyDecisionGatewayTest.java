package com.apunto.engine.service.copy.decision;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.dto.client.CopyGuardWindowSnapshotDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import com.apunto.engine.service.metric.MetricStrategyShadowProjectionMapper;
import com.apunto.engine.service.metric.MetricV2SnapshotStore;
import com.apunto.engine.service.metric.MetricWalletReadModeResolver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.apunto.engine.dto.client.MetricStrategySnapshotTestFixtures.completeMatrix;

class MetricCopyDecisionGatewayTest {

    private static final String WINDOWS = "1d,3d,1w,2w,3w,1mo,2mo,3mo,6mo,9mo,1y,2y,all";

    @Test
    void v2UsesCanonicalExactContractAndKeepsMoneyPromotionDisabled() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summary = snapshot(false, false);
        MetricStrategySnapshotDto full = snapshot(true, false);
        MetricStrategySnapshotDto guard = snapshot(true, true);
        client.summary = List.of(summary);
        client.full = List.of(full);
        client.guard = List.of(guard);
        client.exact = full;
        MetricWalletReadModeResolver resolver = new MetricWalletReadModeResolver("V2");
        MetricV2SnapshotStore store = store(client, resolver);
        store.refreshNow();
        MetricCopyDecisionGateway gateway = new MetricCopyDecisionGateway(client, resolver, store);

        CopyDecisionDto result = gateway.getFullDecisionExact(request());

        assertTrue(result.isFullMaterialized());
        assertTrue(result.isDecisionFinal());
        assertTrue(result.isCanShadow());
        assertTrue(result.isAllowNewEntries());
        assertFalse(result.isCanMicroLive());
        assertFalse(result.isCanLive());
        assertEquals("ALLOW", result.getCopyGuard().getAction());
        assertEquals("V2_MONEY_PROMOTION_DISABLED", result.getReasonCode());
        assertEquals(1, client.exactCalls);
        assertEquals(0, client.legacyCalls);
    }

    @Test
    void v1AndCompareKeepTheLegacyRollbackContract() {
        FakeMetricClient client = new FakeMetricClient();
        CopyDecisionDto legacy = new CopyDecisionDto();
        client.legacy = legacy;
        MetricWalletReadModeResolver v1Resolver = new MetricWalletReadModeResolver("V1");
        MetricWalletReadModeResolver compareResolver = new MetricWalletReadModeResolver("COMPARE");

        CopyDecisionDto v1 = new MetricCopyDecisionGateway(
                client, v1Resolver, store(client, v1Resolver)
        ).getFullDecisionExact(request());
        CopyDecisionDto compare = new MetricCopyDecisionGateway(
                client, compareResolver, store(client, compareResolver)
        ).getFullDecisionExact(request());

        assertSame(legacy, v1);
        assertSame(legacy, compare);
        assertEquals(2, client.legacyCalls);
        assertEquals(0, client.exactCalls);
    }

    private static MetricV2SnapshotStore store(
            MetricWalletsInfoClient client,
            MetricWalletReadModeResolver resolver
    ) {
        return new MetricV2SnapshotStore(
                client,
                new MetricStrategyShadowProjectionMapper(),
                resolver,
                new SimpleMeterRegistry(),
                100,
                20,
                30,
                Duration.ofMinutes(10),
                Duration.ofMinutes(2),
                Duration.ofMinutes(10),
                false,
                true,
                WINDOWS
        );
    }

    private static CopyDecisionRequest request() {
        return new CopyDecisionRequest(
                "0xabc", "MOVEMENT_ALL", "ALL", "ALL",
                "micro-live-entry", "full", 30, 60, 50000, 55000, false
        );
    }

    private static MetricStrategySnapshotDto snapshot(boolean full, boolean guard) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(1);
        MetricStrategySnapshotDto result = MetricStrategySnapshotDto.builder()
                .metricVersion(2)
                .sourceVersion(MetricStrategySnapshotDto.SOURCE_VERSION)
                .generationId("gen-1")
                .generationStatus("ACTIVE")
                .readMode("V2")
                .responseSource(MetricStrategySnapshotDto.RESPONSE_SOURCE)
                .calculatorVersion("wallet-strategy-financial-v3.0.0")
                .policyVersion("wallet-strategy-race-v3.0.0")
                .coveragePct(100.0)
                .evidenceStatus("PASSED")
                .factPayloadLoaded(full && !guard)
                .generationActivatedAt(now.minusMinutes(5))
                .computedAt(now)
                .dataAsOf(now)
                .walletId("0xabc")
                .strategyCode("MOVEMENT_ALL")
                .scopeType("ALL")
                .scopeValue("ALL")
                .strategyKey("0xabc|MOVEMENT_ALL|ALL|ALL")
                .certificationStatus(full ? "CERTIFIED" : "CANDIDATE")
                .degradationState("ACTIVE")
                .allowNewEntries(full)
                .decisionFinal(full)
                .qualityFlags(List.of())
                .reasonCodes(List.of())
                .completeCycles(40)
                .historyDays(60)
                .dataFreshnessSeconds(1L)
                .coverage(MetricStrategySnapshotDto.CoverageDto.builder()
                        .status("COMPLETE")
                        .complete(true)
                        .completeCycles(40)
                        .factsReturned(40)
                        .factsAvailable(40)
                        .truncated(false)
                        .build())
                .unknownEconomicFields(List.of())
                .evaluationMode(full
                        ? MetricStrategySnapshotDto.EvaluationMode.FULL
                        : MetricStrategySnapshotDto.EvaluationMode.SUMMARY)
                .decisionUse(full ? "SHADOW" : "DISCOVERY_ONLY")
                .requiresFullSimulation(!full)
                .allowsMoney(false)
                .eligibleForShadow(full)
                .simulationMatrix(full && !guard
                        ? completeMatrix("0xabc|MOVEMENT_ALL|ALL|ALL", "gen-1")
                        : null)
                .financialMetrics(Map.of("netPnlUsd", Map.of("status", "UNKNOWN")))
                .scores(Map.of(
                        "raceScore", Map.of("value", 70),
                        "evidenceScore", Map.of("value", 45)))
                .operationalDecision(Map.of(
                        "operationalState", "ACTIVE",
                        "executionMode", "SHADOW",
                        "capitalMultiplier", 1,
                        "allowNewEntries", true,
                        "allowsMoney", false))
                .fieldAvailability(Map.of("netPnlUsd", Map.of("available", false)))
                .versions(Map.of(
                        "financialCalculatorVersion", "wallet-strategy-financial-v3.0.0",
                        "racePolicyVersion", "wallet-strategy-race-v3.0.0",
                        "sizingOwner", "ms-signals-orc/modules/copy-target-core"))
                .build();
        if (guard) result.setWindows(allWindows());
        return result;
    }

    private static Map<String, MetricStrategySnapshotDto.WindowDto> allWindows() {
        Map<String, MetricStrategySnapshotDto.WindowDto> result = new LinkedHashMap<>();
        for (String window : WINDOWS.split(",")) {
            result.put(window, MetricStrategySnapshotDto.WindowDto.builder()
                    .mature(true)
                    .complete(true)
                    .cycles(40)
                    .pnlNetUsd(10.0)
                    .coveragePct(100.0)
                    .reasonCodes(List.of())
                    .build());
        }
        return result;
    }

    private static final class FakeMetricClient implements MetricWalletsInfoClient {
        private List<MetricStrategySnapshotDto> summary = List.of();
        private List<MetricStrategySnapshotDto> full = List.of();
        private List<MetricStrategySnapshotDto> guard = List.of();
        private MetricStrategySnapshotDto exact;
        private CopyDecisionDto legacy;
        private int exactCalls;
        private int legacyCalls;

        @Override
        public List<MetricStrategySnapshotDto> metricStrategySnapshots(int limit, int dayz, String simulation) {
            return "full".equals(simulation) ? full : summary;
        }

        @Override
        public List<MetricStrategySnapshotDto> metricStrategyCopyGuardWindows(
                int limit,
                int dayz,
                String mode,
                String windows
        ) {
            return guard;
        }

        @Override
        public MetricStrategySnapshotDto metricStrategyDecision(
                String walletId,
                String strategyCode,
                String scopeType,
                String scopeValue,
                String mode,
                String simulation,
                int minHistoryDays,
                int simulationLookbackDays,
                int maxFactsPerUnit,
                int timeoutMs,
                boolean debug
        ) {
            exactCalls++;
            return exact;
        }

        @Override
        public CopyDecisionDto copyDecision(
                String walletId,
                String strategyCode,
                String scopeType,
                String scopeValue,
                String mode,
                String simulation,
                int minHistoryDays,
                int simulationLookbackDays,
                int maxFactsPerUnit,
                int timeoutMs,
                boolean debug
        ) {
            legacyCalls++;
            return legacy;
        }

        @Override
        public List<MetricaWalletDto> allPositionHistory(int limit, int dayz) {
            throw new AssertionError("V1 history endpoint must not be called");
        }

        @Override
        public List<MetricaWalletDto> joyas(int limit, int dayz, String simulation) {
            throw new AssertionError("V1 joyas endpoint must not be called");
        }

        @Override
        public List<CopyGuardWindowSnapshotDto> copyGuardWindows(
                int limit,
                int dayz,
                String mode,
                String windows
        ) {
            throw new AssertionError("V1 copy guard endpoint must not be called");
        }
    }
}
