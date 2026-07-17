package com.apunto.engine.service.metric;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.dto.client.CopyGuardWindowSnapshotDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static com.apunto.engine.dto.client.MetricStrategySnapshotTestFixtures.completeMatrix;

class MetricV2SnapshotPerformanceTest {

    private static final String WINDOWS = "1d,3d,1w,2w,3w,1mo,2mo,3mo,6mo,9mo,1y,2y,all";

    @Test
    void measuresCacheOnlyGuardLookupWithoutRemoteCalls() {
        assumeTrue(Boolean.getBoolean("metric.v2.benchmark.enabled"),
                "set -Dmetric.v2.benchmark.enabled=true to run the local CPU benchmark");

        FakeMetricClient client = new FakeMetricClient();
        client.summary = List.of(snapshot(false, false));
        client.full = List.of(snapshot(true, false));
        client.guard = List.of(snapshot(true, true));
        MetricV2SnapshotStore store = new MetricV2SnapshotStore(
                client,
                new MetricStrategyShadowProjectionMapper(),
                new MetricWalletReadModeResolver("V2"),
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
        store.refreshNow();
        int callsAfterRefresh = client.calls;

        for (int i = 0; i < 10_000; i++) store.evaluate("0xabc", "MOVEMENT_ALL", "ALL", "ALL");

        int batches = 100;
        int operationsPerBatch = 1_000;
        int allowed = 0;
        double[] microsPerOperation = new double[batches];
        for (int batch = 0; batch < batches; batch++) {
            long started = System.nanoTime();
            for (int operation = 0; operation < operationsPerBatch; operation++) {
                if (store.evaluate("0xabc", "MOVEMENT_ALL", "ALL", "ALL").allowed()) allowed++;
            }
            microsPerOperation[batch] = (System.nanoTime() - started) / 1_000.0 / operationsPerBatch;
        }
        Arrays.sort(microsPerOperation);

        assertEquals(batches * operationsPerBatch, allowed);
        assertEquals(callsAfterRefresh, client.calls, "hot path must remain HTTP-free");
        assertEquals(3, callsAfterRefresh, "one coordinated refresh uses summary, full and guard once");
        System.out.printf(Locale.ROOT,
                "PERF_METRIC_V2_HOT_PATH batches=%d operations=%d remoteCallsDuringHotPath=0 minUs=%.4f p50Us=%.4f p95Us=%.4f maxUs=%.4f%n",
                batches,
                batches * operationsPerBatch,
                microsPerOperation[0],
                percentile(microsPerOperation, 0.50),
                percentile(microsPerOperation, 0.95),
                microsPerOperation[microsPerOperation.length - 1]
        );
    }

    private static double percentile(double[] sorted, double percentile) {
        return sorted[(int) Math.floor((sorted.length - 1) * percentile)];
    }

    private static MetricStrategySnapshotDto snapshot(boolean full, boolean guard) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(1);
        MetricStrategySnapshotDto result = MetricStrategySnapshotDto.builder()
                .metricVersion(2)
                .sourceVersion(MetricStrategySnapshotDto.SOURCE_VERSION)
                .generationId("benchmark-generation")
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
                .build();
        if (guard) result.setWindows(allWindows());
        if (full && !guard) {
            result.setSimulationMatrix(completeMatrix(
                    "0xabc|MOVEMENT_ALL|ALL|ALL", "benchmark-generation"));
        }
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
        private int calls;

        @Override
        public List<MetricStrategySnapshotDto> metricStrategySnapshots(int limit, int dayz, String simulation) {
            calls++;
            return "full".equals(simulation) ? full : summary;
        }

        @Override
        public List<MetricStrategySnapshotDto> metricStrategyCopyGuardWindows(
                int limit,
                int dayz,
                String mode,
                String windows
        ) {
            calls++;
            return guard;
        }

        @Override
        public List<MetricaWalletDto> allPositionHistory(int limit, int dayz) {
            throw new AssertionError("V1 endpoint must not be called");
        }

        @Override
        public List<MetricaWalletDto> joyas(int limit, int dayz, String simulation) {
            throw new AssertionError("V1 endpoint must not be called");
        }

        @Override
        public List<CopyGuardWindowSnapshotDto> copyGuardWindows(int limit, int dayz, String mode, String windows) {
            throw new AssertionError("V1 endpoint must not be called");
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
            throw new AssertionError("exact endpoint is not part of refresh");
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
            throw new AssertionError("exact endpoint is not part of refresh");
        }
    }
}
