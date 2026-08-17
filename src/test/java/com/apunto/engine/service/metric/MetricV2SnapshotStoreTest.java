package com.apunto.engine.service.metric;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.dto.client.CopyGuardWindowSnapshotDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.apunto.engine.dto.client.MetricStrategySnapshotTestFixtures.completeMatrix;
class MetricV2SnapshotStoreTest {

    private static final String WINDOWS = "1d,3d,1w,2w,3w,1mo,2mo,3mo,6mo,9mo,1y,2y,all";

    @Test
    void refreshPaginatesWalletsUntilEveryStrategyKeyIsLoaded() {
        FakeMetricClient client = new FakeMetricClient();
        List<MetricStrategySnapshotDto> summary = List.of(
                walletSnapshot(1, false, false),
                walletSnapshot(2, false, false),
                walletSnapshot(3, false, false));
        List<MetricStrategySnapshotDto> full = List.of(
                walletSnapshot(1, true, false),
                walletSnapshot(2, true, false),
                walletSnapshot(3, true, false));
        List<MetricStrategySnapshotDto> guard = List.of(
                walletSnapshot(1, true, true),
                walletSnapshot(2, true, true),
                walletSnapshot(3, true, true));
        stubRefresh(client, summary, full, guard);
        MetricV2SnapshotStore store = store(
                client, Duration.ofMinutes(10), new SimpleMeterRegistry(), 2, 2);

        store.refreshNow();

        assertEquals(3, store.snapshot().summaryByKey().size());
        assertEquals(0, store.snapshot().fullByKey().size());
        assertEquals(3, store.snapshot().guardByKey().size());
        assertEquals(List.of(0, 2), client.summaryOffsets);
        assertEquals(List.of(), client.fullOffsets);
        assertEquals(List.of(0, 2), client.guardOffsets);
    }

    @Test
    void fullAndGuardOfSameGenerationAllowOnlyTheExactStrategyScopeWithoutHotPathHttp() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summaryBtc = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT", false, false);
        MetricStrategySnapshotDto summaryEth = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "ETHUSDT", false, false);
        MetricStrategySnapshotDto fullBtc = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT", true, false);
        MetricStrategySnapshotDto fullEth = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "ETHUSDT", true, false);
        MetricStrategySnapshotDto guardBtc = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT", true, true);
        MetricStrategySnapshotDto guardEth = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "ETHUSDT", true, true);
        stubRefresh(client, List.of(summaryBtc, summaryEth), List.of(fullBtc, fullEth), List.of(guardBtc, guardEth));
        MetricV2SnapshotStore store = store(client, Duration.ofMinutes(10));

        store.refreshNow();
        store.recordExactFull(fullBtc);
        store.recordExactFull(fullEth);
        int callsAfterRefresh = client.calls;

        CopyStrategyGuardDecision btc = store.evaluate("0xabc", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT");
        CopyStrategyGuardDecision eth = store.evaluate("0xabc", "SYMBOL_SPECIALIST", "SYMBOL", "ETHUSDT");
        CopyStrategyGuardDecision other = store.evaluate("0xabc", "SYMBOL_SPECIALIST", "SYMBOL", "SOLUSDT");

        assertTrue(btc.allowed());
        assertTrue(eth.allowed());
        assertFalse(other.allowed());
        assertEquals(2, store.snapshot().fullByKey().size());
        assertEquals(callsAfterRefresh, client.calls, "the hot path must be cache-only");
    }

    @Test
    void staleOrUnknownEconomicDataBlocksNewExposure() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summary = snapshot("gen-1", "LONG_ONLY", "DIRECTION", "LONG", false, false);
        MetricStrategySnapshotDto full = snapshot("gen-1", "LONG_ONLY", "DIRECTION", "LONG", true, false);
        MetricStrategySnapshotDto guard = snapshot("gen-1", "LONG_ONLY", "DIRECTION", "LONG", true, true);
        full.setUnknownEconomicFields(List.of("FUNDING_USD"));
        stubRefresh(client, List.of(summary), List.of(full), List.of(guard));
        MetricV2SnapshotStore store = store(client, Duration.ofMinutes(10));

        store.refreshNow();
        store.recordExactFull(full);

        assertFalse(store.evaluate("0xabc", "LONG_ONLY", "DIRECTION", "LONG").allowed());

        FakeMetricClient staleClient = new FakeMetricClient();
        MetricStrategySnapshotDto staleSummary = snapshot("gen-1", "SHORT_ONLY", "DIRECTION", "SHORT", false, false);
        MetricStrategySnapshotDto staleFull = snapshot("gen-1", "SHORT_ONLY", "DIRECTION", "SHORT", true, false);
        MetricStrategySnapshotDto staleGuard = snapshot("gen-1", "SHORT_ONLY", "DIRECTION", "SHORT", true, true);
        OffsetDateTime staleAt = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        staleFull.setDataAsOf(staleAt);
        staleGuard.setDataAsOf(staleAt);
        stubRefresh(staleClient, List.of(staleSummary), List.of(staleFull), List.of(staleGuard));
        MetricV2SnapshotStore staleStore = store(staleClient, Duration.ofMinutes(10));

        staleStore.refreshNow();
        staleStore.recordExactFull(staleFull);

        assertFalse(staleStore.evaluate("0xabc", "SHORT_ONLY", "DIRECTION", "SHORT").allowed());
    }

    @Test
    void missingSimulationMatrixIsRejectedBeforeEnteringTheExactCache() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summary = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        MetricStrategySnapshotDto full = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, false);
        MetricStrategySnapshotDto guard = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, true);
        full.setSimulationMatrix(null);
        stubRefresh(client, List.of(summary), List.of(full), List.of(guard));
        MetricV2SnapshotStore store = store(client, Duration.ofMinutes(10));

        store.refreshNow();
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> store.recordExactFull(full)
        );

        assertTrue(error.getMessage().contains("SIMULATION_MATRIX_REQUIRED"));
        assertTrue(store.snapshot().fullByKey().isEmpty());
    }

    @Test
    void generationMismatchRejectsRefreshAtomicallyAndRetainsPreviousSnapshot() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summaryV1 = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        MetricStrategySnapshotDto fullV1 = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, false);
        MetricStrategySnapshotDto guardV1 = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, true);
        stubRefresh(client, List.of(summaryV1), List.of(fullV1), List.of(guardV1));
        MetricV2SnapshotStore store = store(client, Duration.ofMinutes(10));
        store.refreshNow();
        store.recordExactFull(fullV1);

        MetricStrategySnapshotDto summaryV2 = snapshot("gen-2", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        MetricStrategySnapshotDto fullV2 = snapshot("gen-2", "MOVEMENT_ALL", "ALL", "ALL", true, false);
        MetricStrategySnapshotDto guardWrong = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, true);
        stubRefresh(client, List.of(summaryV2), List.of(fullV2), List.of(guardWrong));

        assertThrows(IllegalStateException.class, store::refreshNow);
        assertEquals("gen-1", store.snapshot().fullByKey().values().iterator().next().getGenerationId());
        assertTrue(store.evaluate("0xabc", "MOVEMENT_ALL", "ALL", "ALL").allowed());
    }

    @Test
    void successfulGenerationChangeEmitsCanonicalAndLegacyCounters() {
        FakeMetricClient client = new FakeMetricClient();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        stubRefresh(client,
                List.of(snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false)),
                List.of(snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, false)),
                List.of(snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, true)));
        MetricV2SnapshotStore store = store(client, Duration.ofMinutes(10), registry);
        store.refreshNow();

        stubRefresh(client,
                List.of(snapshot("gen-2", "MOVEMENT_ALL", "ALL", "ALL", false, false)),
                List.of(snapshot("gen-2", "MOVEMENT_ALL", "ALL", "ALL", true, false)),
                List.of(snapshot("gen-2", "MOVEMENT_ALL", "ALL", "ALL", true, true)));
        store.refreshNow();

        assertEquals(1.0, registry.get("signals.metric_generation.change.total").counter().count());
        assertEquals(1.0, registry.get("signals.metric_v2.generation.change.total").counter().count());
    }

    @Test
    void incompleteFullCoverageFailsClosedAndEmitsCoverageRejectionMetric() {
        FakeMetricClient client = new FakeMetricClient();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricStrategySnapshotDto summary = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        MetricStrategySnapshotDto full = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, false);
        MetricStrategySnapshotDto guard = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, true);
        full.getCoverage().setStatus("INCOMPLETE");
        full.getCoverage().setComplete(false);
        stubRefresh(client, List.of(summary), List.of(full), List.of(guard));
        MetricV2SnapshotStore store = store(client, Duration.ofMinutes(10), registry);

        store.refreshNow();
        store.recordExactFull(full);

        assertFalse(store.evaluate("0xabc", "MOVEMENT_ALL", "ALL", "ALL").allowed());
        assertEquals(1.0, registry.get("signals.metric.v2.rejected.coverage.total").counter().count());
    }

    @Test
    void missingGenerationAndSummarySemanticsFailClosed() {
        MetricStrategySnapshotDto summary = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        assertFalse(summary.isEligibleForShadow());
        assertFalse(summary.isAllowNewEntries());
        assertFalse(summary.isDecisionFinal());

        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto invalid = snapshot(null, "MOVEMENT_ALL", "ALL", "ALL", true, false);
        MetricStrategySnapshotDto guard = snapshot(null, "MOVEMENT_ALL", "ALL", "ALL", true, true);
        stubRefresh(client, List.of(summary), List.of(invalid), List.of(guard));

        assertThrows(IllegalStateException.class, () -> store(client, Duration.ofMinutes(10)).refreshNow());
    }

    @Test
    void fullStrategyMissingFromSummaryRejectsAtomicRefresh() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summary = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        MetricStrategySnapshotDto full = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT", true, false);
        MetricStrategySnapshotDto guard = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT", true, true);
        stubRefresh(client, List.of(summary), List.of(full), List.of(guard));

        assertThrows(IllegalStateException.class, () -> store(client, Duration.ofMinutes(10)).refreshNow());
    }

    @Test
    void copyGuardWithExtraStrategyRejectsAtomicRefresh() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summary = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        MetricStrategySnapshotDto full = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, false);
        MetricStrategySnapshotDto guard = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, true);
        MetricStrategySnapshotDto extraGuard = snapshot("gen-1", "LONG_ONLY", "DIRECTION", "LONG", true, true);
        stubRefresh(client, List.of(summary), List.of(full), List.of(guard, extraGuard));

        assertThrows(IllegalStateException.class, () -> store(client, Duration.ofMinutes(10)).refreshNow());
    }

    @Test
    void futureDatedFullAndGuardBlockNewExposure() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summary = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        MetricStrategySnapshotDto full = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, false);
        MetricStrategySnapshotDto guard = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, true);
        OffsetDateTime future = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);
        full.setDataAsOf(future);
        guard.setDataAsOf(future);
        stubRefresh(client, List.of(summary), List.of(full), List.of(guard));
        MetricV2SnapshotStore store = store(client, Duration.ofMinutes(10));

        store.refreshNow();
        store.recordExactFull(full);

        assertFalse(store.evaluate("0xabc", "MOVEMENT_ALL", "ALL", "ALL").allowed());
    }

    @Test
    void changedCandidateSetWithinSameGenerationRefreshesFullAndGuardTogether() {
        FakeMetricClient client = new FakeMetricClient();
        MetricStrategySnapshotDto summaryMovement = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", false, false);
        MetricStrategySnapshotDto fullMovement = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, false);
        MetricStrategySnapshotDto guardMovement = snapshot("gen-1", "MOVEMENT_ALL", "ALL", "ALL", true, true);
        stubRefresh(client, List.of(summaryMovement), List.of(fullMovement), List.of(guardMovement));
        MetricV2SnapshotStore store = store(client, Duration.ofMinutes(10));
        store.refreshNow();
        store.recordExactFull(fullMovement);

        MetricStrategySnapshotDto summaryBtc = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT", false, false);
        MetricStrategySnapshotDto fullBtc = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT", true, false);
        MetricStrategySnapshotDto guardBtc = snapshot("gen-1", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT", true, true);
        stubRefresh(client, List.of(summaryBtc), List.of(fullBtc), List.of(guardBtc));

        store.refreshNow();
        store.recordExactFull(fullBtc);

        assertTrue(store.evaluate("0xabc", "SYMBOL_SPECIALIST", "SYMBOL", "BTCUSDT").allowed());
        assertFalse(store.evaluate("0xabc", "MOVEMENT_ALL", "ALL", "ALL").allowed());
    }

    private static MetricV2SnapshotStore store(MetricWalletsInfoClient client, Duration maxStaleness) {
        return store(client, maxStaleness, new SimpleMeterRegistry());
    }

    private static MetricV2SnapshotStore store(MetricWalletsInfoClient client,
                                                Duration maxStaleness,
                                                SimpleMeterRegistry registry) {
        return store(client, maxStaleness, registry, 100, 20);
    }

    private static MetricV2SnapshotStore store(MetricWalletsInfoClient client,
                                                Duration maxStaleness,
                                                SimpleMeterRegistry registry,
                                                int summaryLimit,
                                                int fullLimit) {
        return new MetricV2SnapshotStore(
                client,
                new MetricStrategyShadowProjectionMapper(),
                new MetricWalletReadModeResolver("V2"),
                registry,
                summaryLimit,
                fullLimit,
                30,
                Duration.ofMinutes(10),
                Duration.ofMinutes(2),
                maxStaleness,
                false,
                true,
                WINDOWS
        );
    }

    private static void stubRefresh(
            FakeMetricClient client,
            List<MetricStrategySnapshotDto> summary,
            List<MetricStrategySnapshotDto> full,
            List<MetricStrategySnapshotDto> guard
    ) {
        client.summary = summary;
        client.full = full;
        client.guard = guard;
    }

    private static MetricStrategySnapshotDto snapshot(
            String generation,
            String strategy,
            String scopeType,
            String scopeValue,
            boolean full,
            boolean guard
    ) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(1);
        String key = MetricStrategySnapshotDto.canonicalStrategyKey("0xabc", strategy, scopeType, scopeValue);
        MetricStrategySnapshotDto result = MetricStrategySnapshotDto.builder()
                .metricVersion(2)
                .sourceVersion(MetricStrategySnapshotDto.SOURCE_VERSION)
                .generationId(generation)
                .generationStatus("ACTIVE")
                .readMode("V2")
                .responseSource(MetricStrategySnapshotDto.RESPONSE_SOURCE)
                .calculatorVersion("wallet-strategy-financial-v3.0.0")
                .policyVersion("wallet-strategy-race-v3.0.0")
                .coveragePct(100.0)
                .evidenceStatus("PASSED")
                .factPayloadLoaded(full && !guard)
                .simulationExecuted(full && !guard)
                .summaryMode(!full)
                .fullMaterialized(full && !guard)
                .requestedMode(full && !guard ? "micro-live-entry" : null)
                .evaluatedMode(full && !guard ? "micro-live-entry" : null)
                .generationActivatedAt(now.minusMinutes(5))
                .computedAt(now)
                .dataAsOf(now)
                .walletId("0xabc")
                .strategyCode(strategy)
                .scopeType(scopeType)
                .scopeValue(scopeValue)
                .strategyKey(key)
                .certificationStatus(full ? "CERTIFIED" : "CANDIDATE")
                .degradationState("ACTIVE")
                .allowNewEntries(full && !guard)
                .decisionFinal(full && !guard)
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
                .requiresFullSimulation(!full || guard)
                .allowsMoney(false)
                .eligibleForShadow(full && !guard)
                .rankWithinStrategy(1)
                .globalRank(1)
                .simulation(full ? Map.of("copyNetPnlUsd", 10.0) : null)
                .financialMetrics(full && !guard
                        ? Map.of("capacityUsd", Map.of("value", 100.0))
                        : null)
                .scores(full && !guard
                        ? Map.of("evidenceScore", Map.of("value", 100.0))
                        : null)
                .operationalDecision(full && !guard
                        ? Map.of(
                                "operationalState", "CANDIDATE",
                                "allowNewEntries", true,
                                "capitalMultiplier", 1)
                        : null)
                .fieldAvailability(full && !guard
                        ? Map.of("executionFacts", Map.of("available", true))
                        : null)
                .versions(full && !guard
                        ? Map.of(
                                "metricVersion", 2,
                                "sourceVersion", MetricStrategySnapshotDto.SOURCE_VERSION)
                        : null)
                .build();
        if (guard) result.setWindows(allWindows());
        if (full && !guard) result.setSimulationMatrix(completeMatrix(key, generation));
        return result;
    }

    private static MetricStrategySnapshotDto walletSnapshot(
            int walletNumber,
            boolean full,
            boolean guard
    ) {
        MetricStrategySnapshotDto result = snapshot(
                "gen-" + walletNumber, "MOVEMENT_ALL", "ALL", "ALL", full, guard);
        String wallet = String.format("0x%040x", walletNumber);
        result.setWalletId(wallet);
        result.setStrategyKey(wallet + "|MOVEMENT_ALL|ALL|ALL");
        return result;
    }

    private static Map<String, MetricStrategySnapshotDto.WindowDto> allWindows() {
        Map<String, MetricStrategySnapshotDto.WindowDto> values = new LinkedHashMap<>();
        for (String window : WINDOWS.split(",")) {
            values.put(window, MetricStrategySnapshotDto.WindowDto.builder()
                    .days("all".equals(window) ? null : 1)
                    .mature(true)
                    .complete(true)
                    .cycles(40)
                    .pnlNetUsd(10.0)
                    .coveragePct(100.0)
                    .reasonCodes(List.of())
                    .build());
        }
        return values;
    }

    private static final class FakeMetricClient implements MetricWalletsInfoClient {
        private List<MetricStrategySnapshotDto> summary = List.of();
        private List<MetricStrategySnapshotDto> full = List.of();
        private List<MetricStrategySnapshotDto> guard = List.of();
        private final List<Integer> summaryOffsets = new java.util.ArrayList<>();
        private final List<Integer> fullOffsets = new java.util.ArrayList<>();
        private final List<Integer> guardOffsets = new java.util.ArrayList<>();
        private int calls;

        @Override
        public List<MetricStrategySnapshotDto> metricStrategySnapshots(int limit, int dayz, String simulation) {
            calls++;
            return "full".equals(simulation) ? full : summary;
        }

        @Override
        public List<MetricStrategySnapshotDto> metricStrategySnapshotsPage(
                int limit, int offsetWallet, int dayz, String simulation) {
            calls++;
            ("full".equals(simulation) ? fullOffsets : summaryOffsets).add(offsetWallet);
            return page("full".equals(simulation) ? full : summary, limit, offsetWallet);
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
        public List<MetricStrategySnapshotDto> metricStrategyCopyGuardWindowsPage(
                int limit,
                int offsetWallet,
                int dayz,
                String mode,
                String windows
        ) {
            calls++;
            guardOffsets.add(offsetWallet);
            return page(guard, limit, offsetWallet);
        }

        private static List<MetricStrategySnapshotDto> page(
                List<MetricStrategySnapshotDto> source,
                int limit,
                int offsetWallet
        ) {
            List<String> wallets = new java.util.ArrayList<>(source.stream()
                    .map(MetricStrategySnapshotDto::getWalletId)
                    .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new)));
            if (offsetWallet >= wallets.size()) return List.of();
            int end = Math.min(wallets.size(), offsetWallet + limit);
            java.util.Set<String> selected = new LinkedHashSet<>(wallets.subList(offsetWallet, end));
            return source.stream().filter(item -> selected.contains(item.getWalletId())).toList();
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
        public List<CopyGuardWindowSnapshotDto> copyGuardWindows(
                int limit,
                int dayz,
                String mode,
                String windows
        ) {
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
            throw new AssertionError("V1 endpoint must not be called");
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
