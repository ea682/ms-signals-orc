package com.apunto.engine.service.impl;
import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.dto.client.CopyGuardWindowSnapshotDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.service.annotation.GetExchange;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MetricWalletServiceImplTest {

    @Test
    void cachedCandidatesNeverTriggerMetricHttpOrAllocationDatabaseLoad() throws Exception {
        UUID userId = UUID.randomUUID();
        CapturingMetricClient client = new CapturingMetricClient(List.of(metricForHistory("0xabc")));
        CacheOnlyAllocationService allocations = new CacheOnlyAllocationService(UserCopyAllocationEntity.builder()
                .id(7L)
                .idUser(userId)
                .walletId("0xabc")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("strategy")
                .scopeValue("MOVEMENT_ALL")
                .allocationPct(new BigDecimal("0.25"))
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .build());
        MetricWalletServiceImpl service = service(client, allocations, 30, 45, "summary", false);
        loadSnapshot(service, 300, 30);

        List<MetricaWalletDto> result = service.getCandidatesForUserWalletCachedOnly(userId, "0xabc");

        assertEquals(1, result.size());
        assertEquals(1, client.joyasCalls, "the hot path must not perform another HTTP request");
        assertEquals(0, allocations.databaseLoads);
        assertEquals(1, allocations.cacheOnlyLoads);
    }

    @Test
    void cachedCandidatesFailClosedWithoutPrimedHistory() {
        UUID userId = UUID.randomUUID();
        CapturingMetricClient client = new CapturingMetricClient(List.of(metricForHistory("0xabc")));
        CacheOnlyAllocationService allocations = new CacheOnlyAllocationService(UserCopyAllocationEntity.builder()
                .id(8L)
                .idUser(userId)
                .walletId("0xabc")
                .copyStrategyCode("MOVEMENT_ALL")
                .allocationPct(new BigDecimal("0.25"))
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .build());
        MetricWalletServiceImpl service = service(client, allocations, 30, 45, "summary", false);

        assertTrue(service.getCandidatesForUserWalletCachedOnly(userId, "0xabc").isEmpty());
        assertEquals(0, client.joyasCalls);
        assertEquals(0, allocations.databaseLoads);
    }

    @Test
    void metricWalletClientUsesCanonicalJoyasEndpointPath() throws Exception {
        Method method = MetricWalletsInfoClient.class.getMethod("joyas", int.class, int.class, String.class);
        GetExchange exchange = method.getAnnotation(GetExchange.class);

        assertNotNull(exchange);
        assertEquals("/operaciones/metrica/joyas", exchange.value());
    }

    @Test
    void metricWalletClientUsesCopyGuardWindowSnapshotEndpointPath() throws Exception {
        Method method = MetricWalletsInfoClient.class.getMethod(
                "copyGuardWindows",
                int.class,
                int.class,
                String.class,
                String.class
        );
        GetExchange exchange = method.getAnnotation(GetExchange.class);

        assertNotNull(exchange);
        assertEquals("/operaciones/metrica/copy-guard/windows", exchange.value());
    }

    @Test
    void joyasHistoryRequestUsesConfiguredLimitDayzAndSimulationParams() throws Exception {
        CapturingMetricClient client = new CapturingMetricClient(List.of(metricForHistory("0xabc")));
        MetricWalletServiceImpl service = service(client, 30, 45, "summary");

        Object snapshot = loadSnapshot(service, 300, 30);

        assertEquals(30, client.lastLimit);
        assertEquals(45, client.lastDayz);
        assertEquals("summary", client.lastSimulation);
        assertEquals(1, snapshotSize(snapshot));
    }

    @Test
    void joyasDiscoveryCacheForcesSummaryEvenIfFullWasConfigured() throws Exception {
        CapturingMetricClient client = new CapturingMetricClient(List.of(metricForHistory("0xabc")));
        MetricWalletServiceImpl service = service(client, 30, 45, "full");

        loadSnapshot(service, 300, 30);

        assertEquals("summary", client.lastSimulation);
    }

    @Test
    void emptyJoyasResponseReturnsEmptySnapshotWithoutCallingLegacyEndpoint() throws Exception {
        CapturingMetricClient client = new CapturingMetricClient(List.of());
        MetricWalletServiceImpl service = service(client, 30, 45, "summary");

        Object snapshot = loadSnapshot(service, 300, 30);

        assertEquals(1, client.joyasCalls);
        assertEquals(0, client.allPositionHistoryCalls);
        assertEquals(0, snapshotSize(snapshot));
    }

    @Test
    void joyasEndpoint404ReturnsEmptySnapshotWithoutMixingItWithSuccessfulEmptyResponse() throws Exception {
        CapturingMetricClient client = new CapturingMetricClient(List.of());
        client.throwNotFound = true;
        MetricWalletServiceImpl service = service(client, 30, 45, "summary");

        Object snapshot = loadSnapshot(service, 300, 30);

        assertEquals(1, client.joyasCalls);
        assertEquals(0, client.allPositionHistoryCalls);
        assertEquals(0, snapshotSize(snapshot));
    }

    @Test
    void realJewelReduceCapitalGuardAllowsCopyWithMultiplier() throws Exception {
        MetricWalletServiceImpl service = service();
        MetricaWalletDto metric = metricWithGuard("REDUCE_CAPITAL", "REDUCE_CAPITAL", true, 0.35);

        CopyStrategyGuardDecision decision = evaluate(service, metric);

        assertTrue(decision.allowed());
        assertEquals("REDUCE_CAPITAL", decision.action());
        assertEquals(0.35, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void realJewelPauseOpenGuardBlocksNewEntriesButKeepsExitStatus() throws Exception {
        MetricWalletServiceImpl service = service();
        MetricaWalletDto metric = metricWithGuard("PAUSE_OPEN", "PAUSE_OPEN", false, 0.25);

        CopyStrategyGuardDecision decision = evaluate(service, metric);

        assertFalse(decision.allowed());
        assertEquals("PAUSE_OPEN", decision.action());
        assertEquals("PAUSED_BY_RISK", decision.statusWhenBlocked());
        assertEquals(0.0, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void oneWeekNegativeWindowWarnsWithoutReducingOrPausing() throws Exception {
        MetricWalletServiceImpl service = service();
        MetricaWalletDto metric = MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder().idWallet("0xabc").build())
                .strategy(MetricaWalletDto.StrategyDto.builder().strategyCode("MOVEMENT_ALL").build())
                .copySimulation(MetricaWalletDto.CopySimulationDto.builder()
                        .pnlCopyTotalNetUSDT(10.0)
                        .pnlCopyNet(java.util.Map.of("1w", -1.0, "2w", 3.0, "1mo", 4.0))
                        .build())
                .build();

        CopyStrategyGuardDecision decision = evaluate(service, metric);

        assertTrue(decision.allowed());
        assertEquals("WARNING", decision.action());
        assertEquals(1.0, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void twoWeekNegativeWindowReducesCapitalTo60Percent() throws Exception {
        MetricWalletServiceImpl service = service();
        MetricaWalletDto metric = MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder().idWallet("0xabc").build())
                .strategy(MetricaWalletDto.StrategyDto.builder().strategyCode("MOVEMENT_ALL").build())
                .copySimulation(MetricaWalletDto.CopySimulationDto.builder()
                        .pnlCopyTotalNetUSDT(10.0)
                        .pnlCopyNet(java.util.Map.of("1w", 1.0, "2w", -1.0, "1mo", 3.0))
                        .build())
                .build();

        CopyStrategyGuardDecision decision = evaluate(service, metric);

        assertTrue(decision.allowed());
        assertEquals("REDUCE_CAPITAL", decision.action());
        assertEquals("NEGATIVE_2W_REDUCE_TO_60", decision.reason());
        assertEquals(0.60, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void metricCopyGuardAllowDoesNotBypassNegativeRequiredOneMonthWindow() throws Exception {
        MetricWalletServiceImpl service = service();
        MetricaWalletDto.CopyGuardDto allowGuard = MetricaWalletDto.CopyGuardDto.builder()
                .status("OK")
                .action("ALLOW")
                .allowNewEntries(true)
                .allowReductions(true)
                .allowCloses(true)
                .capitalMultiplier(1.0)
                .build();
        MetricaWalletDto metric = MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder().idWallet("0xabc").build())
                .strategy(MetricaWalletDto.StrategyDto.builder().strategyCode("MOVEMENT_ALL").build())
                .realJewel(MetricaWalletDto.RealJewelDto.builder()
                        .status("OK")
                        .copyGuard(allowGuard)
                        .build())
                .copySimulation(MetricaWalletDto.CopySimulationDto.builder()
                        .pnlCopyTotalNetUSDT(10.0)
                        .pnlCopyNet(Map.of("1w", 1.0, "2w", 2.0, "1mo", -1.0))
                        .build())
                .build();

        CopyStrategyGuardDecision decision = evaluate(service, metric);

        assertFalse(decision.allowed());
        assertEquals("SHADOW_REVALIDATION", decision.action());
        assertEquals("NEGATIVE_1MO_SHADOW_REVALIDATION", decision.reason());
    }

    @Test
    void incompleteRequiredWindowIsInformationalOnly() throws Exception {
        MetricWalletServiceImpl service = service();
        MetricaWalletDto metric = MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder().idWallet("0xabc").build())
                .strategy(MetricaWalletDto.StrategyDto.builder().strategyCode("MOVEMENT_ALL").build())
                .copySimulation(MetricaWalletDto.CopySimulationDto.builder()
                        .pnlCopyTotalNetUSDT(10.0)
                        .pnlCopyNet(Map.of("1w", 1.0, "2w", 2.0, "1mo", 3.0))
                        .windowMeta(Map.<String, Object>of(
                                "1w", Map.of("complete", true),
                                "2w", Map.of("complete", true),
                                "1mo", Map.of("complete", false)
                        ))
                        .build())
                .build();

        CopyStrategyGuardDecision decision = evaluate(service, metric);

        assertTrue(decision.allowed());
        assertEquals("ALLOW", decision.action());
    }

    @Test
    void simulationAuditFailureBlocksLiveOpenings() throws Exception {
        MetricWalletServiceImpl service = service();
        MetricaWalletDto metric = MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder().idWallet("0xabc").build())
                .strategy(MetricaWalletDto.StrategyDto.builder().strategyCode("MOVEMENT_ALL").build())
                .copySimulation(MetricaWalletDto.CopySimulationDto.builder()
                        .pnlCopyTotalNetUSDT(10.0)
                        .pnlCopyNet(Map.of("1w", 1.0, "2w", 2.0, "1mo", 3.0))
                        .simulationAudit(Map.of(
                                "valid", false,
                                "errors", List.of("WINDOW_SUM_RECONCILIATION_FAILED")
                        ))
                        .build())
                .build();

        CopyStrategyGuardDecision decision = evaluate(service, metric);

        assertFalse(decision.allowed());
        assertEquals("SIMULATION_AUDIT_FAILED", decision.reason());
        assertEquals("PAUSE_OPEN", decision.action());
    }

    @Test
    void shouldUseSnapshotWindowsForCopyGuard() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 4.0, "ALLOW", "OK"),
                "2w", window("2w", -1.0, "REDUCE_CAPITAL", "NEGATIVE_2W_REDUCE_TO_60"),
                "1mo", window("1mo", 10.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("REDUCE_CAPITAL", decision.action());
        assertEquals("NEGATIVE_2W_REDUCE_TO_60", decision.reason());
        assertEquals(0.60, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void shouldNotReduceCapitalWhenSnapshotOneWeekNegative() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", -1.0, "WARNING", "NEGATIVE_1W_WARNING_ONLY"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "1mo", window("1mo", 3.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("WARNING", decision.action());
        assertEquals(1.0, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void shouldPauseOpenWhenSnapshotOneWeekSevereLoss() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", severeWindow("1w", -25.0, "PAUSE_OPEN", "SEVERE_NEGATIVE_1W_PAUSE_OPEN", -0.25, 100.0, "PAUSE_OPEN"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "1mo", window("1mo", 3.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertFalse(decision.allowed());
        assertEquals("PAUSE_OPEN", decision.action());
        assertEquals("SEVERE_NEGATIVE_1W_PAUSE_OPEN", decision.reason());
        assertEquals(0.0, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void shouldShadowRevalidationWhenSnapshotOneWeekBrutalLoss() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", severeWindow("1w", -45.0, "SHADOW_REVALIDATION", "BRUTAL_NEGATIVE_1W_SHADOW_REVALIDATION", -0.45, 100.0, "SHADOW_REVALIDATION"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "1mo", window("1mo", 3.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertFalse(decision.allowed());
        assertEquals("SHADOW_REVALIDATION", decision.action());
        assertEquals("BRUTAL_NEGATIVE_1W_SHADOW_REVALIDATION", decision.reason());
        assertEquals("SHADOW", decision.targetExecutionMode());
    }

    @Test
    void shouldFindSanitizedSnapshotWindowCodes() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w\"", severeWindow("1w\"", -25.0, "PAUSE_OPEN", "SEVERE_NEGATIVE_1W_PAUSE_OPEN", -0.25, 100.0, "PAUSE_OPEN"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "1mo", window("1mo", 3.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertFalse(decision.allowed());
        assertEquals("PAUSE_OPEN", decision.action());
        assertEquals("SEVERE_NEGATIVE_1W_PAUSE_OPEN", decision.reason());
    }

    @Test
    void shouldMoveToShadowRevalidationWhenSnapshotOneMonthNegative() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "1mo", window("1mo", -3.0, "SHADOW_REVALIDATION", "NEGATIVE_1MO_SHADOW_REVALIDATION")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertFalse(decision.allowed());
        assertEquals("SHADOW_REVALIDATION", decision.action());
        assertEquals("NEGATIVE_1MO_SHADOW_REVALIDATION", decision.reason());
    }

    @Test
    void shouldReduceTo30PercentWhenSnapshotThreeWeekNegative() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", -2.0, "REDUCE_CAPITAL", "NEGATIVE_2W_REDUCE_TO_60"),
                "3w", window("3w", -3.0, "REDUCE_CAPITAL", "NEGATIVE_3W_REDUCE_TO_30"),
                "1mo", window("1mo", 4.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("REDUCE_CAPITAL", decision.action());
        assertEquals("NEGATIVE_3W_REDUCE_TO_30", decision.reason());
        assertEquals(0.30, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void shouldRequireMicroLiveAgainWhenSnapshotTwoMonthNegative() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "3w", window("3w", 3.0, "ALLOW", "OK"),
                "1mo", window("1mo", 4.0, "ALLOW", "OK"),
                "2mo", window("2mo", -5.0, "MICRO_LIVE_REQUIRED_REENTRY", "NEGATIVE_2MO_HARD_DOWNGRADE")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertFalse(decision.allowed());
        assertEquals("MICRO_LIVE_REQUIRED_REENTRY", decision.action());
        assertEquals("MICRO_LIVE", decision.targetExecutionMode());
        assertEquals("NEGATIVE_2MO_HARD_DOWNGRADE", decision.reason());
    }

    @Test
    void shouldManualReviewWhenSnapshotThreeMonthNegative() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "3w", window("3w", 3.0, "ALLOW", "OK"),
                "1mo", window("1mo", 4.0, "ALLOW", "OK"),
                "2mo", window("2mo", 5.0, "ALLOW", "OK"),
                "3mo", window("3mo", -6.0, "MANUAL_REVIEW", "NEGATIVE_3MO_MANUAL_REVIEW")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertFalse(decision.allowed());
        assertEquals("MANUAL_REVIEW", decision.action());
        assertEquals("NEGATIVE_3MO_MANUAL_REVIEW", decision.reason());
    }

    @Test
    void shouldAllowWhenAllSnapshotWindowsPositive() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "1mo", window("1mo", 3.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("ALLOW", decision.action());
    }

    @Test
    void shouldNotPauseWhenRequiredSnapshotWindowIsMissing() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "1mo", window("1mo", 3.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("ALLOW", decision.action());
    }

    @Test
    void shouldNotPauseBecauseTwoMonthAndThreeMonthAreIncomplete() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "3w", window("3w", 3.0, "ALLOW", "OK"),
                "1mo", window("1mo", 4.0, "ALLOW", "OK"),
                "2mo", informationalWindow("2mo", "WINDOW_2MO_NOT_MATURE_INFO", true),
                "3mo", informationalWindow("3mo", "WINDOW_3MO_NOT_MATURE_INFO", true)
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("ALLOW", decision.action());
    }

    @Test
    void shouldExposeFutureWindowsAsInformationalOnly() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "3w", window("3w", 3.0, "ALLOW", "OK"),
                "1mo", window("1mo", 4.0, "ALLOW", "OK"),
                "6mo", informationalWindow("6mo", "WINDOW_6MO_NEGATIVE_INFORMATIONAL", false),
                "9mo", informationalWindow("9mo", "WINDOW_9MO_NEGATIVE_INFORMATIONAL", false),
                "1y", informationalWindow("1y", "WINDOW_1Y_NEGATIVE_INFORMATIONAL", false),
                "2y", informationalWindow("2y", "WINDOW_2Y_NEGATIVE_INFORMATIONAL", false)
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("ALLOW", decision.action());
    }

    @Test
    void shouldNotPromoteNewWalletBeforeOneMonthFromSnapshotHeader() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "3w", window("3w", 3.0, "ALLOW", "OK"),
                "1mo", window("1mo", 4.0, "ALLOW", "OK")
        ));
        snapshot.setAction("WATCHLIST_SHADOW");
        snapshot.setStatus("OBSERVATION_SHADOW");
        snapshot.setDecisionReasons(List.of("NEW_WALLET_NOT_MATURE"));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertFalse(decision.allowed());
        assertEquals("WATCHLIST_SHADOW", decision.action());
        assertEquals("NEW_WALLET_NOT_MATURE", decision.reason());
        assertEquals("SHADOW", decision.targetExecutionMode());
    }

    @Test
    void shouldNotCallFullSimulationInCopyGuardSnapshotHotPath() throws Exception {
        CapturingMetricClient client = new CapturingMetricClient(List.of(metricForHistory("0xabc")));
        MetricWalletServiceImpl service = service(client, 30, 45, "full");

        loadCopyGuardSnapshot(service, 300);

        assertEquals(1, client.copyGuardWindowsCalls);
        assertEquals("snapshot", client.lastCopyGuardMode);
        assertNotEquals("full", client.lastCopyGuardMode);
        assertTrue(client.lastCopyGuardWindows.contains("6mo"));
        assertTrue(client.lastCopyGuardWindows.contains("9mo"));
        assertTrue(client.lastCopyGuardWindows.contains("1y"));
        assertTrue(client.lastCopyGuardWindows.contains("2y"));
        assertEquals(0, client.joyasCalls);
        assertEquals(0, client.allPositionHistoryCalls);
    }

    @Test
    void shouldWarnOrAllowWhenSnapshotWindowMissingAndFailOpen() throws Exception {
        MetricWalletServiceImpl service = service(new FakeMetricClient(), 3, 30, "summary", false);
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "1mo", window("1mo", 3.0, "ALLOW", "OK")
        ));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("ALLOW", decision.action());
    }

    @Test
    void shouldMarkSnapshotStale() throws Exception {
        MetricWalletServiceImpl service = service();
        CopyGuardWindowSnapshotDto snapshot = snapshot(Map.of(
                "1w", window("1w", 1.0, "ALLOW", "OK"),
                "2w", window("2w", 2.0, "ALLOW", "OK"),
                "1mo", window("1mo", 3.0, "ALLOW", "OK")
        ));
        snapshot.setComputedAt(OffsetDateTime.now().minusHours(1));

        CopyStrategyGuardDecision decision = evaluateSnapshot(service, snapshot);

        assertTrue(decision.allowed());
        assertEquals("WARNING", decision.action());
        assertEquals("STALE_COPY_GUARD_SNAPSHOT", decision.reason());
    }

    private static MetricWalletServiceImpl service() {
        return service(new FakeMetricClient(), 3, 30, "summary");
    }

    private static MetricWalletServiceImpl service(MetricWalletsInfoClient client, int joyasLimit, int joyasDayz, String joyasSimulation) {
        return service(client, joyasLimit, joyasDayz, joyasSimulation, false);
    }

    private static MetricWalletServiceImpl service(MetricWalletsInfoClient client, int joyasLimit, int joyasDayz, String joyasSimulation, boolean requireWindowData) {
        return service(client, new FakeAllocationService(), joyasLimit, joyasDayz, joyasSimulation, requireWindowData);
    }

    private static MetricWalletServiceImpl service(MetricWalletsInfoClient client,
                                                   UserCopyAllocationService allocationService,
                                                   int joyasLimit,
                                                   int joyasDayz,
                                                   String joyasSimulation,
                                                   boolean requireWindowData) {
        return new MetricWalletServiceImpl(
                client,
                allocationService,
                new CopyStrategyRuntimeRouter(),
                Optional.empty(),
                300,
                30,
                1,
                Duration.ofMinutes(6),
                Duration.ofMinutes(10),
                Duration.ofMillis(250),
                0.90,
                0.90,
                false,
                "joyas",
                joyasLimit,
                joyasDayz,
                joyasSimulation,
                1.0,
                true,
                false,
                requireWindowData,
                0.0,
                0.0,
                -50.0,
                -25.0,
                1.0,
                0.25,
                "1w,2w,3w,1mo,2mo,3mo",
                "1d,3d,1w,2w,3w,1mo,2mo,3mo,6mo,9mo,1y,2y,all",
                true,
                Duration.ofMinutes(10)
        );
    }

    private static Object loadSnapshot(MetricWalletServiceImpl service, Integer limit, Integer dayz) throws Exception {
        Method method = MetricWalletServiceImpl.class.getDeclaredMethod("loadAllPositionHistory", Integer.class, Integer.class);
        method.setAccessible(true);
        return method.invoke(service, limit, dayz);
    }

    private static int snapshotSize(Object snapshot) throws Exception {
        Method method = snapshot.getClass().getDeclaredMethod("size");
        method.setAccessible(true);
        return (int) method.invoke(snapshot);
    }

    private static Object loadCopyGuardSnapshot(MetricWalletServiceImpl service, Integer limit) throws Exception {
        Method method = MetricWalletServiceImpl.class.getDeclaredMethod("loadCopyGuardWindowSnapshots", Integer.class);
        method.setAccessible(true);
        return method.invoke(service, limit);
    }

    private static CopyStrategyGuardDecision evaluate(MetricWalletServiceImpl service, MetricaWalletDto metric) throws Exception {
        Method method = MetricWalletServiceImpl.class.getDeclaredMethod("evaluateCopyGuard", MetricaWalletDto.class);
        method.setAccessible(true);
        return (CopyStrategyGuardDecision) method.invoke(service, metric);
    }

    private static CopyStrategyGuardDecision evaluateSnapshot(MetricWalletServiceImpl service, CopyGuardWindowSnapshotDto snapshot) throws Exception {
        Method method = MetricWalletServiceImpl.class.getDeclaredMethod("evaluateCopyGuardSnapshot", CopyGuardWindowSnapshotDto.class, String.class, String.class);
        method.setAccessible(true);
        return (CopyStrategyGuardDecision) method.invoke(service, snapshot, "0xabc", "MOVEMENT_ALL");
    }

    private static CopyGuardWindowSnapshotDto snapshot(Map<String, CopyGuardWindowSnapshotDto.WindowDto> windows) {
        return CopyGuardWindowSnapshotDto.builder()
                .walletId("0xabc")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("strategy")
                .scopeValue("MOVEMENT_ALL")
                .status("OK")
                .action("ALLOW")
                .allowNewEntries(true)
                .capitalMultiplier(1.0)
                .windows(windows)
                .computedAt(OffsetDateTime.now())
                .expiresAt(OffsetDateTime.now().plusMinutes(10))
                .sourceVersion("test")
                .build();
    }

    private static CopyGuardWindowSnapshotDto.WindowDto window(String code, double pnl, String action, String reasonCode) {
        return CopyGuardWindowSnapshotDto.WindowDto.builder()
                .windowCode(code)
                .complete(true)
                .closedOperations(10)
                .operations(10)
                .pnlNetUsd(pnl)
                .pnlGrossUsd(pnl)
                .feesUsd(0.0)
                .slippageUsd(0.0)
                .action(action)
                .status("ALLOW".equals(action) ? "OK" : action)
                .reasonCode(reasonCode)
                .capitalMultiplier(multiplierForTestWindow(code, action))
                .mature(true)
                .decisionEnabled(true)
                .decisionParticipates(!"INFO".equals(action))
                .build();
    }

    private static CopyGuardWindowSnapshotDto.WindowDto severeWindow(
            String code,
            double pnl,
            String action,
            String reasonCode,
            double lossPct,
            double capitalBaseUsd,
            String level
    ) {
        return CopyGuardWindowSnapshotDto.WindowDto.builder()
                .windowCode(code)
                .complete(true)
                .closedOperations(80)
                .operations(80)
                .pnlNetUsd(pnl)
                .pnlGrossUsd(pnl)
                .feesUsd(0.0)
                .slippageUsd(0.0)
                .lossPct(lossPct)
                .capitalBaseUsd(capitalBaseUsd)
                .severeLossGuardApplied(true)
                .severeLossLevel(level)
                .action(action)
                .status("ALLOW".equals(action) ? "OK" : action)
                .reasonCode(reasonCode)
                .capitalMultiplier(multiplierForTestWindow(code, action))
                .mature(true)
                .decisionEnabled(true)
                .decisionParticipates(!"INFO".equals(action))
                .build();
    }

    private static CopyGuardWindowSnapshotDto.WindowDto informationalWindow(String code, String reasonCode, boolean decisionEnabled) {
        return CopyGuardWindowSnapshotDto.WindowDto.builder()
                .windowCode(code)
                .complete(false)
                .mature(false)
                .decisionEnabled(decisionEnabled)
                .futureWindow(!decisionEnabled)
                .operations(5)
                .closedOperations(5)
                .pnlNetUsd(-1.0)
                .pnlGrossUsd(-1.0)
                .action("INFO")
                .status("INFO")
                .reasonCode(reasonCode)
                .infoReason(reasonCode)
                .capitalMultiplier(1.0)
                .decisionParticipates(false)
                .suggestedAction(decisionEnabled ? null : "MANUAL_REVIEW")
                .build();
    }

    private static double multiplierForTestWindow(String code, String action) {
        if ("REDUCE_CAPITAL".equals(action) && "2w".equals(code)) return 0.60;
        if ("REDUCE_CAPITAL".equals(action) && "3w".equals(code)) return 0.30;
        if ("REDUCE_CAPITAL".equals(action)) return 0.50;
        if ("SHADOW_ONLY".equals(action)) return 0.25;
        if ("PAUSE_OPEN".equals(action)
                || "SHADOW_REVALIDATION".equals(action)
                || "MICRO_LIVE_REQUIRED_REENTRY".equals(action)
                || "MANUAL_REVIEW".equals(action)
                || "WATCHLIST_SHADOW".equals(action)) return 0.0;
        return 1.0;
    }

    private static MetricaWalletDto metricWithGuard(String status, String action, boolean allowNewEntries, double multiplier) {
        MetricaWalletDto.CopyGuardDto guard = MetricaWalletDto.CopyGuardDto.builder()
                .status(status)
                .action(action)
                .allowNewEntries(allowNewEntries)
                .allowReductions(true)
                .allowCloses(true)
                .capitalMultiplier(multiplier)
                .targetExecutionMode("KEEP")
                .severityScore(60.0)
                .reasons(List.of("test_guard"))
                .build();
        return MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder().idWallet("0xabc").build())
                .strategy(MetricaWalletDto.StrategyDto.builder().strategyCode("MOVEMENT_ALL").build())
                .realJewel(MetricaWalletDto.RealJewelDto.builder()
                        .status(status)
                        .copyGuard(guard)
                        .build())
                .build();
    }

    private static final class FakeMetricClient implements MetricWalletsInfoClient {
        @Override
        public List<MetricaWalletDto> allPositionHistory(int limit, int dayz) {
            return List.of();
        }

        @Override
        public List<MetricaWalletDto> joyas(int limit, int dayz, String simulation) {
            return List.of();
        }

        @Override
        public List<CopyGuardWindowSnapshotDto> copyGuardWindows(int limit, int dayz, String mode, String windows) {
            return List.of();
        }

        @Override
        public CopyDecisionDto copyDecision(String walletId, String strategyCode, String scopeType, String scopeValue, String mode, String simulation, int minHistoryDays, int simulationLookbackDays, int maxFactsPerUnit, int timeoutMs, boolean debug) {
            return null;
        }
    }

    private static final class CapturingMetricClient implements MetricWalletsInfoClient {
        private final List<MetricaWalletDto> joyasResponse;
        private int joyasCalls;
        private int allPositionHistoryCalls;
        private int copyGuardWindowsCalls;
        private int lastLimit;
        private int lastDayz;
        private String lastSimulation;
        private String lastCopyGuardMode;
        private String lastCopyGuardWindows;
        private boolean throwNotFound;

        private CapturingMetricClient(List<MetricaWalletDto> joyasResponse) {
            this.joyasResponse = joyasResponse;
        }

        @Override
        public List<MetricaWalletDto> allPositionHistory(int limit, int dayz) {
            allPositionHistoryCalls++;
            return List.of();
        }

        @Override
        public List<MetricaWalletDto> joyas(int limit, int dayz, String simulation) {
            joyasCalls++;
            lastLimit = limit;
            lastDayz = dayz;
            lastSimulation = simulation;
            if (throwNotFound) {
                throw new RestClientResponseException("not found", 404, "Not Found", null, null, null);
            }
            return joyasResponse;
        }

        @Override
        public List<CopyGuardWindowSnapshotDto> copyGuardWindows(int limit, int dayz, String mode, String windows) {
            copyGuardWindowsCalls++;
            lastLimit = limit;
            lastDayz = dayz;
            lastCopyGuardMode = mode;
            lastCopyGuardWindows = windows;
            return List.of();
        }

        @Override
        public CopyDecisionDto copyDecision(String walletId, String strategyCode, String scopeType, String scopeValue, String mode, String simulation, int minHistoryDays, int simulationLookbackDays, int maxFactsPerUnit, int timeoutMs, boolean debug) {
            return null;
        }
    }

    private static MetricaWalletDto metricForHistory(String walletId) {
        return MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder()
                        .idWallet(walletId)
                        .countOperationBreakdown(MetricaWalletDto.CountOperationBreakdownDto.builder()
                                .strategyCode("MOVEMENT_ALL")
                                .scopeType("strategy")
                                .scopeValue("MOVEMENT_ALL")
                                .build())
                        .build())
                .strategy(MetricaWalletDto.StrategyDto.builder()
                        .strategyCode("MOVEMENT_ALL")
                .build())
                .scoring(MetricaWalletDto.ScoringDto.builder()
                        .decisionMetricConservative(75)
                        .decisionMetricScalping(70)
                        .decisionMetricAggressive(70)
                        .build())
                .build();
    }

    private static class FakeAllocationService implements UserCopyAllocationService {
        @Override public void syncDistribution(List<MetricaWalletDto> candidates) {}
        @Override public List<UserCopyAllocationEntity> getWalletUserId(UUID idUser) { return List.of(); }
        @Override public Set<UUID> getActiveUserIdsByWallet(String walletId) { return Set.of(); }
        @Override public List<UserCopyAllocationEntity> getActiveAllocationsByWallet(String walletId) { return List.of(); }
        @Override public List<UserCopyAllocationEntity> getActiveAllocationsForUserWallet(UUID idUser, String walletId) { return List.of(); }
        @Override public Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId) { return Optional.empty(); }
        @Override public Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId, String strategyCode) { return Optional.empty(); }
        @Override public Optional<UserCopyAllocationEntity> findOpenAllocation(UUID idUser, String walletId, String strategyCode) { return Optional.empty(); }
        @Override public void markGuardBlocked(UUID idUser, String walletId, String strategyCode, String targetStatus, String reason, OffsetDateTime cooldownUntil) {}
    }

    private static final class CacheOnlyAllocationService extends FakeAllocationService {
        private final UserCopyAllocationEntity allocation;
        private int databaseLoads;
        private int cacheOnlyLoads;

        private CacheOnlyAllocationService(UserCopyAllocationEntity allocation) {
            this.allocation = allocation;
        }

        @Override
        public List<UserCopyAllocationEntity> getActiveAllocationsForUserWallet(UUID idUser, String walletId) {
            databaseLoads++;
            throw new AssertionError("database-backed allocation lookup reached from hot path");
        }

        @Override
        public List<UserCopyAllocationEntity> getActiveAllocationsForUserWalletCachedOnly(UUID idUser, String walletId) {
            cacheOnlyLoads++;
            return Objects.equals(allocation.getIdUser(), idUser)
                    && allocation.getWalletId().equalsIgnoreCase(walletId)
                    ? List.of(allocation)
                    : List.of();
        }
    }
}
