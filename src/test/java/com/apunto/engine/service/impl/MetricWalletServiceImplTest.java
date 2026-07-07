package com.apunto.engine.service.impl;
import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.service.annotation.GetExchange;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MetricWalletServiceImplTest {

    @Test
    void metricWalletClientUsesCanonicalJoyasEndpointPath() throws Exception {
        Method method = MetricWalletsInfoClient.class.getMethod("joyas", int.class, int.class, String.class);
        GetExchange exchange = method.getAnnotation(GetExchange.class);

        assertNotNull(exchange);
        assertEquals("/operaciones/metrica/joyas", exchange.value());
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
    void oneWeekNegativeWindowReducesWithoutPausing() throws Exception {
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
        assertEquals("REDUCE_CAPITAL", decision.action());
        assertEquals(0.70, decision.capitalMultiplier(), 0.000001);
    }

    @Test
    void twoWeekNegativeWindowPausesLiveOpenings() throws Exception {
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

        assertFalse(decision.allowed());
        assertEquals("PAUSE_OPEN", decision.action());
        assertEquals("PAUSED_BY_NEGATIVE_PNL", decision.statusWhenBlocked());
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

        assertTrue(decision.allowed());
        assertEquals("SHADOW_ONLY", decision.action());
        assertEquals("NEGATIVE_REQUIRED_WINDOW_1MO", decision.reason());
    }

    @Test
    void incompleteRequiredWindowBlocksLiveOpenings() throws Exception {
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

        assertFalse(decision.allowed());
        assertEquals("INCOMPLETE_REQUIRED_WINDOW_1MO", decision.reason());
        assertEquals("PAUSE_OPEN", decision.action());
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

    private static MetricWalletServiceImpl service() {
        return service(new FakeMetricClient(), 3, 30, "summary");
    }

    private static MetricWalletServiceImpl service(MetricWalletsInfoClient client, int joyasLimit, int joyasDayz, String joyasSimulation) {
        return new MetricWalletServiceImpl(
                client,
                new FakeAllocationService(),
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
                true,
                0.0,
                0.0,
                -50.0,
                -25.0,
                0.70,
                0.25,
                "1w,2w,1mo"
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

    private static CopyStrategyGuardDecision evaluate(MetricWalletServiceImpl service, MetricaWalletDto metric) throws Exception {
        Method method = MetricWalletServiceImpl.class.getDeclaredMethod("evaluateCopyGuard", MetricaWalletDto.class);
        method.setAccessible(true);
        return (CopyStrategyGuardDecision) method.invoke(service, metric);
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
    }

    private static final class CapturingMetricClient implements MetricWalletsInfoClient {
        private final List<MetricaWalletDto> joyasResponse;
        private int joyasCalls;
        private int allPositionHistoryCalls;
        private int lastLimit;
        private int lastDayz;
        private String lastSimulation;
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

    private static final class FakeAllocationService implements UserCopyAllocationService {
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
}
