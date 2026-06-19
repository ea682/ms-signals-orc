package com.apunto.engine.service.impl;
import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MetricWalletServiceImplTest {

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

    private static MetricWalletServiceImpl service() {
        return new MetricWalletServiceImpl(
                new FakeMetricClient(),
                new FakeAllocationService(),
                new CopyStrategyRuntimeRouter(),
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
                3,
                30,
                "summary",
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
        public List<MetricaWalletDto> joyas(int limitWallet, int limit, int dayz, String simulation) {
            return List.of();
        }
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
