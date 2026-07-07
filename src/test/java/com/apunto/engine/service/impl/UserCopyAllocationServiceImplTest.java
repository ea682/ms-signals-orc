package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.apunto.engine.service.UserDetailService;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.service.copy.symbol.CopySymbolResolution;
import com.apunto.engine.service.copy.symbol.CopySymbolResolver;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;

class UserCopyAllocationServiceImplTest {

    @Test
    void syncDistributionStillSyncsShadowWhenLiveCandidatesAreEmpty() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto shadowCandidate = metric("0xabc", "SHORT_ONLY");
        AtomicInteger shadowSyncCalls = new AtomicInteger();
        AtomicReference<List<MetricaWalletDto>> syncedShadowCandidates = new AtomicReference<>();

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(),
                () -> List.of(activeUser(userId, 1)),
                new CopyStrategyRuntimeRouter(),
                shadowService(shadowSyncCalls, syncedShadowCandidates),
                defaultSymbolResolver()
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(), List.of(shadowCandidate));

        assertEquals(1, shadowSyncCalls.get());
        assertSame(shadowCandidate, syncedShadowCandidates.get().get(0));
    }

    @Test
    void syncDistributionCreatesMicroLiveAllocationWhenLivePromotableIsFalse() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto microCandidate = metric("0xabc", "MOVEMENT_ALL").toBuilder()
                .decisionFinal(true)
                .realJewel(MetricaWalletDto.RealJewelDto.builder()
                        .strategyCode("MOVEMENT_ALL")
                        .scopeType("ALL")
                        .scopeValue("ALL")
                        .recommendedExecutionMode("MICRO_LIVE")
                        .riskClass("B")
                        .evidenceScore(75.0)
                        .canMicroLive(true)
                        .canLive(false)
                        .hardBlockers(List.of())
                        .softWarnings(List.of("NEEDS_MORE_CLOSED_POSITIONS"))
                        .copyGuard(MetricaWalletDto.CopyGuardDto.builder()
                                .action("ALLOW")
                                .status("OK")
                                .allowNewEntries(true)
                                .allowReductions(true)
                                .allowCloses(true)
                                .build())
                        .build())
                .build();
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());
        AtomicReference<List<UserCopyAllocationEntity>> linked = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1)),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), false, true, linked),
                defaultSymbolResolver()
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(microCandidate), List.of(microCandidate));

        assertEquals(1, saved.get().stream()
                .filter(e -> "MICRO_LIVE".equals(e.getExecutionMode()))
                .count());
        assertEquals(0, saved.get().stream()
                .filter(e -> "LIVE".equals(e.getExecutionMode()))
                .count());
        assertEquals("MICRO_LIVE", linked.get().get(0).getExecutionMode());
    }

    @Test
    void syncDistributionMapsJoyasLegacyCopyModeBeforePersistingAllocation() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto candidate = metric("0xabc", "MOVEMENT_ALL", "copy_movement_all_events");
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1)),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), true, true, new AtomicReference<>()),
                defaultSymbolResolver()
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(candidate), List.of(candidate));

        assertEquals(1, saved.get().size());
        assertEquals("copy_all_metric_movements", saved.get().get(0).getCopyMode());
    }

    @Test
    void syncDistributionSkipsInvalidJoyasCopyModeMappingWithoutPersistingAllocation() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto candidate = metric("0xabc", "UNKNOWN_PROFILE", "SKIP");
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1)),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), true, true, new AtomicReference<>()),
                defaultSymbolResolver()
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(candidate), List.of(candidate));

        assertTrue(saved.get().isEmpty());
    }

    @Test
    void syncDistributionDoesNotCreateMicroLiveWhenHardBlockerExists() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto blocked = metric("0xabc", "MOVEMENT_ALL").toBuilder()
                .decisionFinal(true)
                .realJewel(MetricaWalletDto.RealJewelDto.builder()
                        .strategyCode("MOVEMENT_ALL")
                        .scopeType("ALL")
                        .scopeValue("ALL")
                        .recommendedExecutionMode("MICRO_LIVE")
                        .riskClass("B")
                        .evidenceScore(80.0)
                        .canMicroLive(true)
                        .hardBlockers(List.of("SYMBOL_NOT_SUPPORTED"))
                        .copyGuard(MetricaWalletDto.CopyGuardDto.builder()
                                .action("ALLOW")
                                .status("OK")
                                .allowNewEntries(true)
                                .build())
                        .build())
                .build();
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1)),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), false, false, new AtomicReference<>()),
                defaultSymbolResolver()
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(blocked), List.of(blocked));

        assertTrue(saved.get().stream()
                .noneMatch(e -> "MICRO_LIVE".equals(e.getExecutionMode()) || "LIVE".equals(e.getExecutionMode())));
    }

    @Test
    void syncDistributionDowngradesLiveRecommendationToMicroLiveWhenLiveReadinessFails() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto liveCandidate = metric("0xabc", "MOVEMENT_ALL").toBuilder()
                .decisionFinal(true)
                .realJewel(MetricaWalletDto.RealJewelDto.builder()
                        .strategyCode("MOVEMENT_ALL")
                        .scopeType("ALL")
                        .scopeValue("ALL")
                        .recommendedExecutionMode("LIVE")
                        .riskClass("B")
                        .evidenceScore(82.0)
                        .canMicroLive(true)
                        .canLive(false)
                        .hardBlockers(List.of())
                        .softWarnings(List.of("NEEDS_MORE_CLOSED_POSITIONS"))
                        .copyGuard(MetricaWalletDto.CopyGuardDto.builder()
                                .action("ALLOW")
                                .status("OK")
                                .allowNewEntries(true)
                                .build())
                        .build())
                .build();
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1)),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), false, true, new AtomicReference<>()),
                defaultSymbolResolver()
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(liveCandidate), List.of(liveCandidate));

        assertEquals(1, saved.get().stream()
                .filter(e -> "MICRO_LIVE".equals(e.getExecutionMode()))
                .count());
        assertEquals(0, saved.get().stream()
                .filter(e -> "LIVE".equals(e.getExecutionMode()))
                .count());
    }

    @Test
    void syncDistributionRequiresMicroLiveByDefaultEvenWhenLiveReady() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto liveCandidate = liveReadyMetric("0xabc", "MOVEMENT_ALL");
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1)),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), true, true, new AtomicReference<>()),
                defaultSymbolResolver()
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(liveCandidate), List.of(liveCandidate));

        assertEquals(1, saved.get().stream()
                .filter(e -> "MICRO_LIVE".equals(e.getExecutionMode()))
                .count());
        assertEquals(0, saved.get().stream()
                .filter(e -> "LIVE".equals(e.getExecutionMode()))
                .count());
    }

    @Test
    void syncDistributionAllowsDirectLiveOnlyWithExplicitPolicy() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto liveCandidate = liveReadyMetric("0xabc", "MOVEMENT_ALL");
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1)),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), true, true, new AtomicReference<>()),
                defaultSymbolResolver()
        );
        setField(service, "entityManager", entityManager());
        setField(service, "directLivePolicy", "ALLOW_DIRECT_LIVE_FOR_LIVE_READY");

        service.syncDistribution(List.of(liveCandidate), List.of(liveCandidate));

        assertEquals(1, saved.get().stream()
                .filter(e -> "LIVE".equals(e.getExecutionMode()))
                .count());
        assertEquals("copy_all_metric_movements", saved.get().get(0).getCopyMode());
    }

    @Test
    void syncDistributionResolvesSymbolSpecialistTargetForUserCapitalAsset() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto sourceSymbol = symbolMetric("0xbtc", "BTCUSDT");
        MetricaWalletDto compatibleMovement = metric("0xmovement", "MOVEMENT_ALL");
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 2, "USDC")),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), true, true, new AtomicReference<>()),
                resolver(Map.of("BTCUSDT|USDC", CopySymbolResolution.resolved(
                        "BTCUSDT",
                        "BTCUSDC",
                        "BTC",
                        "USDC",
                        "USDC",
                        false
                )))
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(sourceSymbol, compatibleMovement), List.of(sourceSymbol, compatibleMovement));

        assertEquals(1, saved.get().stream()
                .filter(e -> "SYMBOL_SPECIALIST".equals(e.getCopyStrategyCode()))
                .count());
        UserCopyAllocationEntity symbolAllocation = saved.get().stream()
                .filter(e -> "SYMBOL_SPECIALIST".equals(e.getCopyStrategyCode()))
                .findFirst()
                .orElseThrow();
        assertEquals("BTCUSDT", symbolAllocation.getScopeValue());
        assertEquals("BTCUSDT", symbolAllocation.getSourceSymbol());
        assertEquals("BTCUSDC", symbolAllocation.getTargetSymbol());
        assertEquals("USDC", symbolAllocation.getCapitalAsset());
        assertEquals("RESOLVED", symbolAllocation.getSymbolResolutionStatus());
    }

    @Test
    void syncDistributionSkipsOnlyUnavailableTargetSymbols() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto missingTarget = symbolMetric("0xhype", "HYPEUSDT");
        MetricaWalletDto availableTarget = symbolMetric("0xeth", "ETHUSDT");
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 3, "USDC")),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), true, true, new AtomicReference<>()),
                resolver(Map.of(
                        "HYPEUSDT|USDC", CopySymbolResolution.skipped(
                                "HYPEUSDT",
                                "HYPEUSDC",
                                "HYPE",
                                "USDC",
                                "USDC",
                                "SYMBOL_TARGET_NOT_AVAILABLE"
                        ),
                        "ETHUSDT|USDC", CopySymbolResolution.resolved(
                                "ETHUSDT",
                                "ETHUSDC",
                                "ETH",
                                "USDC",
                                "USDC",
                                false
                        )
                ))
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(missingTarget, availableTarget), List.of(missingTarget, availableTarget));

        assertTrue(saved.get().stream()
                .noneMatch(e -> "HYPEUSDT".equals(e.getSourceSymbol())));
        UserCopyAllocationEntity eth = saved.get().stream()
                .filter(e -> "ETHUSDT".equals(e.getSourceSymbol()))
                .findFirst()
                .orElseThrow();
        assertEquals("ETHUSDC", eth.getTargetSymbol());
    }

    @Test
    void syncDistributionDefaultsMissingCapitalAssetToUsdtForSymbolResolution() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto sourceSymbol = symbolMetric("0xbtc", "BTCUSDT");
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1, null)),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), true, true, new AtomicReference<>()),
                resolver(Map.of("BTCUSDT|USDT", CopySymbolResolution.resolved(
                        "BTCUSDT",
                        "BTCUSDT",
                        "BTC",
                        "USDT",
                        "USDT",
                        false
                )))
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(sourceSymbol), List.of(sourceSymbol));

        UserCopyAllocationEntity allocation = saved.get().stream()
                .filter(e -> "SYMBOL_SPECIALIST".equals(e.getCopyStrategyCode()))
                .findFirst()
                .orElseThrow();
        assertEquals("BTCUSDT", allocation.getTargetSymbol());
        assertEquals("USDT", allocation.getCapitalAsset());
    }

    @Test
    void syncDistributionSkipsInvalidCapitalAssetForSymbolSpecialist() throws Exception {
        UUID userId = UUID.randomUUID();
        MetricaWalletDto sourceSymbol = symbolMetric("0xbtc", "BTCUSDT");
        AtomicReference<List<UserCopyAllocationEntity>> saved = new AtomicReference<>(List.of());

        UserCopyAllocationServiceImpl service = new UserCopyAllocationServiceImpl(
                repository(saved),
                () -> List.of(activeUser(userId, 1, "EUR")),
                new CopyStrategyRuntimeRouter(),
                shadowService(new AtomicInteger(), new AtomicReference<>(), true, true, new AtomicReference<>()),
                resolver(Map.of("BTCUSDT|EUR", CopySymbolResolution.skipped(
                        "BTCUSDT",
                        null,
                        null,
                        null,
                        "EUR",
                        "SYMBOL_CAPITAL_ASSET_INVALID"
                )))
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(sourceSymbol), List.of(sourceSymbol));

        assertTrue(saved.get().stream()
                .noneMatch(e -> "SYMBOL_SPECIALIST".equals(e.getCopyStrategyCode())));
    }

    private static UserCopyAllocationRepository repository() {
        return repository(new AtomicReference<>());
    }

    private static UserCopyAllocationRepository repository(AtomicReference<List<UserCopyAllocationEntity>> saved) {
        return proxy(UserCopyAllocationRepository.class, (method, args) -> {
            if ("findAllByIdUserAndEndsAtIsNull".equals(method.getName())) {
                return List.of();
            }
            if ("findAllByIdUserAndWalletIdIn".equals(method.getName())) {
                return List.of();
            }
            if ("saveAll".equals(method.getName()) || "saveAllAndFlush".equals(method.getName())) {
                saved.set(copyEntities(args[0]));
                return args[0];
            }
            return unexpected(method);
        });
    }

    @SuppressWarnings("unchecked")
    private static List<UserCopyAllocationEntity> copyEntities(Object source) {
        if (source instanceof List<?> list) {
            return List.copyOf((List<UserCopyAllocationEntity>) list);
        }
        List<UserCopyAllocationEntity> out = new ArrayList<>();
        ((Iterable<UserCopyAllocationEntity>) source).forEach(out::add);
        return List.copyOf(out);
    }

    private static ShadowCopyTradingService shadowService(
            AtomicInteger shadowSyncCalls,
            AtomicReference<List<MetricaWalletDto>> syncedShadowCandidates
    ) {
        return shadowService(shadowSyncCalls, syncedShadowCandidates, false, false, new AtomicReference<>());
    }

    private static ShadowCopyTradingService shadowService(
            AtomicInteger shadowSyncCalls,
            AtomicReference<List<MetricaWalletDto>> syncedShadowCandidates,
            boolean livePromotable,
            boolean microLivePromotable,
            AtomicReference<List<UserCopyAllocationEntity>> linkedAllocations
    ) {
        return new ShadowCopyTradingService() {
            @Override
            public void syncShadowAllocations(UUID idUser, List<MetricaWalletDto> candidates, int userMaxWallet, OffsetDateTime now) {
                shadowSyncCalls.incrementAndGet();
                syncedShadowCandidates.set(candidates);
            }

            @Override
            public void linkLiveAllocations(UUID idUser, List<UserCopyAllocationEntity> liveAllocations) {
                linkedAllocations.set(liveAllocations);
            }

            @Override
            public int recordShadowEvent(com.apunto.engine.events.OperacionEvent event) {
                return 0;
            }

            @Override
            public boolean isSeparateShadowEnabled() {
                return true;
            }

            @Override
            public boolean isLivePromotable(UUID idUser, MetricaWalletDto candidate) {
                return livePromotable;
            }

            @Override
            public boolean isMicroLivePromotable(UUID idUser, MetricaWalletDto candidate) {
                return microLivePromotable;
            }
        };
    }

    private static UserDetailDto activeUser(UUID userId, int maxWallet) {
        return activeUser(userId, maxWallet, "USDT");
    }

    private static UserDetailDto activeUser(UUID userId, int maxWallet, String capitalAsset) {
        UserEntity user = new UserEntity();
        user.setId(userId);
        DetailUserEntity detail = new DetailUserEntity();
        detail.setMaxWallet(maxWallet);
        detail.setCapitalAsset(capitalAsset);
        return new UserDetailDto(user, detail, null);
    }

    private static MetricaWalletDto metric(String walletId, String strategyCode) {
        return metric(walletId, strategyCode, null);
    }

    private static MetricaWalletDto metric(String walletId, String strategyCode, String copyMode) {
        return MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder()
                        .idWallet(walletId)
                        .countOperationBreakdown(MetricaWalletDto.CountOperationBreakdownDto.builder()
                                .strategyCode(strategyCode)
                                .scopeType("strategy")
                                .scopeValue(strategyCode)
                                .build())
                        .build())
                .strategy(MetricaWalletDto.StrategyDto.builder()
                        .strategyCode(strategyCode)
                        .copyMode(copyMode)
                        .build())
                .capitalShare(1.0)
                .build();
    }

    private static MetricaWalletDto symbolMetric(String walletId, String symbol) {
        return MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder()
                        .idWallet(walletId)
                        .countOperationBreakdown(MetricaWalletDto.CountOperationBreakdownDto.builder()
                                .strategyCode("SYMBOL_SPECIALIST")
                                .scopeType("SYMBOL")
                                .scopeValue(symbol)
                                .build())
                        .build())
                .strategy(MetricaWalletDto.StrategyDto.builder()
                        .strategyCode("SYMBOL_SPECIALIST")
                        .build())
                .realJewel(MetricaWalletDto.RealJewelDto.builder()
                        .strategyCode("SYMBOL_SPECIALIST")
                        .scopeType("SYMBOL")
                        .scopeValue(symbol)
                        .recommendedExecutionMode("LIVE")
                        .canMicroLive(true)
                        .canLive(true)
                        .hardBlockers(List.of())
                        .copyGuard(MetricaWalletDto.CopyGuardDto.builder()
                                .action("ALLOW")
                                .status("OK")
                                .allowNewEntries(true)
                                .build())
                        .build())
                .capitalShare(1.0)
                .build();
    }

    private static MetricaWalletDto liveReadyMetric(String walletId, String strategyCode) {
        return metric(walletId, strategyCode, "copy_movement_all_events").toBuilder()
                .decisionFinal(true)
                .realJewel(MetricaWalletDto.RealJewelDto.builder()
                        .strategyCode(strategyCode)
                        .scopeType("ALL")
                        .scopeValue("ALL")
                        .recommendedExecutionMode("LIVE")
                        .riskClass("A")
                        .evidenceScore(95.0)
                        .canMicroLive(true)
                        .canLive(true)
                        .hardBlockers(List.of())
                        .copyGuard(MetricaWalletDto.CopyGuardDto.builder()
                                .action("ALLOW")
                                .status("OK")
                                .allowNewEntries(true)
                                .allowReductions(true)
                                .allowCloses(true)
                                .build())
                        .build())
                .build();
    }

    private static CopySymbolResolver defaultSymbolResolver() {
        return (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(
                sourceSymbol,
                sourceSymbol,
                sourceSymbol,
                capitalAsset,
                capitalAsset,
                false
        );
    }

    private static CopySymbolResolver resolver(Map<String, CopySymbolResolution> results) {
        return (sourceSymbol, capitalAsset) -> {
            CopySymbolResolution resolution = results.get(sourceSymbol + "|" + capitalAsset);
            if (resolution != null) {
                return resolution;
            }
            return CopySymbolResolution.skipped(
                    sourceSymbol,
                    sourceSymbol == null ? null : sourceSymbol + capitalAsset,
                    sourceSymbol,
                    capitalAsset,
                    capitalAsset,
                    "SYMBOL_TARGET_NOT_AVAILABLE"
            );
        };
    }

    private static EntityManager entityManager() {
        return proxy(EntityManager.class, (method, args) -> {
            if ("flush".equals(method.getName()) || "clear".equals(method.getName())) {
                return null;
            }
            return unexpected(method);
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Invocation invocation) {
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "toString" -> type.getSimpleName() + "Proxy";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> null;
                };
            }
            return invocation.invoke(method, args == null ? new Object[0] : args);
        };
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object unexpected(Method method) {
        throw new AssertionError("Unexpected call: " + method.getName());
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(Method method, Object[] args);
    }
}
