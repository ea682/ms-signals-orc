package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.CopyWalletProfileEntity;
import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import com.apunto.engine.entity.ShadowCopyOperationEventEntity;
import com.apunto.engine.entity.ShadowPositionStateEntity;
import com.apunto.engine.entity.ShadowWalletProfileValidationEntity;
import com.apunto.engine.repository.CopyWalletProfileRepository;
import com.apunto.engine.repository.ShadowCopyAllocationRepository;
import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowCopyOperationRepository;
import com.apunto.engine.repository.ShadowPositionStateRepository;
import com.apunto.engine.repository.ShadowWalletProfileValidationRepository;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.apunto.engine.shared.enums.PositionSide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowCopyTradingServiceImplTest {

    @Test
    void livePromotionRequiresShadowEvidencePerUser() throws Exception {
        UUID userA = UUID.randomUUID();
        UUID userB = UUID.randomUUID();
        MetricaWalletDto metric = metric("0xabc", "MOVEMENT_ALL");

        ShadowCopyAllocationEntity userAShadow = shadowAllocation(10L, userA);
        ShadowCopyAllocationEntity userBShadow = shadowAllocation(20L, userB);

        ShadowCopyTradingServiceImpl service = service(
                Map.of(userA, userAShadow, userB, userBShadow),
                Map.of(10L, 5L, 20L, 4L),
                Map.of(10L, new BigDecimal("1.25"), 20L, new BigDecimal("10.00"))
        );

        assertTrue(service.isLivePromotable(userA, metric));
        assertFalse(service.isLivePromotable(userB, metric));
        assertFalse(service.isLivePromotable(UUID.randomUUID(), metric));
    }

    @Test
    void advancedRealProfileCanPromoteButScoringWindowCannot() throws Exception {
        UUID user = UUID.randomUUID();
        ShadowCopyTradingServiceImpl service = service(
                Map.of(user, shadowAllocation(10L, user)),
                Map.of(10L, 5L),
                Map.of(10L, new BigDecimal("1.25"))
        );

        assertTrue(service.isLivePromotable(user, metric("0xabc", "TOP_SYMBOLS_ONLY")));
        assertFalse(service.isLivePromotable(user, metric("0xabc", "RECENT_30D")));
    }


    @Test
    void recordShadowEventRunsWithoutLiveAndKeepsProfilesIndependent() throws Exception {
        UUID user = UUID.randomUUID();
        List<ShadowCopyAllocationEntity> activeProfiles = List.of(
                shadowAllocation(10L, user, "MOVEMENT_ALL", "MOVEMENT_ALL"),
                shadowAllocation(20L, user, "LONG_ONLY", "LONG"),
                shadowAllocation(30L, user, "SHORT_ONLY", "SHORT")
        );
        List<ShadowCopyOperationEventEntity> recordedEvents = new ArrayList<>();
        List<ShadowPositionStateEntity> openedPositions = new ArrayList<>();

        ShadowCopyTradingServiceImpl service = serviceForRuntime(activeProfiles, recordedEvents, openedPositions);

        OperacionEvent event = new OperacionEvent(
                OperacionEvent.Tipo.ABIERTA,
                OperacionDto.builder()
                        .idOperacion(UUID.randomUUID())
                        .idCuenta("0xabc")
                        .parSymbol("BTCUSDT")
                        .tipoOperacion(PositionSide.LONG)
                        .sizeQty(new BigDecimal("0.1"))
                        .notionalUsd(new BigDecimal("1000"))
                        .precioEntrada(new BigDecimal("100000"))
                        .fechaCreacion(Instant.parse("2026-06-22T10:00:00Z"))
                        .build()
        );
        event.setDeltaType("OPEN");

        assertEquals(2, service.recordShadowEvent(event));
        assertEquals(2, recordedEvents.size());
        assertEquals(2, openedPositions.size());
        assertTrue(recordedEvents.stream().anyMatch(e -> "MOVEMENT_ALL".equals(e.getCopyStrategyCode())));
        assertTrue(recordedEvents.stream().anyMatch(e -> "LONG_ONLY".equals(e.getCopyStrategyCode())));
        assertFalse(recordedEvents.stream().anyMatch(e -> "SHORT_ONLY".equals(e.getCopyStrategyCode())));
    }

    @Test
    void recordShadowEventDedupesSameGlobalProfileAcrossUsers() throws Exception {
        List<ShadowCopyAllocationEntity> activeProfiles = List.of(
                shadowAllocation(10L, UUID.randomUUID(), "MOVEMENT_ALL", "MOVEMENT_ALL", 100L),
                shadowAllocation(11L, UUID.randomUUID(), "MOVEMENT_ALL", "MOVEMENT_ALL", 100L)
        );
        List<ShadowCopyOperationEventEntity> recordedEvents = new ArrayList<>();
        List<ShadowPositionStateEntity> openedPositions = new ArrayList<>();

        ShadowCopyTradingServiceImpl service = serviceForRuntime(activeProfiles, recordedEvents, openedPositions);

        OperacionEvent event = new OperacionEvent(
                OperacionEvent.Tipo.ABIERTA,
                OperacionDto.builder()
                        .idOperacion(UUID.randomUUID())
                        .idCuenta("0xabc")
                        .parSymbol("BTCUSDT")
                        .tipoOperacion(PositionSide.LONG)
                        .sizeQty(new BigDecimal("0.1"))
                        .notionalUsd(new BigDecimal("1000"))
                        .precioEntrada(new BigDecimal("100000"))
                        .fechaCreacion(Instant.parse("2026-06-22T10:00:00Z"))
                        .build()
        );
        event.setDeltaType("OPEN");

        assertEquals(1, service.recordShadowEvent(event));
        assertEquals(1, recordedEvents.size());
        assertEquals(1, openedPositions.size());
        assertEquals(100L, recordedEvents.get(0).getWalletProfileId());
        assertEquals(100L, openedPositions.get(0).getWalletProfileId());
    }

    @Test
    void syncShadowActivityDoesNotFallbackStrategyOpenedAtToWalletOpenedAt() throws Exception {
        UUID user = UUID.randomUUID();
        OffsetDateTime walletOpened = OffsetDateTime.parse("2026-06-22T10:00:00Z");
        List<ShadowCopyAllocationEntity> saved = new ArrayList<>();
        ShadowCopyTradingServiceImpl service = serviceForShadowSync(saved);

        MetricaWalletDto metric = metric("0xabc", "SHORT_ONLY");
        metric.setActivity(MetricaWalletDto.ActivityDto.builder()
                .lastOpenedAt(walletOpened)
                .walletLastOpenedAt(walletOpened)
                .build());

        service.syncShadowAllocations(user, List.of(metric), 1, OffsetDateTime.parse("2026-06-22T11:00:00Z"));

        assertEquals(1, saved.size());
        assertEquals(walletOpened, saved.get(0).getWalletLastOpenedAt());
        assertNull(saved.get(0).getStrategyLastOpenedAt());
    }

    private static ShadowCopyTradingServiceImpl service(
            Map<UUID, ShadowCopyAllocationEntity> shadowsByUser,
            Map<Long, Long> closedByShadow,
            Map<Long, BigDecimal> netByShadow
    ) throws Exception {
        ShadowCopyTradingServiceImpl service = new ShadowCopyTradingServiceImpl(
                shadowAllocationRepository(shadowsByUser),
                proxy(ShadowCopyOperationRepository.class, (method, args) -> unexpected(method)),
                proxy(ShadowCopyOperationEventRepository.class, (method, args) -> unexpected(method)),
                shadowPositionStateRepository(closedByShadow, netByShadow),
                proxy(CopyWalletProfileRepository.class, (method, args) -> unexpected(method)),
                proxy(ShadowWalletProfileValidationRepository.class, (method, args) -> unexpected(method)),
                new CopyStrategyRuntimeRouter()
        );

        setField(service, "separateShadowEnabled", true);
        setField(service, "requireShadowValidationBeforeLive", true);
        setField(service, "minShadowClosedOperationsForLive", 5);
        setField(service, "minShadowNetPnlUsdtForLive", BigDecimal.ZERO);
        setField(service, "requirePositiveWindows", "2w,1mo");
        setField(service, "shadowVersion", 1);
        setField(service, "shadowSlippageBps", 0.0d);
        return service;
    }

    private static ShadowCopyAllocationEntity shadowAllocation(Long id, UUID userId) {
        return shadowAllocation(id, userId, "MOVEMENT_ALL", "MOVEMENT_ALL");
    }

    private static ShadowCopyAllocationEntity shadowAllocation(Long id, UUID userId, String strategyCode, String scopeValue) {
        return shadowAllocation(id, userId, strategyCode, scopeValue, null);
    }

    private static ShadowCopyAllocationEntity shadowAllocation(Long id, UUID userId, String strategyCode, String scopeValue, Long walletProfileId) {
        return ShadowCopyAllocationEntity.builder()
                .id(id)
                .idUser(userId)
                .walletId("0xabc")
                .copyStrategyCode(strategyCode)
                .scopeType("strategy")
                .scopeValue(scopeValue)
                .strategyKey("0xabc|" + strategyCode + "|strategy|" + scopeValue)
                .walletProfileId(walletProfileId)
                .shadowVersion(1)
                .active(true)
                .status("SHADOW_ACTIVE")
                .build();
    }

    private static MetricaWalletDto metric(String walletId, String strategyCode) {
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
                        .build())
                .copySimulation(MetricaWalletDto.CopySimulationDto.builder()
                        .pnlCopyNet(Map.of("2w", 1.0, "1mo", 2.0))
                        .pnlCopyTotalNetUSDT(2.0)
                        .build())
                .capitalShare(1.0)
                .build();
    }

    private static ShadowCopyTradingServiceImpl serviceForRuntime(
            List<ShadowCopyAllocationEntity> activeProfiles,
            List<ShadowCopyOperationEventEntity> recordedEvents,
            List<ShadowPositionStateEntity> openedPositions
    ) throws Exception {
        ShadowCopyTradingServiceImpl service = new ShadowCopyTradingServiceImpl(
                proxy(ShadowCopyAllocationRepository.class, (method, args) -> {
                    if ("findRuntimeProfileRepresentativesByWallet".equals(method.getName())) {
                        return activeProfiles;
                    }
                    return unexpected(method);
                }),
                proxy(ShadowCopyOperationRepository.class, (method, args) -> {
                    if ("findFirstByWalletProfileIdAndIdOrderOriginAndTypeOperationAndActiveTrue".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("findFirstByShadowAllocationIdAndIdOrderOriginAndTypeOperationAndActiveTrue".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(ShadowCopyOperationEventRepository.class, (method, args) -> {
                    if ("existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime".equals(method.getName())) {
                        return false;
                    }
                    if ("existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime".equals(method.getName())) {
                        return false;
                    }
                    if ("save".equals(method.getName())) {
                        recordedEvents.add((ShadowCopyOperationEventEntity) args[0]);
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(ShadowPositionStateRepository.class, (method, args) -> {
                    if ("findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatus".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatus".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("findAllByWalletProfileIdAndParsymbolAndStatus".equals(method.getName())) {
                        return List.of();
                    }
                    if ("findAllByShadowAllocationIdAndParsymbolAndStatus".equals(method.getName())) {
                        return List.of();
                    }
                    if ("save".equals(method.getName())) {
                        openedPositions.add((ShadowPositionStateEntity) args[0]);
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(CopyWalletProfileRepository.class, (method, args) -> unexpected(method)),
                proxy(ShadowWalletProfileValidationRepository.class, (method, args) -> unexpected(method)),
                new CopyStrategyRuntimeRouter()
        );
        setField(service, "separateShadowEnabled", true);
        setField(service, "shadowVersion", 1);
        setField(service, "shadowSlippageBps", 0.0d);
        return service;
    }

    private static ShadowCopyTradingServiceImpl serviceForShadowSync(List<ShadowCopyAllocationEntity> saved) throws Exception {
        ShadowCopyTradingServiceImpl service = new ShadowCopyTradingServiceImpl(
                proxy(ShadowCopyAllocationRepository.class, (method, args) -> {
                    if ("findActiveStrategy".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("save".equals(method.getName())) {
                        ShadowCopyAllocationEntity entity = (ShadowCopyAllocationEntity) args[0];
                        if (!saved.contains(entity)) {
                            saved.add(entity);
                        }
                        return entity;
                    }
                    if ("findActiveByUser".equals(method.getName())) {
                        return List.of();
                    }
                    if ("flush".equals(method.getName())) {
                        return null;
                    }
                    return unexpected(method);
                }),
                proxy(ShadowCopyOperationRepository.class, (method, args) -> unexpected(method)),
                proxy(ShadowCopyOperationEventRepository.class, (method, args) -> unexpected(method)),
                proxy(ShadowPositionStateRepository.class, (method, args) -> {
                    if ("sumClosedRealizedPnlUsdByWalletProfileId".equals(method.getName())
                            || "sumSlippageUsdByWalletProfileId".equals(method.getName())) {
                        return BigDecimal.ZERO;
                    }
                    if ("countClosedPositionsByWalletProfileId".equals(method.getName())
                            || "countOpenPositionsByWalletProfileId".equals(method.getName())) {
                        return 0L;
                    }
                    return unexpected(method);
                }),
                copyWalletProfileRepository(saved),
                shadowProfileValidationRepository(),
                new CopyStrategyRuntimeRouter()
        );
        setField(service, "separateShadowEnabled", true);
        setField(service, "requireShadowValidationBeforeLive", true);
        setField(service, "minShadowClosedOperationsForLive", 5);
        setField(service, "minShadowNetPnlUsdtForLive", BigDecimal.ZERO);
        setField(service, "requirePositiveWindows", "2w,1mo");
        setField(service, "shadowVersion", 1);
        setField(service, "shadowSlippageBps", 0.0d);
        return service;
    }

    private static ShadowCopyAllocationRepository shadowAllocationRepository(Map<UUID, ShadowCopyAllocationEntity> shadowsByUser) {
        return proxy(ShadowCopyAllocationRepository.class, (method, args) -> {
            if ("findActiveStrategy".equals(method.getName())) {
                UUID userId = (UUID) args[0];
                ShadowCopyAllocationEntity shadow = shadowsByUser.get(userId);
                return Optional.ofNullable(shadow);
            }
            return unexpected(method);
        });
    }

    private static CopyWalletProfileRepository copyWalletProfileRepository(List<ShadowCopyAllocationEntity> savedAllocations) {
        return proxy(CopyWalletProfileRepository.class, new Invocation() {
            long nextId = 100L;

            @Override
            public Object invoke(Method method, Object[] args) {
                if ("findByProfileKey".equals(method.getName())) {
                    return Optional.empty();
                }
                if ("save".equals(method.getName())) {
                    CopyWalletProfileEntity profile = (CopyWalletProfileEntity) args[0];
                    if (profile.getId() == null) {
                        profile.setId(nextId++);
                    }
                    return profile;
                }
                return unexpected(method);
            }
        });
    }

    private static ShadowWalletProfileValidationRepository shadowProfileValidationRepository() {
        return proxy(ShadowWalletProfileValidationRepository.class, new Invocation() {
            long nextId = 200L;

            @Override
            public Object invoke(Method method, Object[] args) {
                if ("findFirstByWalletProfileIdOrderByStartedAtDesc".equals(method.getName())) {
                    return Optional.empty();
                }
                if ("save".equals(method.getName())) {
                    ShadowWalletProfileValidationEntity validation = (ShadowWalletProfileValidationEntity) args[0];
                    if (validation.getId() == null) {
                        validation.setId(nextId++);
                    }
                    return validation;
                }
                return unexpected(method);
            }
        });
    }

    private static ShadowPositionStateRepository shadowPositionStateRepository(
            Map<Long, Long> closedByShadow,
            Map<Long, BigDecimal> netByShadow
    ) {
        return proxy(ShadowPositionStateRepository.class, (method, args) -> {
            if ("countClosedPositions".equals(method.getName())) {
                return closedByShadow.getOrDefault((Long) args[0], 0L);
            }
            if ("sumClosedRealizedPnlUsd".equals(method.getName())) {
                return netByShadow.get((Long) args[0]);
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
        throw new AssertionError("Unexpected repository call: " + method.getName());
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
