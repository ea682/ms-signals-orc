package com.apunto.engine.service.impl;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import com.apunto.engine.repository.ShadowCopyAllocationRepository;
import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowCopyOperationRepository;
import com.apunto.engine.repository.ShadowPositionStateRepository;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
        return ShadowCopyAllocationEntity.builder()
                .id(id)
                .idUser(userId)
                .walletId("0xabc")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("strategy")
                .scopeValue("MOVEMENT_ALL")
                .shadowVersion(1)
                .active(true)
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
