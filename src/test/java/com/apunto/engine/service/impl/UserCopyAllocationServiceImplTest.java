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
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                shadowService(shadowSyncCalls, syncedShadowCandidates)
        );
        setField(service, "entityManager", entityManager());

        service.syncDistribution(List.of(), List.of(shadowCandidate));

        assertEquals(1, shadowSyncCalls.get());
        assertSame(shadowCandidate, syncedShadowCandidates.get().get(0));
    }

    private static UserCopyAllocationRepository repository() {
        return proxy(UserCopyAllocationRepository.class, (method, args) -> {
            if ("findAllByIdUserAndEndsAtIsNull".equals(method.getName())) {
                return List.of();
            }
            if ("saveAll".equals(method.getName()) || "saveAllAndFlush".equals(method.getName())) {
                return args[0];
            }
            return unexpected(method);
        });
    }

    private static ShadowCopyTradingService shadowService(
            AtomicInteger shadowSyncCalls,
            AtomicReference<List<MetricaWalletDto>> syncedShadowCandidates
    ) {
        return new ShadowCopyTradingService() {
            @Override
            public void syncShadowAllocations(UUID idUser, List<MetricaWalletDto> candidates, int userMaxWallet, OffsetDateTime now) {
                shadowSyncCalls.incrementAndGet();
                syncedShadowCandidates.set(candidates);
            }

            @Override
            public void linkLiveAllocations(UUID idUser, List<UserCopyAllocationEntity> liveAllocations) {
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
                return false;
            }
        };
    }

    private static UserDetailDto activeUser(UUID userId, int maxWallet) {
        UserEntity user = new UserEntity();
        user.setId(userId);
        DetailUserEntity detail = new DetailUserEntity();
        detail.setMaxWallet(maxWallet);
        return new UserDetailDto(user, detail, null);
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
                .capitalShare(1.0)
                .build();
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
