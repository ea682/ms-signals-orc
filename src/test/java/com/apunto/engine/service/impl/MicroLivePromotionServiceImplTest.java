package com.apunto.engine.service.impl;

import com.apunto.engine.entity.CopyPromotionAuditEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.CopyOperationEventRepository;
import com.apunto.engine.repository.CopyPromotionAuditRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import com.apunto.engine.service.copy.promotion.LivePromotionResult;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroLivePromotionServiceImplTest {

    @Test
    void approvedMicroLiveClosesMicroAndCreatesLiveAllocationAndAudits() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        MicroLivePromotionServiceImpl service = service(allocation, 12L, 0L, "7.5", 8, saved, audits);

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.evaluated());
        assertEquals(1, result.promoted());
        assertEquals(UserCopyAllocationEntity.Status.CLOSED, allocation.getStatus());
        assertTrue(!allocation.isActive());
        assertEquals("PROMOTED_MICRO_TO_LIVE_CLOSED", allocation.getStatusReason());
        assertEquals(401L, saved.get().getId());
        assertEquals("LIVE", saved.get().getExecutionMode());
        assertEquals("PROMOTED_MICRO_TO_LIVE", saved.get().getStatusReason());
        assertEquals(UserCopyAllocationEntity.Status.ACTIVE, saved.get().getStatus());
        assertTrue(audits.stream().anyMatch(a -> "LIVE_CREATED".equals(a.getDecision())));
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_CLOSED_FOR_LIVE".equals(a.getDecision())
                && allocation.getId().equals(a.getMicroLiveAllocationId())));
        assertTrue(audits.stream().anyMatch(a -> allocation.getId().equals(a.getMicroLiveAllocationId())
                && Long.valueOf(401L).equals(a.getLiveAllocationId())));
    }

    @Test
    void insufficientMicroOrdersDoesNotPromoteAndAuditsReason() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        MicroLivePromotionServiceImpl service = service(allocation, 1L, 0L, "2", 8, saved, audits);

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.rejected());
        assertEquals("MICRO_LIVE", saved.get().getExecutionMode());
        assertEquals("MICRO_LIVE_NOT_READY_MIN_ORDERS", saved.get().getStatusReason());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_NOT_READY_MIN_ORDERS".equals(a.getReasonCode())));
    }

    @Test
    void highMicroLiveErrorRateDoesNotPromote() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        MicroLivePromotionServiceImpl service = service(allocation, 10L, 1L, "2", 8, saved, audits);

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.rejected());
        assertEquals("MICRO_LIVE_NOT_READY_ERROR_RATE", saved.get().getStatusReason());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_NOT_READY_ERROR_RATE".equals(a.getReasonCode())));
    }

    private static MicroLivePromotionServiceImpl service(
            UserCopyAllocationEntity allocation,
            long events,
            long errors,
            String pnl,
            long days,
            AtomicReference<UserCopyAllocationEntity> saved,
            List<CopyPromotionAuditEntity> audits
    ) {
        return new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, saved),
                eventRepository(events, errors, new BigDecimal(pnl), OffsetDateTime.now().minusDays(days)),
                auditRepository(audits),
                properties()
        );
    }

    private static LivePromotionProperties properties() {
        LivePromotionProperties properties = new LivePromotionProperties();
        properties.setEnabled(true);
        properties.setMinMicroDays(7);
        properties.setMinMicroOrders(2);
        properties.setMaxErrorRatePct(new BigDecimal("5"));
        properties.setRequirePositiveNetPnl(false);
        return properties;
    }

    private static UserCopyAllocationEntity microLiveAllocation() {
        return UserCopyAllocationEntity.builder()
                .id(400L)
                .idUser(UUID.randomUUID())
                .walletId("0xabc")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("strategy")
                .scopeValue("MOVEMENT_ALL")
                .strategyKey("0xabc|MOVEMENT_ALL|strategy|MOVEMENT_ALL")
                .allocationPct(new BigDecimal("0.500000"))
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .executionMode("MICRO_LIVE")
                .linkedShadowAllocationId(10L)
                .promotedFromShadowAt(OffsetDateTime.now().minusDays(8))
                .updatedAt(OffsetDateTime.now().minusDays(8))
                .build();
    }

    private static UserCopyAllocationRepository allocationRepository(
            UserCopyAllocationEntity allocation,
            AtomicReference<UserCopyAllocationEntity> saved
    ) {
        return proxy(UserCopyAllocationRepository.class, (method, args) -> {
            if ("findMicroLivePromotionCandidates".equals(method.getName())) return List.of(allocation);
            if ("findOpenLiveAllocationForUserWalletStrategyScope".equals(method.getName())) return Optional.empty();
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) {
                UserCopyAllocationEntity entity = (UserCopyAllocationEntity) args[0];
                if (entity.getId() == null) {
                    entity.setId(401L);
                }
                saved.set(entity);
                return entity;
            }
            return unexpected(method);
        });
    }

    private static CopyOperationEventRepository eventRepository(
            long events,
            long errors,
            BigDecimal pnl,
            OffsetDateTime firstEventAt
    ) {
        return proxy(CopyOperationEventRepository.class, (method, args) -> {
            return switch (method.getName()) {
                case "countRuntimeEventsForAllocation" -> events;
                case "countRuntimeErrorEventsForAllocation" -> errors;
                case "sumRuntimeRealizedPnlUsdForAllocation" -> pnl;
                case "findFirstRuntimeEventTimeForAllocation" -> firstEventAt;
                default -> unexpected(method);
            };
        });
    }

    private static CopyPromotionAuditRepository auditRepository(List<CopyPromotionAuditEntity> audits) {
        return proxy(CopyPromotionAuditRepository.class, (method, args) -> {
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) {
                CopyPromotionAuditEntity entity = (CopyPromotionAuditEntity) args[0];
                entity.setId(500L + audits.size());
                audits.add(entity);
                return entity;
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
        throw new AssertionError("Unexpected call: " + method);
    }

    private interface Invocation {
        Object invoke(Method method, Object[] args);
    }
}
