package com.apunto.engine.service.impl;

import com.apunto.engine.entity.CopyPromotionAuditEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.repository.CopyDispatchIntentRepository;
import com.apunto.engine.repository.MicroLiveExecutionEvidenceProjection;
import com.apunto.engine.repository.CopyPromotionAuditRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.copy.decision.CopyDecisionGateway;
import com.apunto.engine.service.copy.decision.CopyDecisionRequest;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver;
import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import com.apunto.engine.service.copy.promotion.LivePromotionResult;
import com.apunto.engine.service.copy.readiness.MicroLiveExecutionEvidencePolicy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroLivePromotionServiceImplTest {

    @Test
    void genericRuntimeEventsCannotPromoteWhenNoOrderWasActuallySubmitted() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, saved),
                evidenceRepository(0L, 0L, 0L, 0L, 0L, "0", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits),
                properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations()
        );

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.rejected());
        assertEquals(0, result.promoted());
        assertEquals("MICRO_LIVE_NOT_READY_ZERO_SUBMITTED_ORDERS", saved.get().getStatusReason());
    }

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
        assertEquals("copy_all_metric_movements", saved.get().getCopyMode());
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
        assertEquals("MICRO_LIVE_NOT_READY_MIN_SUBMITTED_ORDERS", saved.get().getStatusReason());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_NOT_READY_MIN_SUBMITTED_ORDERS".equals(a.getReasonCode())));
    }

    @Test
    void highMicroLiveErrorRateDoesNotPromote() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        MicroLivePromotionServiceImpl service = service(allocation, 10L, 1L, "2", 8, saved, audits);

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.rejected());
        assertEquals("MICRO_LIVE_NOT_READY_DISPATCH_ERRORS", saved.get().getStatusReason());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_NOT_READY_DISPATCH_ERRORS".equals(a.getReasonCode())));
    }

    @Test
    void microLiveLegacyCopyModeIsRemappedBeforeCreatingLive() {
        UserCopyAllocationEntity allocation = microLiveAllocation("MOVEMENT_ALL", "copy_movement_all_events");
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        MicroLivePromotionServiceImpl service = service(allocation, 12L, 0L, "7.5", 8, saved, audits);

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.promoted());
        assertEquals("LIVE", saved.get().getExecutionMode());
        assertEquals("copy_all_metric_movements", saved.get().getCopyMode());
    }

    @Test
    void microLiveSkipCopyModeIsResolvedFromStrategyAndNotPersisted() {
        UserCopyAllocationEntity allocation = microLiveAllocation("MOVEMENT_ALL", "SKIP");
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        MicroLivePromotionServiceImpl service = service(allocation, 12L, 0L, "7.5", 8, saved, audits);

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.promoted());
        assertEquals("copy_all_metric_movements", saved.get().getCopyMode());
    }

    @Test
    void microLiveShortAndLongStrategiesResolveDbAllowedCopyModes() {
        assertMicroLiveCopyMode("SHORT_ONLY", null, "copy_only_short_events");
        assertMicroLiveCopyMode("LONG_ONLY", null, "copy_only_long_events");
    }

    @Test
    void microLiveInvalidCopyModeMappingRejectsCandidate() {
        UserCopyAllocationEntity allocation = microLiveAllocation("UNKNOWN_PROFILE", "SKIP");
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        MicroLivePromotionServiceImpl service = service(allocation, 12L, 0L, "7.5", 8, saved, audits);

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.rejected());
        assertEquals("INVALID_COPY_MODE_MAPPING", saved.get().getStatusReason());
        assertTrue(audits.stream().anyMatch(a -> "INVALID_COPY_MODE_MAPPING".equals(a.getReasonCode())));
    }

    @Test
    void existingLiveAllocationIsNoopAndDoesNotDuplicate() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        UserCopyAllocationEntity existingLive = liveAllocation(allocation);
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, existingLive, saved, false),
                evidenceRepository(12L, 12L, 12L, 3L, 0L, "7.5", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits),
                properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations()
        );

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.skipped());
        assertEquals(0, result.promoted());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_PROMOTION_NOOP".equals(a.getDecision())
                && "LIVE_ALLOCATION_ALREADY_EXISTS".equals(a.getReasonCode())));
    }

    @Test
    void duplicateLiveConstraintIsTreatedAsNoopWhenLiveAppearsOnRequery() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        UserCopyAllocationEntity existingLive = liveAllocation(allocation);
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, existingLive, saved, true),
                evidenceRepository(12L, 12L, 12L, 3L, 0L, "7.5", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits),
                properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations()
        );

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.skipped());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_PROMOTION_NOOP".equals(a.getDecision())));
    }

    @Test
    void runtimeEvidenceMustPassBeforeCallingFullDecision() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        CapturingCopyDecisionGateway gateway = new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE"));

        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, saved),
                evidenceRepository(1L, 1L, 1L, 1L, 0L, "1", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits),
                properties,
                gateway,
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations()
        );

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.rejected());
        assertEquals(0, gateway.calls.get());
    }

    @Test
    void fullDecisionCanBlockMicroLivePromotionToLive() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        CapturingCopyDecisionGateway gateway = new CapturingCopyDecisionGateway(fullDecisionBlocked("NEGATIVE_REQUIRED_WINDOW_1MO"));

        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, saved),
                evidenceRepository(12L, 12L, 12L, 3L, 0L, "7.5", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits),
                properties,
                gateway,
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations()
        );

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.rejected());
        assertEquals(1, gateway.calls.get());
        assertEquals("live-entry", gateway.lastRequest.mode());
        assertEquals("FULL_DECISION_BLOCKED_BY_COPY_GUARD", saved.get().getStatusReason());
        assertTrue(audits.stream().anyMatch(a -> "FULL_DECISION_BLOCKED_BY_COPY_GUARD".equals(a.getReasonCode())));
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
        LivePromotionProperties properties = properties();
        return new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, saved),
                evidenceRepository(events, events, events, Math.min(events, 3L), errors, pnl, OffsetDateTime.now().minusDays(days)),
                auditRepository(audits),
                properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations()
        );
    }

    private static LivePromotionProperties properties() {
        LivePromotionProperties properties = new LivePromotionProperties();
        properties.setEnabled(true);
        properties.setMinMicroDays(7);
        properties.setMinMicroOrders(2);
        properties.setMinSubmittedOrders(2);
        properties.setMinAcknowledgedOrders(2);
        properties.setMinFilledOrders(2);
        properties.setMinClosedOperations(1);
        properties.setMinSlippageSamples(1);
        properties.setMaxDispatchErrors(0);
        properties.setMaxReconciliationPending(0);
        properties.setMaxDuplicateCount(0);
        properties.setMaxUnresolvedAmbiguousTimeouts(0);
        properties.setMaxErrorRatePct(new BigDecimal("5"));
        properties.setRequirePositiveNetPnl(false);
        properties.setRequireFullDecisionForLive(true);
        return properties;
    }

    private static CopyDecisionDto fullDecisionAllowed(boolean canMicroLive, boolean canLive, String reasonCode) {
        CopyDecisionDto dto = new CopyDecisionDto();
        dto.setWalletId("0xabc");
        dto.setStrategyCode("MOVEMENT_ALL");
        dto.setScopeType("ALL");
        dto.setScopeValue("ALL");
        dto.setMode(canLive ? "live-entry" : "micro-live-entry");
        dto.setSimulationMode("full");
        dto.setFullMaterialized(true);
        dto.setFactPayloadLoaded(true);
        dto.setDecisionFinal(true);
        dto.setRequiresFullSimulation(false);
        dto.setCanMicroLive(canMicroLive);
        dto.setCanLive(canLive);
        dto.setReasonCode(reasonCode);
        CopyDecisionDto.CopyGuardDto guard = new CopyDecisionDto.CopyGuardDto();
        guard.setStatus("OK");
        guard.setAction("ALLOW");
        guard.setAllowNewEntries(true);
        dto.setCopyGuard(guard);
        return dto;
    }

    private static CopyDecisionDto fullDecisionBlocked(String reason) {
        CopyDecisionDto dto = fullDecisionAllowed(false, false, "FULL_DECISION_BLOCKED_BY_COPY_GUARD");
        CopyDecisionDto.CopyGuardDto guard = new CopyDecisionDto.CopyGuardDto();
        guard.setStatus("BLOCKED");
        guard.setAction("PAUSE_OPEN");
        guard.setAllowNewEntries(false);
        guard.setReasons(List.of(reason));
        dto.setCopyGuard(guard);
        return dto;
    }

    private static final class CapturingCopyDecisionGateway implements CopyDecisionGateway {
        private final AtomicInteger calls = new AtomicInteger();
        private final CopyDecisionDto response;
        private CopyDecisionRequest lastRequest;

        private CapturingCopyDecisionGateway(CopyDecisionDto response) {
            this.response = response;
        }

        @Override
        public CopyDecisionDto getFullDecisionExact(CopyDecisionRequest request) {
            this.lastRequest = request;
            this.calls.incrementAndGet();
            return response;
        }
    }

    private static UserCopyAllocationEntity microLiveAllocation() {
        return microLiveAllocation("MOVEMENT_ALL", null);
    }

    private static UserCopyAllocationEntity microLiveAllocation(String strategyCode, String copyMode) {
        return UserCopyAllocationEntity.builder()
                .id(400L)
                .idUser(UUID.randomUUID())
                .walletId("0xabc")
                .copyStrategyCode(strategyCode)
                .copyMode(copyMode)
                .scopeType("strategy")
                .scopeValue(strategyCode)
                .strategyKey("0xabc|" + strategyCode + "|strategy|" + strategyCode)
                .allocationPct(new BigDecimal("0.500000"))
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .executionMode("MICRO_LIVE")
                .linkedShadowAllocationId(10L)
                .promotedFromShadowAt(OffsetDateTime.now().minusDays(8))
                .updatedAt(OffsetDateTime.now().minusDays(8))
                .build();
    }

    private static UserCopyAllocationEntity liveAllocation(UserCopyAllocationEntity micro) {
        return UserCopyAllocationEntity.builder()
                .id(401L)
                .idUser(micro.getIdUser())
                .walletId(micro.getWalletId())
                .copyStrategyCode(micro.getCopyStrategyCode())
                .copyMode("copy_all_metric_movements")
                .scopeType(micro.getScopeType())
                .scopeValue(micro.getScopeValue())
                .allocationPct(micro.getAllocationPct())
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .executionMode("LIVE")
                .build();
    }

    private static void assertMicroLiveCopyMode(String strategyCode, String sourceCopyMode, String expectedCopyMode) {
        UserCopyAllocationEntity allocation = microLiveAllocation(strategyCode, sourceCopyMode);
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        MicroLivePromotionServiceImpl service = service(allocation, 12L, 0L, "7.5", 8, saved, new ArrayList<>());

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.promoted());
        assertEquals(expectedCopyMode, saved.get().getCopyMode());
    }

    private static UserCopyAllocationRepository allocationRepository(
            UserCopyAllocationEntity allocation,
            AtomicReference<UserCopyAllocationEntity> saved
    ) {
        return allocationRepository(allocation, null, saved, false);
    }

    private static UserCopyAllocationRepository allocationRepository(
            UserCopyAllocationEntity allocation,
            UserCopyAllocationEntity existingLive,
            AtomicReference<UserCopyAllocationEntity> saved,
            boolean throwDuplicateOnLiveSave
    ) {
        AtomicReference<Boolean> firstLiveLookup = new AtomicReference<>(true);
        return proxy(UserCopyAllocationRepository.class, (method, args) -> {
            if ("findMicroLivePromotionCandidates".equals(method.getName())) return List.of(allocation);
            if ("findOpenLiveAllocationForUserWalletStrategyScope".equals(method.getName())) {
                if (existingLive == null) return Optional.empty();
                return throwDuplicateOnLiveSave && firstLiveLookup.getAndSet(false)
                        ? Optional.empty()
                        : Optional.of(existingLive);
            }
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) {
                UserCopyAllocationEntity entity = (UserCopyAllocationEntity) args[0];
                if ("LIVE".equals(entity.getExecutionMode()) && throwDuplicateOnLiveSave) {
                    throw new DataIntegrityViolationException("duplicate live");
                }
                if (entity.getId() == null) {
                    entity.setId(401L);
                }
                if ("LIVE".equals(entity.getExecutionMode())) {
                    assertTrue(UserCopyAllocationCopyModeResolver.isAllowedCopyMode(entity.getCopyMode()));
                }
                saved.set(entity);
                return entity;
            }
            return unexpected(method);
        });
    }

    private static CopyDispatchIntentRepository evidenceRepository(
            long submitted,
            long acknowledged,
            long filled,
            long closed,
            long errors,
            String pnl,
            OffsetDateTime firstSubmittedAt
    ) {
        MicroLiveExecutionEvidenceProjection projection = proxy(MicroLiveExecutionEvidenceProjection.class, (method, args) -> switch (method.getName()) {
            case "getAllocationId" -> 400L;
            case "getSubmittedOrders" -> submitted;
            case "getAcknowledgedOrders" -> acknowledged;
            case "getFilledOrders" -> filled;
            case "getClosedOperations" -> closed;
            case "getDispatchErrors" -> errors;
            case "getReconciliationPending", "getDuplicateCount", "getUnresolvedAmbiguousTimeouts" -> 0L;
            case "getSlippageSamples" -> submitted;
            case "getRealizedPnlUsd" -> new BigDecimal(pnl);
            case "getMaxDrawdownUsd" -> BigDecimal.ZERO;
            case "getAdverseSlippageP95Bps" -> new BigDecimal("1");
            case "getFirstSubmittedAt" -> firstSubmittedAt;
            default -> unexpected(method);
        });
        return proxy(CopyDispatchIntentRepository.class, (method, args) -> {
            if ("findMicroLiveExecutionEvidence".equals(method.getName())) return List.of(projection);
            return unexpected(method);
        });
    }

    private static TransactionOperations transactionOperations() {
        PlatformTransactionManager manager = new PlatformTransactionManager() {
            @Override
            public TransactionStatus getTransaction(TransactionDefinition definition) {
                return new SimpleTransactionStatus();
            }

            @Override
            public void commit(TransactionStatus status) {
            }

            @Override
            public void rollback(TransactionStatus status) {
            }
        };
        return new TransactionTemplate(manager);
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
