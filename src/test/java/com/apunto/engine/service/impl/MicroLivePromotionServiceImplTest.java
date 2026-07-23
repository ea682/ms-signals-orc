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
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolution;
import com.apunto.engine.service.copy.allocation.LiveAllocationPercentageResolver;
import com.apunto.engine.service.copy.distribution.CopyDistributionUnitExecutor;
import com.apunto.engine.service.copy.distribution.CopyDistributionUnitExecutor.UnitMutationResult;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver;
import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import com.apunto.engine.service.copy.promotion.LivePromotionResult;
import com.apunto.engine.service.copy.readiness.MicroLiveExecutionEvidencePolicy;
import com.apunto.engine.service.copy.lifecycle.MicroLiveFlatness;
import com.apunto.engine.service.copy.lifecycle.MicroLiveFlatnessGate;
import com.apunto.engine.service.copy.certification.LiveUserRuntimeEligibilityGate;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroLivePromotionServiceImplTest {

    @Test
    void legacyBulkPromotionCannotCreateLiveWhenManualCertificationIsRequired() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        LivePromotionProperties properties = properties();
        properties.setManualCertificationRequired(true);
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, saved),
                evidenceRepository(12L, 12L, 12L, 0L, 0L, "7.5", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits), properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations());

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(0, result.promoted());
        assertTrue(allocation.isActive());
        assertEquals("MICRO_LIVE", allocation.getExecutionMode());
    }

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
        assertEquals(new BigDecimal("0.120000"), saved.get().getAllocationPct());
        assertEquals("PERCENTAGE", saved.get().getSizingMode());
        assertEquals("SIGNALS_CURRENT_LIVE_DISTRIBUTION", saved.get().getAllocationPctSource());
        assertTrue(saved.get().getAllocationPctSourceId() != null);
        assertEquals(new BigDecimal("0.120000"), saved.get().getWalletTotalAllocationPct());
        assertTrue(audits.stream().anyMatch(a -> "LIVE_CREATED".equals(a.getDecision())));
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_CLOSED_FOR_LIVE".equals(a.getDecision())
                && allocation.getId().equals(a.getMicroLiveAllocationId())));
        assertTrue(audits.stream().anyMatch(a -> allocation.getId().equals(a.getMicroLiveAllocationId())
                && Long.valueOf(401L).equals(a.getLiveAllocationId())));
    }

    @Test
    void failedLiveInsertRollsBackMicroCloseAndLeavesNoLiveAllocation() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        AtomicInteger liveInsertAttempts = new AtomicInteger();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        UserCopyAllocationRepository repository = proxy(UserCopyAllocationRepository.class, (method, args) -> {
            if ("findMicroLivePromotionCandidates".equals(method.getName())) return List.of(allocation);
            if ("findOpenLiveAllocationForUserWalletStrategyScope".equals(method.getName())) return Optional.empty();
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) {
                UserCopyAllocationEntity entity = (UserCopyAllocationEntity) args[0];
                if ("LIVE".equals(entity.getExecutionMode())) {
                    liveInsertAttempts.incrementAndGet();
                    throw new IllegalStateException("simulated live insert failure");
                }
                return entity;
            }
            return unexpected(method);
        });
        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                repository,
                evidenceRepository(12L, 12L, 12L, 3L, 0L, "7.5", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits),
                properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                rollbackRestoringTransactionOperations(allocation)
        );
        setField(service, "liveAllocationPercentageResolver",
                (LiveAllocationPercentageResolver) request -> resolvedPercentage("0.120000", "0.120000"));

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.rejected());
        assertEquals(0, result.promoted());
        assertEquals(1, liveInsertAttempts.get());
        assertEquals("MICRO_LIVE", allocation.getExecutionMode());
        assertEquals(UserCopyAllocationEntity.Status.ACTIVE, allocation.getStatus());
        assertTrue(allocation.isActive());
        assertEquals(null, allocation.getEndsAt());
        assertTrue(audits.stream().anyMatch(a -> "PROMOTION_FAILED".equals(a.getReasonCode())));
    }

    @Test
    void legacyMicroPercentageCannotBeCopiedToLiveWithoutCurrentDistribution() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        allocation.setAllocationPct(new BigDecimal("0.000001"));
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        MicroLivePromotionServiceImpl service = service(
                allocation,
                12L,
                0L,
                "7.5",
                8,
                saved,
                audits,
                LiveAllocationPercentageResolution.rejected("LIVE_DISTRIBUTION_NOT_AVAILABLE")
        );

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(0, result.promoted());
        assertEquals(1, result.rejected());
        assertEquals("MICRO_LIVE", allocation.getExecutionMode());
        assertTrue(allocation.isActive());
        assertEquals(UserCopyAllocationEntity.Status.ACTIVE, allocation.getStatus());
        assertEquals("LIVE_DISTRIBUTION_NOT_AVAILABLE", saved.get().getStatusReason());
    }

    @Test
    void currentDistributionAtPromotionReplacesHistoricalMicroPercentage() {
        UserCopyAllocationEntity allocation = microLiveAllocation();
        allocation.setAllocationPct(new BigDecimal("0.150000"));
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();

        MicroLivePromotionServiceImpl service = service(
                allocation,
                12L,
                0L,
                "7.5",
                8,
                saved,
                new ArrayList<>(),
                resolvedPercentage("0.070000", "0.100000")
        );

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.promoted());
        assertEquals(new BigDecimal("0.070000"), saved.get().getAllocationPct());
        assertEquals("LIVE", saved.get().getExecutionMode());
    }

    @Test
    void invalidMissingPlaceholderAndStaleLivePercentagesAllKeepMicroLiveActive() {
        OffsetDateTime calculatedAt = OffsetDateTime.now().minusMinutes(1);
        UUID sourceId = UUID.randomUUID();
        List<InvalidPercentageCase> cases = List.of(
                new InvalidPercentageCase(
                        LiveAllocationPercentageResolution.rejected("LIVE_ALLOCATION_PCT_MISSING"),
                        "LIVE_ALLOCATION_PCT_MISSING"),
                new InvalidPercentageCase(
                        LiveAllocationPercentageResolution.resolved(
                                BigDecimal.ZERO, new BigDecimal("0.100000"),
                                "SIGNALS_CURRENT_LIVE_DISTRIBUTION", sourceId,
                                calculatedAt, calculatedAt.plusMinutes(5)),
                        "LIVE_ALLOCATION_PCT_INVALID"),
                new InvalidPercentageCase(
                        LiveAllocationPercentageResolution.resolved(
                                new BigDecimal("-0.010000"), new BigDecimal("0.100000"),
                                "SIGNALS_CURRENT_LIVE_DISTRIBUTION", sourceId,
                                calculatedAt, calculatedAt.plusMinutes(5)),
                        "LIVE_ALLOCATION_PCT_INVALID"),
                new InvalidPercentageCase(
                        LiveAllocationPercentageResolution.resolved(
                                new BigDecimal("1.010000"), new BigDecimal("1.010000"),
                                "SIGNALS_CURRENT_LIVE_DISTRIBUTION", sourceId,
                                calculatedAt, calculatedAt.plusMinutes(5)),
                        "LIVE_ALLOCATION_PCT_INVALID"),
                new InvalidPercentageCase(
                        LiveAllocationPercentageResolution.resolved(
                                new BigDecimal("0.000001"), new BigDecimal("0.100000"),
                                "LEGACY_MICRO_LIVE_SENTINEL", sourceId,
                                calculatedAt, calculatedAt.plusMinutes(5)),
                        "LIVE_ALLOCATION_PCT_INVALID"),
                new InvalidPercentageCase(
                        LiveAllocationPercentageResolution.resolved(
                                new BigDecimal("0.100000"), new BigDecimal("0.100000"),
                                "SIGNALS_CURRENT_LIVE_DISTRIBUTION", sourceId,
                                calculatedAt.minusMinutes(10), calculatedAt.minusMinutes(5)),
                        "LIVE_ALLOCATION_PCT_STALE")
        );

        for (InvalidPercentageCase invalid : cases) {
            UserCopyAllocationEntity allocation = microLiveAllocation();
            AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
            MicroLivePromotionServiceImpl service = service(
                    allocation, 12L, 0L, "7.5", 8, saved, new ArrayList<>(), invalid.resolution());

            LivePromotionResult result = service.promoteMicroLiveToLive();

            assertEquals(0, result.promoted(), invalid.expectedReason());
            assertEquals(1, result.rejected(), invalid.expectedReason());
            assertTrue(allocation.isActive(), invalid.expectedReason());
            assertEquals("MICRO_LIVE", allocation.getExecutionMode(), invalid.expectedReason());
            assertEquals(invalid.expectedReason(), saved.get().getStatusReason());
        }
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
    void promotionWithOpenMicroPositionCreatesLiveButKeepsMicroExitOnlyAndReserved() {
        UserCopyAllocationEntity micro = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        MicroLivePromotionServiceImpl service = service(micro, 12L, 0L, "7.5", 8, saved, audits);
        setField(service, "microLiveFlatnessGate",
                (MicroLiveFlatnessGate) allocationId -> new MicroLiveFlatness(false, 1, 0));

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.promoted());
        assertEquals("LIVE", saved.get().getExecutionMode());
        assertEquals(UserCopyAllocationEntity.Status.EXIT_ONLY, micro.getStatus());
        assertTrue(micro.isActive());
        assertEquals(null, micro.getEndsAt());
        assertEquals("MICRO_LIVE_PROMOTED_EXIT_ONLY_PENDING_FLAT", micro.getStatusReason());
    }

    @Test
    void degradedLiveIsReusedAndReactivatedAfterValidMicroRecertification() {
        UserCopyAllocationEntity micro = microLiveAllocation();
        UUID certificationId = UUID.randomUUID();
        micro.setLiveCertificationId(certificationId);
        UserCopyAllocationEntity degradedLive = liveAllocation(micro);
        degradedLive.setLiveCertificationId(certificationId);
        degradedLive.setStatus(UserCopyAllocationEntity.Status.EXIT_ONLY);
        degradedLive.setStatusReason("LIVE_CERTIFICATION_LIVE_DEGRADED");
        AtomicInteger liveRowsCreated = new AtomicInteger();
        List<UserCopyAllocationEntity> saved = new ArrayList<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        UserCopyAllocationRepository repository = proxy(UserCopyAllocationRepository.class, (method, args) -> {
            if ("findMicroLivePromotionCandidates".equals(method.getName())) return List.of(micro);
            if ("findOpenLiveAllocationForUserWalletStrategyScope".equals(method.getName())) {
                return Optional.of(degradedLive);
            }
            if ("findById".equals(method.getName())) return Optional.of(micro);
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) {
                UserCopyAllocationEntity entity = (UserCopyAllocationEntity) args[0];
                if ("LIVE".equals(entity.getExecutionMode()) && entity.getId() == null) {
                    liveRowsCreated.incrementAndGet();
                    entity.setId(999L);
                }
                saved.add(entity);
                return entity;
            }
            return unexpected(method);
        });
        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                repository,
                evidenceRepository(12L, 12L, 12L, 3L, 0L, "7.5", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits), properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations());
        setField(service, "liveAllocationPercentageResolver",
                (LiveAllocationPercentageResolver) request -> resolvedPercentage("0.120000", "0.120000"));
        setField(service, "liveUserRuntimeEligibilityGate",
                (LiveUserRuntimeEligibilityGate) allocation -> LiveUserRuntimeEligibilityGate.Decision.permit());

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.promoted());
        assertEquals(0, liveRowsCreated.get(), "recertification must never insert a second LIVE allocation");
        assertEquals(401L, degradedLive.getId());
        assertEquals(UserCopyAllocationEntity.Status.ACTIVE, degradedLive.getStatus());
        assertTrue(degradedLive.isActive());
        assertEquals("LIVE_RECERTIFIED", degradedLive.getStatusReason());
        assertEquals(UserCopyAllocationEntity.Status.CLOSED, micro.getStatus());
        assertTrue(saved.stream().anyMatch(entity -> entity == degradedLive));
        assertTrue(audits.stream().anyMatch(audit -> "LIVE_RECERTIFIED".equals(audit.getDecision())));
    }

    @Test
    void degradedLiveIsNotReactivatedWhenUserRuntimeEligibilityChanged() {
        UserCopyAllocationEntity micro = microLiveAllocation();
        UUID certificationId = UUID.randomUUID();
        micro.setLiveCertificationId(certificationId);
        UserCopyAllocationEntity degradedLive = liveAllocation(micro);
        degradedLive.setLiveCertificationId(certificationId);
        degradedLive.setStatus(UserCopyAllocationEntity.Status.EXIT_ONLY);
        degradedLive.setStatusReason("LIVE_CERTIFICATION_LIVE_DEGRADED");
        AtomicReference<UserCopyAllocationEntity> saved = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                allocationRepository(micro, degradedLive, saved, false),
                evidenceRepository(12L, 12L, 12L, 3L, 0L, "7.5", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits), properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations());
        setField(service, "liveAllocationPercentageResolver",
                (LiveAllocationPercentageResolver) request -> resolvedPercentage("0.120000", "0.120000"));
        setField(service, "liveUserRuntimeEligibilityGate",
                (LiveUserRuntimeEligibilityGate) allocation ->
                        LiveUserRuntimeEligibilityGate.Decision.block("COPY_WALLET_BLOCKED_BY_USER"));

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(0, result.promoted());
        assertEquals(1, result.rejected());
        assertEquals(UserCopyAllocationEntity.Status.EXIT_ONLY, degradedLive.getStatus());
        assertTrue(degradedLive.isActive());
        assertEquals("LIVE_CERTIFICATION_LIVE_DEGRADED", degradedLive.getStatusReason());
        assertEquals("COPY_WALLET_BLOCKED_BY_USER", micro.getStatusReason());
        assertTrue(audits.stream().anyMatch(audit ->
                "COPY_WALLET_BLOCKED_BY_USER".equals(audit.getReasonCode())));
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
        setField(service, "liveAllocationPercentageResolver",
                (LiveAllocationPercentageResolver) request -> resolvedPercentage("0.120000", "0.120000"));

        LivePromotionResult result = service.promoteMicroLiveToLive();

        assertEquals(1, result.skipped());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_PROMOTION_NOOP".equals(a.getDecision())));
    }

    @Test
    void simultaneousPromotionsCreateExactlyOneLiveAllocation() throws Exception {
        UserCopyAllocationEntity micro = microLiveAllocation();
        AtomicReference<UserCopyAllocationEntity> microState = new AtomicReference<>(micro);
        AtomicReference<UserCopyAllocationEntity> liveState = new AtomicReference<>();
        AtomicInteger liveInserts = new AtomicInteger();
        CyclicBarrier evaluationBarrier = new CyclicBarrier(2);
        List<CopyPromotionAuditEntity> audits = new CopyOnWriteArrayList<>();
        UserCopyAllocationRepository repository = concurrentAllocationRepository(
                microState, liveState, liveInserts, evaluationBarrier);
        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                repository,
                evidenceRepository(12L, 12L, 12L, 3L, 0L, "7.5", OffsetDateTime.now().minusDays(8)),
                auditRepository(audits),
                properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations()
        );
        setField(service, "liveAllocationPercentageResolver",
                (LiveAllocationPercentageResolver) request -> resolvedPercentage("0.120000", "0.120000"));
        setField(service, "copyDistributionUnitExecutor", new SynchronizedProfileExecutor());

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<LivePromotionResult> first = pool.submit(() -> {
                start.await(5, TimeUnit.SECONDS);
                return service.promoteMicroLiveToLive();
            });
            Future<LivePromotionResult> second = pool.submit(() -> {
                start.await(5, TimeUnit.SECONDS);
                return service.promoteMicroLiveToLive();
            });
            start.countDown();

            LivePromotionResult firstResult = first.get(10, TimeUnit.SECONDS);
            LivePromotionResult secondResult = second.get(10, TimeUnit.SECONDS);

            assertEquals(1, firstResult.promoted() + secondResult.promoted());
            assertEquals(1, firstResult.skipped() + secondResult.skipped());
            assertEquals(1, liveInserts.get());
            assertTrue(liveState.get() != null);
            assertEquals(new BigDecimal("0.120000"), liveState.get().getAllocationPct());
            assertEquals(UserCopyAllocationEntity.Status.CLOSED, microState.get().getStatus());
            assertTrue(!microState.get().isActive());
        } finally {
            pool.shutdownNow();
        }
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
        return service(allocation, events, errors, pnl, days, saved, audits,
                resolvedPercentage("0.120000", "0.120000"));
    }

    private static MicroLivePromotionServiceImpl service(
            UserCopyAllocationEntity allocation,
            long events,
            long errors,
            String pnl,
            long days,
            AtomicReference<UserCopyAllocationEntity> saved,
            List<CopyPromotionAuditEntity> audits,
            LiveAllocationPercentageResolution percentageResolution
    ) {
        LivePromotionProperties properties = properties();
        MicroLivePromotionServiceImpl service = new MicroLivePromotionServiceImpl(
                allocationRepository(allocation, saved),
                evidenceRepository(events, events, events, Math.min(events, 3L), errors, pnl, OffsetDateTime.now().minusDays(days)),
                auditRepository(audits),
                properties,
                new CapturingCopyDecisionGateway(fullDecisionAllowed(false, true, "FULL_DECISION_OK_FOR_LIVE")),
                new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry()),
                transactionOperations()
        );
        setField(service, "liveAllocationPercentageResolver",
                (LiveAllocationPercentageResolver) request -> percentageResolution);
        return service;
    }

    private static LiveAllocationPercentageResolution resolvedPercentage(String strategyPct, String walletPct) {
        OffsetDateTime calculatedAt = OffsetDateTime.now().minusMinutes(1);
        return LiveAllocationPercentageResolution.resolved(
                new BigDecimal(strategyPct),
                new BigDecimal(walletPct),
                "SIGNALS_CURRENT_LIVE_DISTRIBUTION",
                UUID.randomUUID(),
                calculatedAt,
                calculatedAt.plusMinutes(5)
        );
    }

    private static void setField(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("missing test injection field " + fieldName, failure);
        }
    }

    private static LivePromotionProperties properties() {
        LivePromotionProperties properties = new LivePromotionProperties();
        properties.setEnabled(true);
        properties.setManualCertificationRequired(false);
        properties.setMinMicroDays(7);
        properties.setMinMicroOrders(2);
        properties.setMinSubmittedOrders(2);
        properties.setMinAcknowledgedOrders(2);
        properties.setMinFilledOrders(2);
        properties.setMinClosedOperations(1);
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

    private record InvalidPercentageCase(
            LiveAllocationPercentageResolution resolution,
            String expectedReason
    ) {
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

    private static UserCopyAllocationRepository concurrentAllocationRepository(
            AtomicReference<UserCopyAllocationEntity> microState,
            AtomicReference<UserCopyAllocationEntity> liveState,
            AtomicInteger liveInserts,
            CyclicBarrier evaluationBarrier
    ) {
        AtomicInteger initialLiveLookups = new AtomicInteger();
        return proxy(UserCopyAllocationRepository.class, (method, args) -> {
            switch (method.getName()) {
                case "findMicroLivePromotionCandidates":
                    return List.of(microState.get());
                case "findById":
                    return Optional.ofNullable(microState.get());
                case "findOpenLiveAllocationForUserWalletStrategyScope":
                    if (initialLiveLookups.getAndIncrement() < 2) {
                        try {
                            evaluationBarrier.await(5, TimeUnit.SECONDS);
                        } catch (Exception failure) {
                            throw new AssertionError("concurrent evaluation barrier failed", failure);
                        }
                        return Optional.empty();
                    }
                    return Optional.ofNullable(liveState.get());
                case "save", "saveAndFlush":
                    UserCopyAllocationEntity entity = (UserCopyAllocationEntity) args[0];
                    if ("LIVE".equals(entity.getExecutionMode())) {
                        if (liveState.get() == null) {
                            entity.setId(401L);
                            liveState.set(entity);
                            liveInserts.incrementAndGet();
                        }
                        return liveState.get();
                    }
                    microState.set(entity);
                    return entity;
                default:
                    return unexpected(method);
            }
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

    private static TransactionOperations rollbackRestoringTransactionOperations(
            UserCopyAllocationEntity allocation
    ) {
        AtomicReference<AllocationMutationSnapshot> snapshot = new AtomicReference<>();
        PlatformTransactionManager manager = new PlatformTransactionManager() {
            @Override
            public TransactionStatus getTransaction(TransactionDefinition definition) {
                snapshot.set(AllocationMutationSnapshot.capture(allocation));
                return new SimpleTransactionStatus();
            }

            @Override
            public void commit(TransactionStatus status) {
            }

            @Override
            public void rollback(TransactionStatus status) {
                snapshot.get().restore(allocation);
            }
        };
        return new TransactionTemplate(manager);
    }

    private static final class SynchronizedProfileExecutor extends CopyDistributionUnitExecutor {
        private final Map<String, Object> locks = new ConcurrentHashMap<>();

        private SynchronizedProfileExecutor() {
            super(null, new SimpleMeterRegistry(), null, noOpTransactionManager(), 15);
        }

        @Override
        public UnitMutationResult execute(
                UUID userId,
                String walletId,
                String profileKey,
                Long allocationId,
                Supplier<UnitMutationResult> work
        ) {
            synchronized (locks.computeIfAbsent(profileKey, ignored -> new Object())) {
                return work.get();
            }
        }
    }

    private static PlatformTransactionManager noOpTransactionManager() {
        return new PlatformTransactionManager() {
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

    private record AllocationMutationSnapshot(
            UserCopyAllocationEntity.Status status,
            boolean active,
            String executionMode,
            String statusReason,
            OffsetDateTime statusUpdatedAt,
            OffsetDateTime updatedAt,
            OffsetDateTime endsAt
    ) {
        private static AllocationMutationSnapshot capture(UserCopyAllocationEntity allocation) {
            return new AllocationMutationSnapshot(
                    allocation.getStatus(),
                    allocation.isActive(),
                    allocation.getExecutionMode(),
                    allocation.getStatusReason(),
                    allocation.getStatusUpdatedAt(),
                    allocation.getUpdatedAt(),
                    allocation.getEndsAt());
        }

        private void restore(UserCopyAllocationEntity allocation) {
            allocation.setStatus(status);
            allocation.setActive(active);
            allocation.setExecutionMode(executionMode);
            allocation.setStatusReason(statusReason);
            allocation.setStatusUpdatedAt(statusUpdatedAt);
            allocation.setUpdatedAt(updatedAt);
            allocation.setEndsAt(endsAt);
        }
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
