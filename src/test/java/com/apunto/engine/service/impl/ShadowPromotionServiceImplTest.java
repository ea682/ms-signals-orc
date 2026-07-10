package com.apunto.engine.service.impl;

import com.apunto.engine.entity.CopyPromotionAuditEntity;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import com.apunto.engine.entity.ShadowWalletProfileValidationEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.entity.UserWalletCopyPlanEntity;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.repository.CopyPromotionAuditRepository;
import com.apunto.engine.repository.DetailUserRepository;
import com.apunto.engine.repository.ShadowCopyAllocationRepository;
import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowPositionStateRepository;
import com.apunto.engine.repository.ShadowWalletProfileValidationRepository;
import com.apunto.engine.repository.UserApiKeyRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.repository.UserRepository;
import com.apunto.engine.repository.UserWalletCopyPlanRepository;
import com.apunto.engine.service.copy.decision.CopyDecisionGateway;
import com.apunto.engine.service.copy.decision.CopyDecisionRequest;
import com.apunto.engine.service.copy.promotion.ShadowPromotionProperties;
import com.apunto.engine.service.copy.promotion.ShadowPromotionResult;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver;
import com.apunto.engine.service.copy.symbol.CopySymbolResolution;
import com.apunto.engine.service.copy.symbol.CopySymbolResolver;
import org.springframework.dao.DataIntegrityViolationException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowPromotionServiceImplTest {

    private static final Set<String> DB_ALLOWED_COPY_MODES = Set.of(
            "copy_all_metric_movements",
            "copy_only_short_events",
            "copy_only_long_events",
            "copy_open_and_full_close_only",
            "copy_first_open_final_close",
            "copy_strategy_filtered_events",
            "copy_only_flip_events"
    );

    private static final Set<String> DB_ALLOWED_SHADOW_STATUSES = Set.of(
            "SHADOW_ACTIVE",
            "SHADOW_WARNING",
            "SHADOW_VALIDATED",
            "SHADOW_REJECTED",
            "SHADOW_ONLY",
            "PROMOTED_TO_LIVE",
            "SHADOW_PAUSED"
    );

    @Test
    void approvedShadowCreatesPlanMicroLiveAllocationAndAudit() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "SYMBOL_SPECIALIST", "BTCUSDT");
        ShadowWalletProfileValidationEntity validation = validation(5, 12, "8.5");
        AtomicReference<UserWalletCopyPlanEntity> savedPlan = new AtomicReference<>();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation,
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                savedPlan,
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(
                        sourceSymbol,
                        "BTCUSDC",
                        "BTC",
                        "USDC",
                        capitalAsset,
                        false
                )
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.evaluated());
        assertEquals(1, result.created());
        assertEquals("MICRO_LIVE", savedAllocation.get().getExecutionMode());
        assertEquals(UserCopyAllocationEntity.Status.ACTIVE, savedAllocation.get().getStatus());
        assertEquals(shadow.getId(), savedAllocation.get().getLinkedShadowAllocationId());
        assertEquals("PROMOTED_FROM_SHADOW", savedAllocation.get().getStatusReason());
        assertEquals("BTCUSDT", savedAllocation.get().getSourceSymbol());
        assertEquals("BTCUSDC", savedAllocation.get().getTargetSymbol());
        assertEquals("USDC", savedAllocation.get().getCapitalAsset());
        assertNotNull(savedAllocation.get().getPromotedFromShadowAt());
        assertEquals("ACTIVE", savedPlan.get().getStatus());
        assertTrue(savedPlan.get().isActive());
        assertEquals(userId, savedPlan.get().getIdUser());
        assertEquals("0xabc", savedPlan.get().getWalletLc());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_CREATED".equals(a.getDecision())));
        assertEquals("SHADOW_VALIDATED", shadow.getStatus());
        assertTrue(DB_ALLOWED_SHADOW_STATUSES.contains(shadow.getStatus()));
        assertEquals("PROMOTED_TO_MICRO_LIVE_RECORDED_AS_REASON", shadow.getLastValidationReason());
        assertTrue(audits.stream().anyMatch(a -> "PROMOTED_TO_MICRO_LIVE_RECORDED_AS_REASON".equals(a.getReasonDetails().get("shadowPromotionReasonCode"))));
    }

    @Test
    void emptyCopyPlanAndZeroShadowAllocationPctUsesUserDetailCapitalForFirstMicroLivePromotion() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setAllocationPct(BigDecimal.ZERO);
        shadow.setTargetLiveAllocationPct(null);
        AtomicReference<UserWalletCopyPlanEntity> savedPlan = new AtomicReference<>();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                savedPlan,
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertNotNull(savedPlan.get());
        assertNotNull(savedAllocation.get());
        assertEquals("MICRO_LIVE", savedAllocation.get().getExecutionMode());
        assertEquals(new BigDecimal("0.000001"), savedAllocation.get().getAllocationPct());
        assertEquals(new BigDecimal("100.00000000"), savedPlan.get().getAllocatedCapitalUsd());
        assertTrue(audits.stream().anyMatch(a -> "CAPITAL_CONFIG_FOUND_FROM_USER_DETAIL".equals(a.getReasonDetails().get("capitalConfigReasonCode"))));
        assertTrue(audits.stream().anyMatch(a -> "COPY_PLAN_CREATED".equals(a.getReasonDetails().get("copyPlanReasonCode"))));
    }

    @Test
    void movementAllCreatesDbAllowedCopyMode() {
        assertPromotedCopyMode("MOVEMENT_ALL", null, "copy_all_metric_movements");
    }

    @Test
    void shortOnlyCreatesDbAllowedCopyMode() {
        assertPromotedCopyMode("SHORT_ONLY", null, "copy_only_short_events");
    }

    @Test
    void longOnlyCreatesDbAllowedCopyMode() {
        assertPromotedCopyMode("LONG_ONLY", null, "copy_only_long_events");
    }

    @Test
    void legacyMovementAllSourceCopyModeIsMappedToDbAllowedValue() {
        assertPromotedCopyMode("MOVEMENT_ALL", "copy_movement_all_events", "copy_all_metric_movements");
    }

    @Test
    void legacyShortSourceCopyModeIsMappedToDbAllowedValue() {
        assertPromotedCopyMode("SHORT_ONLY", "copy_short_events", "copy_only_short_events");
    }

    @Test
    void legacyLongSourceCopyModeIsMappedToDbAllowedValue() {
        assertPromotedCopyMode("LONG_ONLY", "copy_long_events", "copy_only_long_events");
    }

    @Test
    void skipSourceCopyModeIsNeverPersistedAsAllocationCopyMode() {
        assertPromotedCopyMode("MOVEMENT_ALL", "SKIP", "copy_all_metric_movements");
    }

    @Test
    void knownA445MovementAllCandidateCreatesMicroLiveWithConstraintSafeCopyMode() {
        UUID userId = UUID.fromString("c01b3bc3-3c92-40fd-91d4-39bcae01bbe7");
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setId(18L);
        shadow.setWalletId("0xa445a0a15b1d50fa0c4bfe6796d9447e0da5329d");
        shadow.setCopyMode("copy_movement_all_events");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertNotNull(savedAllocation.get());
        assertEquals("0xa445a0a15b1d50fa0c4bfe6796d9447e0da5329d", savedAllocation.get().getWalletId());
        assertEquals("MOVEMENT_ALL", savedAllocation.get().getCopyStrategyCode());
        assertEquals("copy_all_metric_movements", savedAllocation.get().getCopyMode());
        assertEquals("MICRO_LIVE", savedAllocation.get().getExecutionMode());
        assertEquals(UserCopyAllocationEntity.Status.ACTIVE, savedAllocation.get().getStatus());
        assertTrue(savedAllocation.get().isActive());
        assertEquals(18L, savedAllocation.get().getLinkedShadowAllocationId());
        assertTrue(audits.stream().anyMatch(a -> "COPY_MODE_CONSTRAINT_SAFE".equals(a.getReasonDetails().get("copyModeConstraintReasonCode"))));
    }

    @Test
    void invalidCopyModeMappingRejectsOnlyThatCandidateAndBatchContinues() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity bad = readyShadow(userId, "UNKNOWN_PROFILE", "UNKNOWN_PROFILE");
        bad.setId(11L);
        bad.setCopyMode("SKIP");
        ShadowCopyAllocationEntity good = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        good.setId(12L);
        good.setCopyMode("copy_movement_all_events");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(bad, good),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertEquals(1, result.rejected());
        assertEquals(12L, savedAllocation.get().getLinkedShadowAllocationId());
        assertEquals("copy_all_metric_movements", savedAllocation.get().getCopyMode());
        assertTrue(audits.stream().anyMatch(a -> "INVALID_COPY_MODE_MAPPING".equals(a.getReasonCode())));
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_CREATED".equals(a.getDecision())));
    }

    @Test
    void shadowToLiveDirectIsRejectedWhenPolicyRequiresMicroLive() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setLastValidationReason("LIVE_READY_FROM_SHADOW");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        ShadowPromotionProperties properties = promotionProperties();
        properties.setDefaultTargetMode("LIVE");
        properties.setDirectLivePolicy("REQUIRE_MICRO_LIVE");

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false),
                0L,
                properties
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_REQUIRED_BY_POLICY".equals(a.getReasonCode())));
    }

    @Test
    void shadowToLiveDirectIsRejectedWhenPolicyAllowsButShadowIsNotLiveReady() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setLastValidationReason("SHADOW_VALIDATED_READY_FOR_MICRO");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        ShadowPromotionProperties properties = promotionProperties();
        properties.setDefaultTargetMode("LIVE");
        properties.setDirectLivePolicy("ALLOW_DIRECT_LIVE_FOR_LIVE_READY");

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false),
                0L,
                properties
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "LIVE_NOT_READY_FROM_SHADOW".equals(a.getReasonCode())));
    }

    @Test
    void shadowToLiveDirectCreatesLiveOnlyWhenPolicyAllowsAndShadowIsLiveReady() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setLastValidationReason("LIVE_READY_FROM_SHADOW");
        shadow.setCopyMode("copy_movement_all_events");
        AtomicReference<UserWalletCopyPlanEntity> savedPlan = new AtomicReference<>();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        ShadowPromotionProperties properties = promotionProperties();
        properties.setDefaultTargetMode("LIVE");
        properties.setDirectLivePolicy("ALLOW_DIRECT_LIVE_FOR_LIVE_READY");

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                savedPlan,
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false),
                0L,
                properties
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertNotNull(savedPlan.get());
        assertNotNull(savedAllocation.get());
        assertEquals("LIVE", savedAllocation.get().getExecutionMode());
        assertEquals("copy_all_metric_movements", savedAllocation.get().getCopyMode());
        assertEquals("PROMOTED_DIRECT_FROM_SHADOW", savedAllocation.get().getStatusReason());
        assertEquals("PROMOTED_TO_LIVE", shadow.getStatus());
        assertTrue(audits.stream().anyMatch(a -> "LIVE_ALLOCATION_CREATED".equals(a.getDecision())));
    }

    @Test
    void insufficientShadowEvidenceDoesNotCreateAllocationAndAuditsReason() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        ShadowWalletProfileValidationEntity validation = validation(0, 0, "0");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation,
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "SHADOW_NOT_READY_MIN_EVENTS".equals(a.getReasonCode())));
    }

    @Test
    void readyShadowWithSummaryBlockerCallsFullDecisionAndCanPromoteToMicroLive() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setCopyGuardStatus("SHADOW_ONLY");
        shadow.setCopyGuardAction("SHADOW_ONLY");
        shadow.setLastValidationReason("SUMMARY_OR_MISSING_FACT_PAYLOAD_REQUIRES_SHADOW_OR_FULL_VALIDATION");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        CapturingCopyDecisionGateway gateway = new CapturingCopyDecisionGateway(fullDecisionAllowed(true, false, "FULL_DECISION_OK_FOR_MICRO_LIVE"));

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false),
                0L,
                promotionProperties(),
                gateway
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertNotNull(savedAllocation.get());
        assertEquals(1, gateway.calls.get());
        assertEquals("micro-live-entry", gateway.lastRequest.mode());
        assertEquals("full", gateway.lastRequest.simulation());
        assertEquals(30, gateway.lastRequest.minHistoryDays());
        assertEquals(60, gateway.lastRequest.simulationLookbackDays());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_CREATED".equals(a.getDecision())));
    }

    @Test
    void readyShadowDoesNotPromoteWhenFullDecisionCopyGuardBlocks() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        CapturingCopyDecisionGateway gateway = new CapturingCopyDecisionGateway(fullDecisionBlocked("NEGATIVE_REQUIRED_WINDOW_2W"));

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false),
                0L,
                promotionProperties(),
                gateway
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertEquals(1, gateway.calls.get());
        assertTrue(audits.stream().anyMatch(a -> "FULL_DECISION_BLOCKED_BY_COPY_GUARD".equals(a.getReasonCode())));
    }

    @Test
    void incompleteShadowEvidenceDoesNotCallFullDecision() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        CapturingCopyDecisionGateway gateway = new CapturingCopyDecisionGateway(fullDecisionAllowed(true, false, "FULL_DECISION_OK_FOR_MICRO_LIVE"));

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(0, 0, "0"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false),
                0L,
                promotionProperties(),
                gateway
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertEquals(0, gateway.calls.get());
    }

    @Test
    void doesNotPromoteWithOperationsButWithoutClosedPositions() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(0, 12, "4"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "SHADOW_NOT_READY_MIN_CLOSED_POSITIONS".equals(a.getReasonCode())));
    }

    @Test
    void doesNotPromoteWhenCopyGuardBlocksOpen() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setCopyGuardAction("BLOCKED");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "4"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "SHADOW_NOT_READY_COPY_GUARD".equals(a.getReasonCode())));
    }

    @Test
    void promotesSummaryNotFinalShadowOnlyAfterEnoughRealShadowEvidence() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setStatus("SHADOW_ONLY");
        shadow.setCopyGuardStatus("SHADOW_ONLY");
        shadow.setCopyGuardAction("SHADOW_ONLY");
        shadow.setLastValidationReason("SUMMARY_NOT_FINAL_LIVE_BLOCKED");
        AtomicReference<UserWalletCopyPlanEntity> savedPlan = new AtomicReference<>();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validationWithSimulatedEventsOnly(5, 12, "4"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                savedPlan,
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertNotNull(savedPlan.get());
        assertNotNull(savedAllocation.get());
        assertEquals("MICRO_LIVE", savedAllocation.get().getExecutionMode());
        assertEquals(UserCopyAllocationEntity.Status.ACTIVE, savedAllocation.get().getStatus());
        assertTrue(savedAllocation.get().isActive());
        assertEquals("SHADOW_VALIDATED", shadow.getStatus());
        assertTrue(DB_ALLOWED_SHADOW_STATUSES.contains(shadow.getStatus()));
        assertEquals("PROMOTED_TO_MICRO_LIVE_RECORDED_AS_REASON", shadow.getLastValidationReason());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_CREATED".equals(a.getDecision())));
        assertTrue(audits.stream().anyMatch(a -> "SHADOW_STATUS_CONSTRAINT_SAFE".equals(a.getReasonDetails().get("shadowStatusConstraintReasonCode"))));
    }

    @Test
    void keepsSummaryNotFinalShadowOnlyBlockedWithoutEnoughRealShadowEvidence() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setStatus("SHADOW_ONLY");
        shadow.setCopyGuardStatus("SHADOW_ONLY");
        shadow.setCopyGuardAction("SHADOW_ONLY");
        shadow.setLastValidationReason("SUMMARY_NOT_FINAL_LIVE_BLOCKED");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(0, 0, "0"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "SHADOW_NOT_READY_MIN_EVENTS".equals(a.getReasonCode())));
    }

    @Test
    void doesNotPromoteInactiveUser() {
        UUID userId = UUID.randomUUID();
        UserBundle bundle = activeUser(userId, true, 192, 5, "USDC");
        bundle.user().setActivo(false);
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(readyShadow(userId, "MOVEMENT_ALL", "ALL")),
                validation(5, 12, "4"),
                bundle,
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        service.promoteShadowToMicroLive();

        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "NO_ACTIVE_USER".equals(a.getReasonCode())));
    }

    @Test
    void missingBinanceApiDoesNotCreateAllocationAndAuditsReason() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "4"),
                activeUser(userId, false, 192, 5, "USDC"),
                null,
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "NO_ACTIVE_BINANCE_API_KEY".equals(a.getReasonCode())));
    }

    @Test
    void doesNotPromoteWithoutCapital() {
        UUID userId = UUID.randomUUID();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(readyShadow(userId, "MOVEMENT_ALL", "ALL")),
                validation(5, 12, "4"),
                activeUser(userId, true, 0, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        service.promoteShadowToMicroLive();

        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "NO_CAPITAL_CONFIG".equals(a.getReasonCode())));
    }

    @Test
    void doesNotPromoteWhenUserDetailCapitalIsNull() {
        UUID userId = UUID.randomUUID();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(readyShadow(userId, "MOVEMENT_ALL", "ALL")),
                validation(5, 12, "4"),
                activeUser(userId, true, null, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        service.promoteShadowToMicroLive();

        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "NO_CAPITAL_CONFIG".equals(a.getReasonCode())
                && "CAPITAL_CONFIG_MISSING_FROM_USER_DETAIL".equals(a.getReasonDetails().get("capitalConfigReasonCode"))));
    }

    @Test
    void doesNotPromoteWhenCapitalAssetIsMissingOrInvalid() {
        for (String capitalAsset : new String[]{null, "", "BTC"}) {
            UUID userId = UUID.randomUUID();
            AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
            List<CopyPromotionAuditEntity> audits = new ArrayList<>();

            ShadowPromotionServiceImpl service = service(
                    List.of(readyShadow(userId, "MOVEMENT_ALL", "ALL")),
                    validation(5, 12, "4"),
                    activeUser(userId, true, 192, 5, capitalAsset),
                    apiKey(userId, true),
                    new AtomicReference<>(),
                    savedAllocation,
                    audits,
                    (sourceSymbol, resolvedCapitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, resolvedCapitalAsset, resolvedCapitalAsset, false)
            );

            service.promoteShadowToMicroLive();

            assertNull(savedAllocation.get());
            assertTrue(audits.stream().anyMatch(a -> "NO_CAPITAL_CONFIG".equals(a.getReasonCode())
                    && "CAPITAL_CONFIG_MISSING_FROM_USER_DETAIL".equals(a.getReasonDetails().get("capitalConfigReasonCode"))));
        }
    }

    @Test
    void doesNotPromoteWhenMaxWalletIsMissing() {
        UUID userId = UUID.randomUUID();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(readyShadow(userId, "MOVEMENT_ALL", "ALL")),
                validation(5, 12, "4"),
                activeUser(userId, true, 192, null, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        service.promoteShadowToMicroLive();

        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "NO_CAPITAL_CONFIG".equals(a.getReasonCode())
                && "CAPITAL_CONFIG_MISSING_FROM_USER_DETAIL".equals(a.getReasonDetails().get("capitalConfigReasonCode"))));
    }

    @Test
    void doesNotPromoteWhenMaxWalletReached() {
        UUID userId = UUID.randomUUID();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(readyShadow(userId, "MOVEMENT_ALL", "ALL")),
                validation(5, 12, "4"),
                activeUser(userId, true, 192, 1, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false),
                1L
        );

        service.promoteShadowToMicroLive();

        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "MAX_WALLET_REACHED".equals(a.getReasonCode())));
    }

    @Test
    void existingCopyPlanIsReusedWithoutChangingIdentity() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        UserWalletCopyPlanEntity existingPlan = UserWalletCopyPlanEntity.builder()
                .id(123L)
                .idUser(userId)
                .walletLc("0xabc")
                .allocationPct(new BigDecimal("0.250000"))
                .status("ACTIVE")
                .active(true)
                .createdAt(OffsetDateTime.now().minusDays(1))
                .updatedAt(OffsetDateTime.now().minusDays(1))
                .build();
        AtomicReference<UserWalletCopyPlanEntity> savedPlan = new AtomicReference<>(existingPlan);
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "4"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                savedPlan,
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertEquals(123L, savedPlan.get().getId());
        assertNotNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "COPY_PLAN_ALREADY_EXISTS".equals(a.getReasonDetails().get("copyPlanReasonCode"))));
    }

    @Test
    void duplicateConstraintWhenCreatingCopyPlanIsRetriedAsPlanReuse() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        UserWalletCopyPlanEntity existingPlan = UserWalletCopyPlanEntity.builder()
                .id(123L)
                .idUser(userId)
                .walletLc("0xabc")
                .allocationPct(new BigDecimal("0.250000"))
                .status("ACTIVE")
                .active(true)
                .createdAt(OffsetDateTime.now().minusDays(1))
                .updatedAt(OffsetDateTime.now().minusDays(1))
                .build();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        AtomicBoolean firstFind = new AtomicBoolean(true);

        ShadowPromotionServiceImpl service = new ShadowPromotionServiceImpl(
                shadowRepository(List.of(shadow)),
                validationRepository(validation(5, 12, "4")),
                eventRepository(),
                positionRepository(),
                userRepository(activeUser(userId, true, 192, 5, "USDC").user()),
                detailRepository(activeUser(userId, true, 192, 5, "USDC").detail()),
                apiKeyRepository(apiKey(userId, true)),
                proxy(UserWalletCopyPlanRepository.class, (method, args) -> {
                    if ("findByIdUserAndWalletLc".equals(method.getName())) {
                        return firstFind.getAndSet(false) ? Optional.empty() : Optional.of(existingPlan);
                    }
                    if ("saveAndFlush".equals(method.getName()) || "save".equals(method.getName())) {
                        if (firstFind.get()) {
                            throw new AssertionError("plan should be looked up before save");
                        }
                        throw new DataIntegrityViolationException("duplicate plan");
                    }
                    return unexpected(method);
                }),
                allocationRepository(savedAllocation, 0L),
                auditRepository(audits),
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false),
                promotionProperties(),
                new CapturingCopyDecisionGateway(fullDecisionAllowed(true, false, "FULL_DECISION_OK_FOR_MICRO_LIVE"))
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertNotNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "COPY_PLAN_ALREADY_EXISTS".equals(a.getReasonDetails().get("copyPlanReasonCode"))));
    }

    @Test
    void unresolvedTargetSymbolSkipsOnlyThatShadowAllocation() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "SYMBOL_SPECIALIST", "HYPEUSDT");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "4"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.skipped(
                        sourceSymbol,
                        null,
                        "HYPE",
                        capitalAsset,
                        capitalAsset,
                        "SYMBOL_TARGET_NOT_AVAILABLE"
                )
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "SYMBOL_TARGET_NOT_AVAILABLE".equals(a.getReasonCode())));
    }

    @Test
    void isIdempotentWhenRunTwice() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "SYMBOL_SPECIALIST", "BTCUSDT");
        AtomicReference<UserWalletCopyPlanEntity> savedPlan = new AtomicReference<>();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                savedPlan,
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, "BTCUSDC", "BTC", "USDC", capitalAsset, false)
        );

        ShadowPromotionResult first = service.promoteShadowToMicroLive();
        ShadowPromotionResult second = service.promoteShadowToMicroLive();

        assertEquals(1, first.created());
        assertEquals(0, second.created());
        assertTrue(audits.stream().anyMatch(a -> "ALREADY_PROMOTED".equals(a.getReasonCode())));
    }

    @Test
    void continuesWhenOneCandidateFails() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity bad = readyShadow(userId, "SYMBOL_SPECIALIST", "HYPEUSDT");
        bad.setId(11L);
        ShadowCopyAllocationEntity good = readyShadow(userId, "SYMBOL_SPECIALIST", "BTCUSDT");
        good.setId(12L);
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(bad, good),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> {
                    if ("HYPEUSDT".equals(sourceSymbol)) {
                        return CopySymbolResolution.skipped(sourceSymbol, null, "HYPE", capitalAsset, capitalAsset, "SYMBOL_TARGET_NOT_AVAILABLE");
                    }
                    return CopySymbolResolution.resolved(sourceSymbol, "BTCUSDC", "BTC", "USDC", capitalAsset, false);
                }
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertEquals(1, result.rejected());
        assertEquals(12L, savedAllocation.get().getLinkedShadowAllocationId());
        assertTrue(audits.stream().anyMatch(a -> "SYMBOL_TARGET_NOT_AVAILABLE".equals(a.getReasonCode())));
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_CREATED".equals(a.getDecision())));
    }

    @Test
    void dataRiskCopyGuardStatusBlocksEvenWithCapitalAndShadowEvidence() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        shadow.setCopyGuardStatus("DATA_RISK");
        shadow.setLastValidationReason("DATA_RISK");
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "4"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.rejected());
        assertNull(savedAllocation.get());
        assertTrue(audits.stream().anyMatch(a -> "SHADOW_NOT_READY_COPY_GUARD".equals(a.getReasonCode())));
    }

    @Test
    void existingMicroLiveAllocationIsNoopBeforeCapitalValidation() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "MOVEMENT_ALL", "ALL");
        UserCopyAllocationEntity existing = UserCopyAllocationEntity.builder()
                .id(778L)
                .idUser(userId)
                .walletId("0xabc")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("all")
                .scopeValue("ALL")
                .allocationPct(new BigDecimal("0.500000"))
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .executionMode("MICRO_LIVE")
                .linkedShadowAllocationId(shadow.getId())
                .promotedFromShadowAt(OffsetDateTime.now())
                .build();
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>(existing);
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "4"),
                activeUser(userId, true, null, null, null),
                apiKey(userId, false),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(0, result.created());
        assertEquals(1, result.skipped());
        assertEquals(0, result.rejected());
        assertEquals("SHADOW_VALIDATED", shadow.getStatus());
        assertTrue(DB_ALLOWED_SHADOW_STATUSES.contains(shadow.getStatus()));
        assertEquals(778L, shadow.getLinkedLiveAllocationId());
        assertTrue(audits.stream().noneMatch(a -> "NO_CAPITAL_CONFIG".equals(a.getReasonCode())));
        assertTrue(audits.stream().anyMatch(a -> "SHADOW_PROMOTION_NOOP".equals(a.getDecision())
                && "ALREADY_PROMOTED".equals(a.getReasonCode())));
    }

    @Test
    void treatsDuplicateConstraintAsIdempotentNoopWhenExistingAllocationAppears() {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, "SYMBOL_SPECIALIST", "BTCUSDT");
        UserCopyAllocationEntity existing = UserCopyAllocationEntity.builder()
                .id(777L)
                .idUser(userId)
                .walletId("0xabc")
                .copyStrategyCode("SYMBOL_SPECIALIST")
                .scopeType("symbol")
                .scopeValue("BTCUSDT")
                .allocationPct(new BigDecimal("0.500000"))
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .isActive(true)
                .executionMode("MICRO_LIVE")
                .linkedShadowAllocationId(shadow.getId())
                .promotedFromShadowAt(OffsetDateTime.now())
                .build();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();
        AtomicBoolean firstFind = new AtomicBoolean(true);

        ShadowPromotionServiceImpl service = new ShadowPromotionServiceImpl(
                shadowRepository(List.of(shadow)),
                validationRepository(validation(5, 12, "8.5")),
                eventRepository(),
                positionRepository(),
                userRepository(activeUser(userId, true, 192, 5, "USDC").user()),
                detailRepository(activeUser(userId, true, 192, 5, "USDC").detail()),
                apiKeyRepository(apiKey(userId, true)),
                planRepository(new AtomicReference<>()),
                proxy(UserCopyAllocationRepository.class, (method, args) -> {
                    if ("countActiveExecutionAllocationsByUser".equals(method.getName())) return 0L;
                    if ("findOpenAllocationForUserWalletStrategyScope".equals(method.getName())) {
                        return firstFind.getAndSet(false) ? Optional.empty() : Optional.of(existing);
                    }
                    if ("saveAndFlush".equals(method.getName())) {
                        throw new DataIntegrityViolationException("duplicate");
                    }
                    if ("save".equals(method.getName())) return args[0];
                    return unexpected(method);
                }),
                auditRepository(audits),
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, "BTCUSDC", "BTC", "USDC", capitalAsset, false),
                promotionProperties(),
                new CapturingCopyDecisionGateway(fullDecisionAllowed(true, false, "FULL_DECISION_OK_FOR_MICRO_LIVE"))
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(0, result.created());
        assertEquals(1, result.skipped());
        assertEquals("SHADOW_VALIDATED", shadow.getStatus());
        assertTrue(DB_ALLOWED_SHADOW_STATUSES.contains(shadow.getStatus()));
        assertEquals(777L, shadow.getLinkedLiveAllocationId());
        assertTrue(audits.stream().anyMatch(a -> "SHADOW_PROMOTION_NOOP".equals(a.getDecision())
                && "ALREADY_PROMOTED".equals(a.getReasonCode())));
    }

    private static ShadowPromotionServiceImpl service(
            List<ShadowCopyAllocationEntity> shadows,
            ShadowWalletProfileValidationEntity validation,
            UserBundle user,
            UserApiKeyEntity apiKey,
            AtomicReference<UserWalletCopyPlanEntity> savedPlan,
            AtomicReference<UserCopyAllocationEntity> savedAllocation,
            List<CopyPromotionAuditEntity> audits,
            CopySymbolResolver symbolResolver
    ) {
        return service(shadows, validation, user, apiKey, savedPlan, savedAllocation, audits, symbolResolver, 0L);
    }

    private static ShadowPromotionServiceImpl service(
            List<ShadowCopyAllocationEntity> shadows,
            ShadowWalletProfileValidationEntity validation,
            UserBundle user,
            UserApiKeyEntity apiKey,
            AtomicReference<UserWalletCopyPlanEntity> savedPlan,
            AtomicReference<UserCopyAllocationEntity> savedAllocation,
            List<CopyPromotionAuditEntity> audits,
            CopySymbolResolver symbolResolver,
            long activeAllocationCount
    ) {
        return service(shadows, validation, user, apiKey, savedPlan, savedAllocation, audits, symbolResolver, activeAllocationCount, promotionProperties());
    }

    private static ShadowPromotionServiceImpl service(
            List<ShadowCopyAllocationEntity> shadows,
            ShadowWalletProfileValidationEntity validation,
            UserBundle user,
            UserApiKeyEntity apiKey,
            AtomicReference<UserWalletCopyPlanEntity> savedPlan,
            AtomicReference<UserCopyAllocationEntity> savedAllocation,
            List<CopyPromotionAuditEntity> audits,
            CopySymbolResolver symbolResolver,
            long activeAllocationCount,
            ShadowPromotionProperties properties
    ) {
        return service(shadows, validation, user, apiKey, savedPlan, savedAllocation, audits, symbolResolver, activeAllocationCount, properties, new CapturingCopyDecisionGateway(fullDecisionAllowed(true, false, "FULL_DECISION_OK_FOR_MICRO_LIVE")));
    }

    private static ShadowPromotionServiceImpl service(
            List<ShadowCopyAllocationEntity> shadows,
            ShadowWalletProfileValidationEntity validation,
            UserBundle user,
            UserApiKeyEntity apiKey,
            AtomicReference<UserWalletCopyPlanEntity> savedPlan,
            AtomicReference<UserCopyAllocationEntity> savedAllocation,
            List<CopyPromotionAuditEntity> audits,
            CopySymbolResolver symbolResolver,
            long activeAllocationCount,
            ShadowPromotionProperties properties,
            CopyDecisionGateway copyDecisionGateway
    ) {
        return new ShadowPromotionServiceImpl(
                shadowRepository(shadows),
                validationRepository(validation),
                eventRepository(),
                positionRepository(),
                userRepository(user == null ? null : user.user()),
                detailRepository(user == null ? null : user.detail()),
                apiKeyRepository(apiKey),
                planRepository(savedPlan),
                allocationRepository(savedAllocation, activeAllocationCount),
                auditRepository(audits),
                symbolResolver,
                properties,
                copyDecisionGateway
        );
    }

    private static ShadowPromotionProperties promotionProperties() {
        ShadowPromotionProperties properties = new ShadowPromotionProperties();
        properties.setEnabled(true);
        properties.setFromShadowEnabled(true);
        properties.setMinShadowDays(0);
        properties.setMinShadowEvents(1);
        properties.setMinShadowClosedPositions(1);
        properties.setMinShadowCoveragePct(new BigDecimal("90"));
        properties.setRequireActiveWallet(true);
        properties.setMaxInactiveDays(3);
        properties.setRequirePositiveShadowPnl(false);
        properties.setMicroLiveInitialCapitalUsd(new BigDecimal("100"));
        properties.setMicroLiveMaxCapitalUsd(new BigDecimal("100"));
        properties.setRequireFullDecisionForMicroLive(true);
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

    private static void assertPromotedCopyMode(String strategyCode, String sourceCopyMode, String expectedCopyMode) {
        UUID userId = UUID.randomUUID();
        ShadowCopyAllocationEntity shadow = readyShadow(userId, strategyCode, scopeForStrategy(strategyCode));
        shadow.setCopyMode(sourceCopyMode);
        AtomicReference<UserCopyAllocationEntity> savedAllocation = new AtomicReference<>();
        List<CopyPromotionAuditEntity> audits = new ArrayList<>();

        ShadowPromotionServiceImpl service = service(
                List.of(shadow),
                validation(5, 12, "8.5"),
                activeUser(userId, true, 192, 5, "USDC"),
                apiKey(userId, true),
                new AtomicReference<>(),
                savedAllocation,
                audits,
                (sourceSymbol, capitalAsset) -> CopySymbolResolution.resolved(sourceSymbol, sourceSymbol, null, capitalAsset, capitalAsset, false)
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(1, result.created());
        assertNotNull(savedAllocation.get());
        assertEquals(expectedCopyMode, savedAllocation.get().getCopyMode());
        assertTrue(DB_ALLOWED_COPY_MODES.contains(savedAllocation.get().getCopyMode()));
        assertTrue(audits.stream().anyMatch(a -> "COPY_MODE_CONSTRAINT_SAFE".equals(a.getReasonDetails().get("copyModeConstraintReasonCode"))));
    }

    private static String scopeForStrategy(String strategyCode) {
        if ("SHORT_ONLY".equals(strategyCode)) return "SHORT";
        if ("LONG_ONLY".equals(strategyCode)) return "LONG";
        return "ALL";
    }

    private static ShadowCopyAllocationEntity readyShadow(UUID userId, String strategyCode, String scopeValue) {
        OffsetDateTime now = OffsetDateTime.now();
        return ShadowCopyAllocationEntity.builder()
                .id(10L)
                .idUser(userId)
                .walletId("0xabc")
                .copyStrategyCode(strategyCode)
                .scopeType("SYMBOL_SPECIALIST".equals(strategyCode) ? "SYMBOL" : "ALL")
                .scopeValue(scopeValue)
                .strategyKey("0xabc|" + strategyCode + "|" + ("SYMBOL_SPECIALIST".equals(strategyCode) ? "SYMBOL" : "ALL") + "|" + scopeValue)
                .walletProfileId(100L)
                .shadowValidationId(200L)
                .status("SHADOW_ACTIVE")
                .allocationPct(new BigDecimal("0.500000"))
                .targetLiveAllocationPct(new BigDecimal("0.500000"))
                .copyGuardStatus("OK")
                .copyGuardAction("ALLOW")
                .strategyLastActivityAt(now.minusHours(1))
                .createdAt(now.minusDays(2))
                .lastSeenAt(now.minusMinutes(10))
                .active(true)
                .build();
    }

    private static ShadowWalletProfileValidationEntity validation(long closed, long events, String pnl) {
        return ShadowWalletProfileValidationEntity.builder()
                .id(200L)
                .walletProfileId(100L)
                .status("VALIDATED")
                .closedPositions(closed)
                .recordedEvents(events)
                .simulatedEvents(events)
                .skippedEvents(0L)
                .errorEvents(0L)
                .netPnlUsd(new BigDecimal(pnl))
                .maxDrawdown(BigDecimal.ZERO)
                .startedAt(OffsetDateTime.now().minusDays(2))
                .build();
    }

    private static ShadowWalletProfileValidationEntity validationWithSimulatedEventsOnly(long closed, long events, String pnl) {
        ShadowWalletProfileValidationEntity validation = validation(closed, events, pnl);
        validation.setRecordedEvents(0L);
        validation.setSimulatedEvents(events);
        return validation;
    }

    private static UserBundle activeUser(UUID userId, boolean binanceKeyActive, Integer capital, Integer maxWallet, String capitalAsset) {
        UserEntity user = new UserEntity();
        user.setId(userId);
        user.setActivo(true);
        user.setEmail("test@example.com");
        DetailUserEntity detail = new DetailUserEntity();
        detail.setUser(user);
        detail.setUserActive(true);
        detail.setApiKeyBinar(binanceKeyActive);
        detail.setCapital(capital);
        detail.setMaxWallet(maxWallet);
        detail.setCapitalAsset(capitalAsset);
        return new UserBundle(user, detail);
    }

    private static UserApiKeyEntity apiKey(UUID userId, boolean usable) {
        UserEntity user = new UserEntity();
        user.setId(userId);
        UserApiKeyEntity apiKey = new UserApiKeyEntity();
        apiKey.setUser(user);
        apiKey.setApiKey(usable ? "key" : "");
        apiKey.setApiSecret(usable ? "secret" : "");
        return apiKey;
    }

    private static ShadowCopyAllocationRepository shadowRepository(List<ShadowCopyAllocationEntity> shadows) {
        return proxy(ShadowCopyAllocationRepository.class, (method, args) -> {
            if ("findPromotionCandidates".equals(method.getName())) return shadows;
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) return args[0];
            return unexpected(method);
        });
    }

    private static ShadowWalletProfileValidationRepository validationRepository(ShadowWalletProfileValidationEntity validation) {
        return proxy(ShadowWalletProfileValidationRepository.class, (method, args) -> {
            if ("findFirstByWalletProfileIdOrderByStartedAtDesc".equals(method.getName())) return Optional.ofNullable(validation);
            return unexpected(method);
        });
    }

    private static ShadowCopyOperationEventRepository eventRepository() {
        return proxy(ShadowCopyOperationEventRepository.class, (method, args) -> {
            if ("countByShadowAllocationId".equals(method.getName())) return 0L;
            return unexpected(method);
        });
    }

    private static ShadowPositionStateRepository positionRepository() {
        return proxy(ShadowPositionStateRepository.class, (method, args) -> {
            if ("countClosedPositions".equals(method.getName()) || "countOpenPositions".equals(method.getName())) return 0L;
            if ("sumClosedRealizedPnlUsd".equals(method.getName())) return BigDecimal.ZERO;
            return unexpected(method);
        });
    }

    private static UserRepository userRepository(UserEntity user) {
        return proxy(UserRepository.class, (method, args) -> {
            if ("findById".equals(method.getName())) return Optional.ofNullable(user);
            return unexpected(method);
        });
    }

    private static DetailUserRepository detailRepository(DetailUserEntity detail) {
        return proxy(DetailUserRepository.class, (method, args) -> {
            if ("findByUser_Id".equals(method.getName())) return detail;
            return unexpected(method);
        });
    }

    private static UserApiKeyRepository apiKeyRepository(UserApiKeyEntity apiKey) {
        return proxy(UserApiKeyRepository.class, (method, args) -> {
            if ("findByUser_Id".equals(method.getName())) return apiKey;
            return unexpected(method);
        });
    }

    private static UserWalletCopyPlanRepository planRepository(AtomicReference<UserWalletCopyPlanEntity> savedPlan) {
        return proxy(UserWalletCopyPlanRepository.class, (method, args) -> {
            if ("findByIdUserAndWalletLc".equals(method.getName())) return Optional.ofNullable(savedPlan.get());
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) {
                UserWalletCopyPlanEntity entity = (UserWalletCopyPlanEntity) args[0];
                if (entity.getId() == null) {
                    entity.setId(300L);
                }
                savedPlan.set(entity);
                return entity;
            }
            return unexpected(method);
        });
    }

    private static UserCopyAllocationRepository allocationRepository(
            AtomicReference<UserCopyAllocationEntity> savedAllocation,
            long activeAllocationCount
    ) {
        return proxy(UserCopyAllocationRepository.class, (method, args) -> {
            if ("findOpenAllocationForUserWalletStrategyScope".equals(method.getName())) return Optional.ofNullable(savedAllocation.get());
            if ("countActiveExecutionAllocationsByUser".equals(method.getName())) return activeAllocationCount;
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) {
                UserCopyAllocationEntity entity = (UserCopyAllocationEntity) args[0];
                entity.setId(400L);
                assertTrue(UserCopyAllocationCopyModeResolver.isAllowedCopyMode(entity.getCopyMode()),
                        "copy_mode must satisfy chk_user_copy_allocation_copy_mode, actual=" + entity.getCopyMode());
                savedAllocation.set(entity);
                return entity;
            }
            return unexpected(method);
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

    private record UserBundle(UserEntity user, DetailUserEntity detail) {
    }
}
