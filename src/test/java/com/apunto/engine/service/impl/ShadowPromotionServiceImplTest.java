package com.apunto.engine.service.impl;

import com.apunto.engine.entity.CopyPromotionAuditEntity;
import com.apunto.engine.entity.DetailUserEntity;
import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import com.apunto.engine.entity.ShadowWalletProfileValidationEntity;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.entity.UserEntity;
import com.apunto.engine.entity.UserWalletCopyPlanEntity;
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
import com.apunto.engine.service.copy.promotion.ShadowPromotionProperties;
import com.apunto.engine.service.copy.promotion.ShadowPromotionResult;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowPromotionServiceImplTest {

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
        assertEquals("PROMOTED_TO_MICRO_LIVE", shadow.getStatus());
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
        assertEquals("PROMOTED_TO_MICRO_LIVE", shadow.getStatus());
        assertEquals("SHADOW_VALIDATED_READY_FOR_MICRO", shadow.getLastValidationReason());
        assertTrue(audits.stream().anyMatch(a -> "MICRO_LIVE_CREATED".equals(a.getDecision())));
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
                promotionProperties()
        );

        ShadowPromotionResult result = service.promoteShadowToMicroLive();

        assertEquals(0, result.created());
        assertEquals(1, result.skipped());
        assertEquals("PROMOTED_TO_MICRO_LIVE", shadow.getStatus());
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
                promotionProperties()
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
        return properties;
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

    private static UserBundle activeUser(UUID userId, boolean binanceKeyActive, int capital, int maxWallet, String capitalAsset) {
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
            if ("findByIdUserAndWalletLc".equals(method.getName())) return Optional.empty();
            if ("save".equals(method.getName()) || "saveAndFlush".equals(method.getName())) {
                UserWalletCopyPlanEntity entity = (UserWalletCopyPlanEntity) args[0];
                entity.setId(300L);
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
