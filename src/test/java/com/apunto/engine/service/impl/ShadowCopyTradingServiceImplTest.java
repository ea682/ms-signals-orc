package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.CopyWalletProfileEntity;
import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import com.apunto.engine.entity.ShadowCopyOperationEntity;
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
import java.util.HashMap;
import java.util.List;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
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
    void resizeWithoutOpenRecordsSkippedEventWithoutShadowOperation() throws Exception {
        ShadowCopyAllocationEntity allocation = shadowAllocation(10L, UUID.randomUUID(), "MOVEMENT_ALL", "MOVEMENT_ALL", 100L);
        allocation.setCreatedAt(OffsetDateTime.parse("2026-06-22T08:00:00Z"));
        allocation.setLastSeenAt(OffsetDateTime.parse("2026-06-22T08:00:00Z"));
        List<ShadowCopyOperationEventEntity> recordedEvents = new ArrayList<>();
        List<ShadowPositionStateEntity> openedPositions = new ArrayList<>();
        List<ShadowWalletProfileValidationEntity> validations = new ArrayList<>();

        ShadowCopyTradingServiceImpl service = serviceForRuntime(List.of(allocation), recordedEvents, openedPositions, validations);

        OperacionEvent event = new OperacionEvent(
                OperacionEvent.Tipo.ABIERTA,
                OperacionDto.builder()
                        .idOperacion(UUID.randomUUID())
                        .idCuenta("0xabc")
                        .parSymbol("BTCUSDT")
                        .tipoOperacion(PositionSide.LONG)
                        .sizeQty(new BigDecimal("0.2"))
                        .notionalUsd(new BigDecimal("2000"))
                        .precioMercado(new BigDecimal("100500"))
                        .fechaCreacion(Instant.parse("2026-06-22T10:00:00Z"))
                        .build()
        );
        event.setDeltaType("RESIZE");

        assertEquals(1, service.recordShadowEvent(event));
        assertEquals(1, recordedEvents.size());
        assertEquals("RESIZE_WITHOUT_SHADOW_OPEN", recordedEvents.get(0).getReasonCode());
        assertEquals("SKIPPED", recordedEvents.get(0).getDecision());
        assertNull(recordedEvents.get(0).getShadowOperationId());
        assertNull(recordedEvents.get(0).getShadowPositionId());
        assertTrue(openedPositions.isEmpty());
        assertEquals(1L, validations.get(validations.size() - 1).getSkippedEvents());
    }

    @Test
    void closeFindsOpenShadowOperationByProfilePositionWhenOriginIdDiffers() throws Exception {
        ShadowCopyAllocationEntity allocation = shadowAllocation(10L, UUID.randomUUID(), "MOVEMENT_ALL", "MOVEMENT_ALL", 100L);
        UUID shadowPositionId = UUID.randomUUID();
        UUID shadowOperationId = UUID.randomUUID();
        ShadowPositionStateEntity openState = ShadowPositionStateEntity.builder()
                .id(shadowPositionId)
                .shadowAllocationId(allocation.getId())
                .walletProfileId(allocation.getWalletProfileId())
                .idUser(allocation.getIdUser().toString())
                .walletId(allocation.getWalletId())
                .copyStrategyCode(allocation.getCopyStrategyCode())
                .scopeType(allocation.getScopeType())
                .scopeValue(allocation.getScopeValue())
                .strategyKey(allocation.getStrategyKey())
                .parsymbol("BTCUSDT")
                .positionSide("LONG")
                .qty(BigDecimal.ONE)
                .entryPrice(new BigDecimal("100"))
                .status("OPEN")
                .openedAt(OffsetDateTime.parse("2026-06-22T09:00:00Z"))
                .build();
        ShadowCopyOperationEntity openOperation = ShadowCopyOperationEntity.builder()
                .idOperation(shadowOperationId)
                .shadowAllocationId(allocation.getId())
                .walletProfileId(allocation.getWalletProfileId())
                .idUser(allocation.getIdUser().toString())
                .idOrderOrigin(UUID.randomUUID().toString())
                .idWalletOrigin(allocation.getWalletId())
                .copyStrategyCode(allocation.getCopyStrategyCode())
                .scopeType(allocation.getScopeType())
                .scopeValue(allocation.getScopeValue())
                .strategyKey(allocation.getStrategyKey())
                .parsymbol("BTCUSDT")
                .typeOperation("LONG")
                .dateCreation(OffsetDateTime.parse("2026-06-22T09:00:00Z"))
                .active(true)
                .status("OPEN")
                .simulatedFeeUsd(BigDecimal.ZERO)
                .simulatedSlippageUsd(BigDecimal.ZERO)
                .build();
        List<ShadowCopyOperationEventEntity> recordedEvents = new ArrayList<>();

        ShadowCopyTradingServiceImpl service = new ShadowCopyTradingServiceImpl(
                proxy(ShadowCopyAllocationRepository.class, (method, args) -> {
                    if ("findRuntimeProfileRepresentativesByWallet".equals(method.getName())) {
                        return List.of(allocation);
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(ShadowCopyOperationRepository.class, (method, args) -> {
                    if ("findFirstByWalletProfileIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc".equals(method.getName())) {
                        return Optional.of(openOperation);
                    }
                    if ("findFirstByWalletProfileIdAndIdOrderOriginAndTypeOperationAndActiveTrue".equals(method.getName())
                            || "findFirstByShadowAllocationIdAndIdOrderOriginAndTypeOperationAndActiveTrue".equals(method.getName())
                            || "findFirstByShadowAllocationIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(ShadowCopyOperationEventRepository.class, (method, args) -> {
                    if ("lockShadowEventIdempotency".equals(method.getName())) {
                        return null;
                    }
                    if ("existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime".equals(method.getName())) {
                        return false;
                    }
                    if ("existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime".equals(method.getName())) {
                        return false;
                    }
                    if ("countByWalletProfileIdAndDecision".equals(method.getName())) {
                        Long walletProfileId = (Long) args[0];
                        String decision = (String) args[1];
                        return recordedEvents.stream()
                                .filter(e -> walletProfileId.equals(e.getWalletProfileId()))
                                .filter(e -> decision.equals(e.getDecision()))
                                .count();
                    }
                    if ("save".equals(method.getName())) {
                        recordedEvents.add((ShadowCopyOperationEventEntity) args[0]);
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(ShadowPositionStateRepository.class, (method, args) -> {
                    if ("findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatus".equals(method.getName())) {
                        return Optional.of(openState);
                    }
                    if ("findAllByWalletProfileIdAndParsymbolAndStatus".equals(method.getName())
                            || "findAllByShadowAllocationIdAndParsymbolAndStatus".equals(method.getName())) {
                        return List.of();
                    }
                    if ("sumClosedRealizedPnlUsdByWalletProfileId".equals(method.getName())
                            || "sumSlippageUsdByWalletProfileId".equals(method.getName())) {
                        return BigDecimal.ZERO;
                    }
                    if ("countClosedPositionsByWalletProfileId".equals(method.getName())
                            || "countOpenPositionsByWalletProfileId".equals(method.getName())) {
                        return 0L;
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(CopyWalletProfileRepository.class, (method, args) -> {
                    if ("findById".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                shadowProfileValidationRepository(),
                new CopyStrategyRuntimeRouter()
        );
        setField(service, "separateShadowEnabled", true);
        setField(service, "shadowVersion", 1);
        setField(service, "shadowSlippageBps", 0.0d);
        setField(service, "shadowWarmupMinutes", 60L);

        OperacionEvent event = new OperacionEvent(
                OperacionEvent.Tipo.CERRADA,
                OperacionDto.builder()
                        .idOperacion(UUID.randomUUID())
                        .idCuenta("0xabc")
                        .parSymbol("BTCUSDT")
                        .tipoOperacion(PositionSide.LONG)
                        .sizeQty(BigDecimal.ONE)
                        .notionalUsd(new BigDecimal("110"))
                        .precioCierre(new BigDecimal("110"))
                        .fechaCierre(Instant.parse("2026-06-22T10:00:00Z"))
                        .build()
        );
        event.setDeltaType("CLOSE");

        assertEquals(1, service.recordShadowEvent(event));
        assertEquals("CLOSED", openOperation.getStatus());
        assertFalse(openOperation.isActive());
        assertEquals(new BigDecimal("10.000000000000"), openOperation.getRealizedPnlUsd());
        assertEquals("SHADOW_POSITION_CLOSED", recordedEvents.get(0).getReasonCode());
        assertEquals(shadowOperationId, recordedEvents.get(0).getShadowOperationId());
        assertEquals(shadowPositionId, recordedEvents.get(0).getShadowPositionId());
    }

    @Test
    void flipShortToLongClosesPreviousShadowOperationAndRecordsCloseEvent() throws Exception {
        assertFlipClosesPreviousSide(PositionSide.SHORT, PositionSide.LONG);
    }

    @Test
    void flipLongToShortClosesPreviousShadowOperationAndRecordsCloseEvent() throws Exception {
        assertFlipClosesPreviousSide(PositionSide.LONG, PositionSide.SHORT);
    }

    @Test
    void duplicateFlipDoesNotCloseOrOpenTwice() throws Exception {
        UUID originId = UUID.randomUUID();
        FlipRuntime runtime = flipRuntime(PositionSide.SHORT);
        OperacionEvent event = flipEvent(originId, PositionSide.LONG, new BigDecimal("90"));

        assertEquals(1, runtime.service().recordShadowEvent(event));
        int eventCount = runtime.recordedEvents().size();
        int stateCount = runtime.states().size();
        int operationCount = runtime.operations().size();

        assertEquals(0, runtime.service().recordShadowEvent(event));
        assertEquals(eventCount, runtime.recordedEvents().size());
        assertEquals(stateCount, runtime.states().size());
        assertEquals(operationCount, runtime.operations().size());
        assertEquals(1, runtime.recordedEvents().stream()
                .filter(e -> "SHADOW_POSITION_CLOSED_BY_FLIP".equals(e.getReasonCode()))
                .count());
        assertEquals(1, runtime.recordedEvents().stream()
                .filter(e -> "SHADOW_POSITION_OPENED_BY_FLIP".equals(e.getReasonCode()))
                .count());
    }

    @Test
    void flipWithoutPreviousShadowOpenRecordsSkippedWithoutOpeningNewSide() throws Exception {
        ShadowCopyAllocationEntity allocation = shadowAllocation(10L, UUID.randomUUID(), "MOVEMENT_ALL", "MOVEMENT_ALL", 100L);
        allocation.setCreatedAt(OffsetDateTime.parse("2026-06-22T08:00:00Z"));
        allocation.setLastSeenAt(OffsetDateTime.parse("2026-06-22T08:00:00Z"));
        List<ShadowCopyOperationEventEntity> recordedEvents = new ArrayList<>();
        List<ShadowPositionStateEntity> openedPositions = new ArrayList<>();
        List<ShadowWalletProfileValidationEntity> validations = new ArrayList<>();

        ShadowCopyTradingServiceImpl service = serviceForRuntime(List.of(allocation), recordedEvents, openedPositions, validations);

        assertEquals(1, service.recordShadowEvent(flipEvent(UUID.randomUUID(), PositionSide.LONG, new BigDecimal("90"))));
        assertEquals(1, recordedEvents.size());
        assertEquals("FLIP_WITHOUT_SHADOW_OPEN", recordedEvents.get(0).getReasonCode());
        assertEquals("SKIPPED", recordedEvents.get(0).getDecision());
        assertNull(recordedEvents.get(0).getShadowOperationId());
        assertNull(recordedEvents.get(0).getShadowPositionId());
        assertTrue(openedPositions.isEmpty());
        assertEquals(1L, validations.get(validations.size() - 1).getSkippedEvents());
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

    @Test
    void summaryNotFinalBlocksLivePromotionButAllowsShadowSync() throws Exception {
        UUID user = UUID.randomUUID();
        ShadowCopyTradingServiceImpl service = service(new HashMap<>(), new HashMap<>(), new HashMap<>());
        setField(service, "requireShadowValidationBeforeLive", false);

        MetricaWalletDto metric = metric("0xabc", "MOVEMENT_ALL");
        metric.setDecisionFinal(false);
        metric.setSummaryConsistency(Map.of(
                "businessDecisionEquivalentToFull", false,
                "reason", "FULL_DECISION_NOT_MATERIALIZED"
        ));

        assertFalse(service.isLivePromotable(user, metric));

        List<ShadowCopyAllocationEntity> saved = new ArrayList<>();
        ShadowCopyTradingServiceImpl syncService = serviceForShadowSync(saved);
        syncService.syncShadowAllocations(user, List.of(metric), 1, OffsetDateTime.parse("2026-06-22T11:00:00Z"));

        assertEquals(1, saved.size());
        assertEquals("SUMMARY_NOT_FINAL_LIVE_BLOCKED", saved.get(0).getLastValidationReason());
        assertNull(saved.get(0).getTargetLiveAllocationPct());
    }

    @Test
    void simulationAuditFailureBlocksLivePromotion() throws Exception {
        UUID user = UUID.randomUUID();
        MetricaWalletDto metric = metric("0xabc", "MOVEMENT_ALL");
        metric.getCopySimulation().setSimulationAudit(Map.of(
                "valid", false,
                "errors", List.of("WINDOW_SUM_RECONCILIATION_FAILED")
        ));
        ShadowCopyTradingServiceImpl service = service(
                Map.of(user, shadowAllocation(10L, user)),
                Map.of(10L, 5L),
                Map.of(10L, BigDecimal.ONE)
        );

        assertFalse(service.isLivePromotable(user, metric));
    }

    @Test
    void incompleteRequiredWindowBlocksLivePromotion() throws Exception {
        UUID user = UUID.randomUUID();
        MetricaWalletDto metric = metric("0xabc", "MOVEMENT_ALL");
        metric.getCopySimulation().setWindowMeta(Map.<String, Object>of(
                "2w", Map.of("complete", true),
                "1mo", Map.of("complete", false)
        ));
        ShadowCopyTradingServiceImpl service = service(
                Map.of(user, shadowAllocation(10L, user)),
                Map.of(10L, 5L),
                Map.of(10L, BigDecimal.ONE)
        );

        assertFalse(service.isLivePromotable(user, metric));
    }

    @Test
    void syncShadowActivityUsesTopLevelStrategyActivityWhenPresent() throws Exception {
        UUID user = UUID.randomUUID();
        OffsetDateTime walletOpened = OffsetDateTime.parse("2026-06-22T10:00:00Z");
        OffsetDateTime strategyOpened = OffsetDateTime.parse("2026-06-22T10:30:00Z");
        List<ShadowCopyAllocationEntity> saved = new ArrayList<>();
        ShadowCopyTradingServiceImpl service = serviceForShadowSync(saved);

        MetricaWalletDto metric = metric("0xabc", "SHORT_ONLY");
        metric.setWalletActivity(MetricaWalletDto.ActivityDto.builder()
                .lastOpenedAt(walletOpened)
                .build());
        metric.setStrategyActivity(MetricaWalletDto.ActivityDto.builder()
                .lastOpenedAt(strategyOpened)
                .build());

        service.syncShadowAllocations(user, List.of(metric), 1, OffsetDateTime.parse("2026-06-22T11:00:00Z"));

        assertEquals(1, saved.size());
        assertEquals(walletOpened, saved.get(0).getWalletLastOpenedAt());
        assertEquals(strategyOpened, saved.get(0).getStrategyLastOpenedAt());
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

    private static void assertFlipClosesPreviousSide(PositionSide previousSide, PositionSide newSide) throws Exception {
        UUID originId = UUID.randomUUID();
        BigDecimal flipPrice = previousSide == PositionSide.SHORT ? new BigDecimal("90") : new BigDecimal("110");
        FlipRuntime runtime = flipRuntime(previousSide);

        assertEquals(1, runtime.service().recordShadowEvent(flipEvent(originId, newSide, flipPrice)));

        ShadowCopyOperationEventEntity closeEvent = runtime.recordedEvents().stream()
                .filter(e -> "SHADOW_POSITION_CLOSED_BY_FLIP".equals(e.getReasonCode()))
                .findFirst()
                .orElseThrow();
        ShadowCopyOperationEventEntity openEvent = runtime.recordedEvents().stream()
                .filter(e -> "SHADOW_POSITION_OPENED_BY_FLIP".equals(e.getReasonCode()))
                .findFirst()
                .orElseThrow();

        assertEquals("CLOSED", runtime.previousState().getStatus());
        assertEquals(BigDecimal.ZERO, runtime.previousState().getQty());
        assertEquals("CLOSED", runtime.previousOperation().getStatus());
        assertFalse(runtime.previousOperation().isActive());
        assertEquals(runtime.previousState().getId(), closeEvent.getShadowPositionId());
        assertEquals(runtime.previousOperation().getIdOperation(), closeEvent.getShadowOperationId());
        assertEquals("SHADOW_POSITION_CLOSED", closeEvent.getEventType());
        assertEquals(previousSide.name(), closeEvent.getPositionSide());
        assertEquals("SIMULATED", closeEvent.getDecision());
        assertEquals(new BigDecimal("10.000000000000"), closeEvent.getRealizedPnlUsd());
        assertEquals("FLIP", openEvent.getEventType());
        assertEquals(newSide.name(), openEvent.getPositionSide());
        assertTrue(openEvent.getShadowPositionId() != null);
        assertTrue(runtime.states().stream().anyMatch(s ->
                newSide.name().equals(s.getPositionSide()) && "OPEN".equals(s.getStatus())));
    }

    private static OperacionEvent flipEvent(UUID originId, PositionSide newSide, BigDecimal price) {
        OperacionEvent event = new OperacionEvent(
                OperacionEvent.Tipo.ABIERTA,
                OperacionDto.builder()
                        .idOperacion(originId)
                        .idCuenta("0xabc")
                        .parSymbol("BTCUSDT")
                        .tipoOperacion(newSide)
                        .sizeQty(BigDecimal.ONE)
                        .notionalUsd(new BigDecimal("100"))
                        .precioEntrada(price)
                        .precioMercado(price)
                        .fechaCreacion(Instant.parse("2026-06-22T10:00:00Z"))
                        .build()
        );
        event.setDeltaType("FLIP");
        return event;
    }

    private static FlipRuntime flipRuntime(PositionSide previousSide) throws Exception {
        ShadowCopyAllocationEntity allocation = shadowAllocation(10L, UUID.randomUUID(), "MOVEMENT_ALL", "MOVEMENT_ALL", 100L);
        allocation.setCreatedAt(OffsetDateTime.parse("2026-06-22T08:00:00Z"));
        allocation.setLastSeenAt(OffsetDateTime.parse("2026-06-22T08:00:00Z"));
        UUID shadowPositionId = UUID.randomUUID();
        UUID shadowOperationId = UUID.randomUUID();
        ShadowPositionStateEntity previousState = ShadowPositionStateEntity.builder()
                .id(shadowPositionId)
                .shadowAllocationId(allocation.getId())
                .walletProfileId(allocation.getWalletProfileId())
                .idUser(allocation.getIdUser().toString())
                .walletId(allocation.getWalletId())
                .copyStrategyCode(allocation.getCopyStrategyCode())
                .scopeType(allocation.getScopeType())
                .scopeValue(allocation.getScopeValue())
                .strategyKey(allocation.getStrategyKey())
                .parsymbol("BTCUSDT")
                .positionSide(previousSide.name())
                .qty(BigDecimal.ONE)
                .entryPrice(new BigDecimal("100"))
                .markPrice(new BigDecimal("100"))
                .notionalUsd(new BigDecimal("100"))
                .realizedPnlUsd(BigDecimal.ZERO)
                .feesUsd(BigDecimal.ZERO)
                .slippageUsd(BigDecimal.ZERO)
                .status("OPEN")
                .openedAt(OffsetDateTime.parse("2026-06-22T09:00:00Z"))
                .build();
        ShadowCopyOperationEntity previousOperation = ShadowCopyOperationEntity.builder()
                .idOperation(shadowOperationId)
                .shadowAllocationId(allocation.getId())
                .walletProfileId(allocation.getWalletProfileId())
                .idUser(allocation.getIdUser().toString())
                .idOrderOrigin(UUID.randomUUID().toString())
                .idWalletOrigin(allocation.getWalletId())
                .copyStrategyCode(allocation.getCopyStrategyCode())
                .scopeType(allocation.getScopeType())
                .scopeValue(allocation.getScopeValue())
                .strategyKey(allocation.getStrategyKey())
                .parsymbol("BTCUSDT")
                .typeOperation(previousSide.name())
                .sizePar(BigDecimal.ONE)
                .sizeUsd(new BigDecimal("100"))
                .priceEntry(new BigDecimal("100"))
                .dateCreation(OffsetDateTime.parse("2026-06-22T09:00:00Z"))
                .active(true)
                .status("OPEN")
                .simulatedFeeUsd(BigDecimal.ZERO)
                .simulatedSlippageUsd(BigDecimal.ZERO)
                .build();
        List<ShadowPositionStateEntity> states = new ArrayList<>(List.of(previousState));
        List<ShadowCopyOperationEntity> operations = new ArrayList<>(List.of(previousOperation));
        List<ShadowCopyOperationEventEntity> recordedEvents = new ArrayList<>();
        List<ShadowWalletProfileValidationEntity> validations = new ArrayList<>();

        ShadowCopyTradingServiceImpl service = new ShadowCopyTradingServiceImpl(
                proxy(ShadowCopyAllocationRepository.class, (method, args) -> {
                    if ("findRuntimeProfileRepresentativesByWallet".equals(method.getName())) {
                        return List.of(allocation);
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(ShadowCopyOperationRepository.class, (method, args) -> {
                    String methodName = method.getName();
                    if ("findFirstByWalletProfileIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc".equals(methodName)) {
                        return operations.stream()
                                .filter(ShadowCopyOperationEntity::isActive)
                                .filter(o -> Objects.equals(o.getWalletProfileId(), args[0]))
                                .filter(o -> Objects.equals(o.getParsymbol(), args[1]))
                                .filter(o -> Objects.equals(o.getTypeOperation(), args[2]))
                                .findFirst();
                    }
                    if ("findFirstByShadowAllocationIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc".equals(methodName)) {
                        return operations.stream()
                                .filter(ShadowCopyOperationEntity::isActive)
                                .filter(o -> Objects.equals(o.getShadowAllocationId(), args[0]))
                                .filter(o -> Objects.equals(o.getParsymbol(), args[1]))
                                .filter(o -> Objects.equals(o.getTypeOperation(), args[2]))
                                .findFirst();
                    }
                    if ("findFirstByWalletProfileIdAndIdOrderOriginAndTypeOperationAndActiveTrue".equals(methodName)) {
                        return operations.stream()
                                .filter(ShadowCopyOperationEntity::isActive)
                                .filter(o -> Objects.equals(o.getWalletProfileId(), args[0]))
                                .filter(o -> Objects.equals(o.getIdOrderOrigin(), args[1]))
                                .filter(o -> Objects.equals(o.getTypeOperation(), args[2]))
                                .findFirst();
                    }
                    if ("findFirstByShadowAllocationIdAndIdOrderOriginAndTypeOperationAndActiveTrue".equals(methodName)) {
                        return operations.stream()
                                .filter(ShadowCopyOperationEntity::isActive)
                                .filter(o -> Objects.equals(o.getShadowAllocationId(), args[0]))
                                .filter(o -> Objects.equals(o.getIdOrderOrigin(), args[1]))
                                .filter(o -> Objects.equals(o.getTypeOperation(), args[2]))
                                .findFirst();
                    }
                    if ("save".equals(methodName)) {
                        ShadowCopyOperationEntity operation = (ShadowCopyOperationEntity) args[0];
                        if (operations.stream().noneMatch(o -> Objects.equals(o.getIdOperation(), operation.getIdOperation()))) {
                            operations.add(operation);
                        }
                        return operation;
                    }
                    return unexpected(method);
                }),
                proxy(ShadowCopyOperationEventRepository.class, (method, args) -> {
                    String methodName = method.getName();
                    if ("lockShadowEventIdempotency".equals(methodName)) {
                        return null;
                    }
                    if ("existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime".equals(methodName)) {
                        return recordedEvents.stream().anyMatch(e ->
                                Objects.equals(e.getWalletProfileId(), args[0])
                                        && Objects.equals(e.getIdOrderOrigin(), args[1])
                                        && Objects.equals(e.getEventType(), args[2])
                                        && Objects.equals(e.getPositionSide(), args[3])
                                        && Objects.equals(e.getEventTime(), args[4]));
                    }
                    if ("existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime".equals(methodName)) {
                        return recordedEvents.stream().anyMatch(e ->
                                Objects.equals(e.getShadowAllocationId(), args[0])
                                        && Objects.equals(e.getIdOrderOrigin(), args[1])
                                        && Objects.equals(e.getEventType(), args[2])
                                        && Objects.equals(e.getPositionSide(), args[3])
                                        && Objects.equals(e.getEventTime(), args[4]));
                    }
                    if ("countByWalletProfileIdAndDecision".equals(methodName)) {
                        Long walletProfileId = (Long) args[0];
                        String decision = (String) args[1];
                        return recordedEvents.stream()
                                .filter(e -> walletProfileId.equals(e.getWalletProfileId()))
                                .filter(e -> decision.equals(e.getDecision()))
                                .count();
                    }
                    if ("save".equals(methodName)) {
                        recordedEvents.add((ShadowCopyOperationEventEntity) args[0]);
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(ShadowPositionStateRepository.class, (method, args) -> {
                    String methodName = method.getName();
                    if ("findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatus".equals(methodName)) {
                        return states.stream()
                                .filter(s -> Objects.equals(s.getWalletProfileId(), args[0]))
                                .filter(s -> Objects.equals(s.getParsymbol(), args[1]))
                                .filter(s -> Objects.equals(s.getPositionSide(), args[2]))
                                .filter(s -> Objects.equals(s.getStatus(), args[3]))
                                .findFirst();
                    }
                    if ("findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatus".equals(methodName)) {
                        return states.stream()
                                .filter(s -> Objects.equals(s.getShadowAllocationId(), args[0]))
                                .filter(s -> Objects.equals(s.getParsymbol(), args[1]))
                                .filter(s -> Objects.equals(s.getPositionSide(), args[2]))
                                .filter(s -> Objects.equals(s.getStatus(), args[3]))
                                .findFirst();
                    }
                    if ("findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatusOrderByClosedAtDesc".equals(methodName)
                            || "findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatusOrderByClosedAtDesc".equals(methodName)) {
                        return Optional.empty();
                    }
                    if ("findAllByWalletProfileIdAndParsymbolAndStatus".equals(methodName)) {
                        return states.stream()
                                .filter(s -> Objects.equals(s.getWalletProfileId(), args[0]))
                                .filter(s -> Objects.equals(s.getParsymbol(), args[1]))
                                .filter(s -> Objects.equals(s.getStatus(), args[2]))
                                .toList();
                    }
                    if ("findAllByShadowAllocationIdAndParsymbolAndStatus".equals(methodName)) {
                        return states.stream()
                                .filter(s -> Objects.equals(s.getShadowAllocationId(), args[0]))
                                .filter(s -> Objects.equals(s.getParsymbol(), args[1]))
                                .filter(s -> Objects.equals(s.getStatus(), args[2]))
                                .toList();
                    }
                    if ("sumClosedRealizedPnlUsdByWalletProfileId".equals(methodName)) {
                        return states.stream()
                                .filter(s -> Objects.equals(s.getWalletProfileId(), args[0]))
                                .filter(s -> "CLOSED".equals(s.getStatus()))
                                .map(ShadowPositionStateEntity::getRealizedPnlUsd)
                                .filter(Objects::nonNull)
                                .reduce(BigDecimal.ZERO, BigDecimal::add);
                    }
                    if ("sumSlippageUsdByWalletProfileId".equals(methodName)) {
                        return states.stream()
                                .filter(s -> Objects.equals(s.getWalletProfileId(), args[0]))
                                .map(ShadowPositionStateEntity::getSlippageUsd)
                                .filter(Objects::nonNull)
                                .reduce(BigDecimal.ZERO, BigDecimal::add);
                    }
                    if ("countClosedPositionsByWalletProfileId".equals(methodName)) {
                        return states.stream()
                                .filter(s -> Objects.equals(s.getWalletProfileId(), args[0]))
                                .filter(s -> "CLOSED".equals(s.getStatus()))
                                .count();
                    }
                    if ("countOpenPositionsByWalletProfileId".equals(methodName)) {
                        return states.stream()
                                .filter(s -> Objects.equals(s.getWalletProfileId(), args[0]))
                                .filter(s -> "OPEN".equals(s.getStatus()))
                                .count();
                    }
                    if ("save".equals(methodName)) {
                        ShadowPositionStateEntity state = (ShadowPositionStateEntity) args[0];
                        if (states.stream().noneMatch(s -> Objects.equals(s.getId(), state.getId()))) {
                            states.add(state);
                        }
                        return state;
                    }
                    return unexpected(method);
                }),
                proxy(CopyWalletProfileRepository.class, (method, args) -> {
                    if ("findById".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                shadowProfileValidationRepository(validations),
                new CopyStrategyRuntimeRouter()
        );
        setField(service, "separateShadowEnabled", true);
        setField(service, "shadowVersion", 1);
        setField(service, "shadowSlippageBps", 0.0d);
        setField(service, "shadowWarmupMinutes", 60L);
        return new FlipRuntime(service, previousState, previousOperation, states, operations, recordedEvents);
    }

    private record FlipRuntime(
            ShadowCopyTradingServiceImpl service,
            ShadowPositionStateEntity previousState,
            ShadowCopyOperationEntity previousOperation,
            List<ShadowPositionStateEntity> states,
            List<ShadowCopyOperationEntity> operations,
            List<ShadowCopyOperationEventEntity> recordedEvents
    ) {
    }

    private static ShadowCopyTradingServiceImpl serviceForRuntime(
            List<ShadowCopyAllocationEntity> activeProfiles,
            List<ShadowCopyOperationEventEntity> recordedEvents,
            List<ShadowPositionStateEntity> openedPositions
    ) throws Exception {
        return serviceForRuntime(activeProfiles, recordedEvents, openedPositions, new ArrayList<>());
    }

    private static ShadowCopyTradingServiceImpl serviceForRuntime(
            List<ShadowCopyAllocationEntity> activeProfiles,
            List<ShadowCopyOperationEventEntity> recordedEvents,
            List<ShadowPositionStateEntity> openedPositions,
            List<ShadowWalletProfileValidationEntity> validations
    ) throws Exception {
        ShadowCopyTradingServiceImpl service = new ShadowCopyTradingServiceImpl(
                proxy(ShadowCopyAllocationRepository.class, (method, args) -> {
                    if ("findRuntimeProfileRepresentativesByWallet".equals(method.getName())) {
                        return activeProfiles;
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
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
                    if ("findFirstByWalletProfileIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("findFirstByShadowAllocationIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(ShadowCopyOperationEventRepository.class, (method, args) -> {
                    if ("lockShadowEventIdempotency".equals(method.getName())) {
                        return null;
                    }
                    if ("existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime".equals(method.getName())) {
                        return false;
                    }
                    if ("existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime".equals(method.getName())) {
                        return false;
                    }
                    if ("countByWalletProfileIdAndDecision".equals(method.getName())) {
                        Long walletProfileId = (Long) args[0];
                        String decision = (String) args[1];
                        return recordedEvents.stream()
                                .filter(e -> walletProfileId.equals(e.getWalletProfileId()))
                                .filter(e -> decision.equals(e.getDecision()))
                                .count();
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
                    if ("findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatusOrderByClosedAtDesc".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatusOrderByClosedAtDesc".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("findAllByWalletProfileIdAndParsymbolAndStatus".equals(method.getName())) {
                        return List.of();
                    }
                    if ("findAllByShadowAllocationIdAndParsymbolAndStatus".equals(method.getName())) {
                        return List.of();
                    }
                    if ("sumClosedRealizedPnlUsdByWalletProfileId".equals(method.getName())
                            || "sumSlippageUsdByWalletProfileId".equals(method.getName())) {
                        return BigDecimal.ZERO;
                    }
                    if ("countClosedPositionsByWalletProfileId".equals(method.getName())
                            || "countOpenPositionsByWalletProfileId".equals(method.getName())) {
                        return 0L;
                    }
                    if ("save".equals(method.getName())) {
                        openedPositions.add((ShadowPositionStateEntity) args[0]);
                        return args[0];
                    }
                    return unexpected(method);
                }),
                proxy(CopyWalletProfileRepository.class, (method, args) -> {
                    if ("findById".equals(method.getName())) {
                        return Optional.empty();
                    }
                    if ("save".equals(method.getName())) {
                        return args[0];
                    }
                    return unexpected(method);
                }),
                shadowProfileValidationRepository(validations),
                new CopyStrategyRuntimeRouter()
        );
        setField(service, "separateShadowEnabled", true);
        setField(service, "shadowVersion", 1);
        setField(service, "shadowSlippageBps", 0.0d);
        setField(service, "shadowWarmupMinutes", 60L);
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
                proxy(ShadowCopyOperationEventRepository.class, (method, args) -> {
                    if ("countByWalletProfileIdAndDecision".equals(method.getName())) {
                        return 0L;
                    }
                    return unexpected(method);
                }),
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
        return shadowProfileValidationRepository(new ArrayList<>());
    }

    private static ShadowWalletProfileValidationRepository shadowProfileValidationRepository(List<ShadowWalletProfileValidationEntity> saved) {
        return proxy(ShadowWalletProfileValidationRepository.class, new Invocation() {
            long nextId = 200L;

            @Override
            public Object invoke(Method method, Object[] args) {
                if ("findFirstByWalletProfileIdOrderByStartedAtDesc".equals(method.getName())) {
                    Long walletProfileId = (Long) args[0];
                    return saved.stream()
                            .filter(v -> walletProfileId.equals(v.getWalletProfileId()))
                            .reduce((first, second) -> second);
                }
                if ("save".equals(method.getName())) {
                    ShadowWalletProfileValidationEntity validation = (ShadowWalletProfileValidationEntity) args[0];
                    if (validation.getId() == null) {
                        validation.setId(nextId++);
                        saved.add(validation);
                    } else if (saved.stream().noneMatch(v -> Objects.equals(v.getId(), validation.getId()))) {
                        saved.add(validation);
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
