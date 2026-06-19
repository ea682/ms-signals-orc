package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import com.apunto.engine.entity.ShadowCopyOperationEntity;
import com.apunto.engine.entity.ShadowCopyOperationEventEntity;
import com.apunto.engine.entity.ShadowPositionStateEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.repository.ShadowCopyAllocationRepository;
import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowCopyOperationRepository;
import com.apunto.engine.repository.ShadowPositionStateRepository;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ShadowCopyTradingServiceImpl implements ShadowCopyTradingService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final ShadowCopyAllocationRepository shadowAllocationRepository;
    private final ShadowCopyOperationRepository shadowOperationRepository;
    private final ShadowCopyOperationEventRepository shadowEventRepository;
    private final ShadowPositionStateRepository shadowPositionStateRepository;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;

    @Value("${metric-wallet.shadow.separate-enabled:true}")
    private boolean separateShadowEnabled;

    @Value("${metric-wallet.shadow.max-strategies:80}")
    private int maxShadowStrategies;

    @Value("${metric-wallet.shadow.version:1}")
    private int shadowVersion;

    @Value("${metric-wallet.shadow.require-positive-windows:2w,1mo}")
    private String requirePositiveWindows;

    @Value("${metric-wallet.shadow.slippage-bps:2}")
    private double shadowSlippageBps;

    @Override
    @Transactional
    public void syncShadowAllocations(UUID idUser, List<MetricaWalletDto> candidates, int userMaxWallet, OffsetDateTime now) {
        if (!separateShadowEnabled || idUser == null || candidates == null || candidates.isEmpty()) {
            return;
        }

        OffsetDateTime effectiveNow = now == null ? OffsetDateTime.now() : now;
        int limit = maxShadowStrategies <= 0 ? Math.max(1, userMaxWallet) : maxShadowStrategies;

        List<MetricaWalletDto> relevant = candidates.stream()
                .filter(Objects::nonNull)
                .filter(copyStrategyRuntimeRouter::isCopyableJoyasCandidate)
                .filter(this::isRelevantForShadow)
                .sorted(Comparator
                        .comparingDouble((MetricaWalletDto dto) -> safePct(dto.getCapitalShare()).doubleValue())
                        .thenComparingDouble(this::strategyScore)
                        .reversed())
                .limit(limit)
                .toList();

        Set<String> activeKeys = new HashSet<>();
        int created = 0;
        int updated = 0;
        int validated = 0;
        int rejected = 0;

        for (MetricaWalletDto dto : relevant) {
            String walletId = walletId(dto);
            String strategyCode = strategyCode(dto);
            String scopeType = scopeType(dto);
            String scopeValue = scopeValue(dto, strategyCode);
            if (walletId == null || strategyCode == null) {
                continue;
            }
            String strategyKey = strategyKey(walletId, strategyCode, scopeType, scopeValue);
            activeKeys.add(strategyKey);

            ShadowCopyAllocationEntity entity = shadowAllocationRepository
                    .findActiveStrategy(idUser, walletId, strategyCode, scopeType, scopeValue, shadowVersion)
                    .orElse(null);
            boolean isNew = entity == null;
            if (entity == null) {
                entity = ShadowCopyAllocationEntity.builder()
                        .idUser(idUser)
                        .walletId(walletId)
                        .copyStrategyCode(strategyCode)
                        .scopeType(scopeType)
                        .scopeValue(scopeValue)
                        .strategyKey(strategyKey)
                        .shadowVersion(shadowVersion)
                        .active(true)
                        .createdAt(effectiveNow)
                        .build();
            }

            boolean livePromotable = isLivePromotable(dto);
            String status = shadowStatus(dto);
            String validationReason = validationReason(dto, livePromotable);
            entity.setCopyStrategySlug(strategySlug(dto));
            entity.setCopyStrategyLabel(strategyLabel(dto));
            entity.setCopyMode(copyMode(dto));
            entity.setStrategySourceEndpoint(sourceEndpoint(dto));
            entity.setAllocationPct(safePct(dto.getCapitalShare()));
            entity.setTargetLiveAllocationPct(livePromotable ? safePct(dto.getCapitalShare()) : null);
            entity.setRankWithinStrategy(rankWithinStrategy(dto));
            entity.setGlobalRank(globalRank(dto));
            entity.setStrategyScore(strategyScoreDecimal(dto));
            entity.setDecisionScore(decisionScore(dto));
            entity.setCopyGuardStatus(copyGuardStatus(dto));
            entity.setCopyGuardAction(copyGuardAction(dto));
            entity.setCopyGuardReasons(copyGuardReasons(dto));
            entity.setLastValidationReason(validationReason);
            entity.setStatus(entity.getLinkedLiveAllocationId() != null && livePromotable
                    ? "PROMOTED_TO_LIVE"
                    : status);
            entity.setLastSeenAt(effectiveNow);
            entity.setUpdatedAt(effectiveNow);
            entity.setEndsAt(null);
            entity.setActive(true);
            shadowAllocationRepository.save(entity);

            if (isNew) {
                created++;
                log.info("event=shadow_created userId={} walletId={} strategyCode={} scopeType={} scopeValue={} status={} strategyKey={} reasonCode=shadow_created copyImpact=shadow_tracking",
                        idUser, walletId, strategyCode, scopeType, scopeValue, entity.getStatus(), strategyKey);
            } else {
                updated++;
            }
            if (livePromotable) {
                validated++;
                log.info("event=shadow_validated_for_live userId={} walletId={} strategyCode={} status={} strategyKey={} reasonCode={} copyImpact=live_candidate",
                        idUser, walletId, strategyCode, status, strategyKey, validationReason);
            }
            if (!livePromotable) {
                rejected++;
                log.info("event=shadow_rejected_for_live userId={} walletId={} strategyCode={} status={} reasonCode={} strategyKey={} copyImpact=no_live_open",
                        idUser, walletId, strategyCode, status, validationReason, strategyKey);
            }
        }

        int paused = pauseInactiveUnlinkedShadow(idUser, activeKeys, effectiveNow);
        shadowAllocationRepository.flush();

        log.info("event=shadow_sync_ok userId={} candidates={} relevant={} created={} updated={} validated={} rejected={} paused={} copyImpact=shadow_distribution_synced",
                idUser, candidates.size(), relevant.size(), created, updated, validated, rejected, paused);
    }

    @Override
    @Transactional
    public void linkLiveAllocations(UUID idUser, List<UserCopyAllocationEntity> liveAllocations) {
        if (!separateShadowEnabled || idUser == null || liveAllocations == null || liveAllocations.isEmpty()) {
            return;
        }
        OffsetDateTime now = OffsetDateTime.now();
        for (UserCopyAllocationEntity live : liveAllocations) {
            if (live == null || live.getId() == null || live.getWalletId() == null) {
                continue;
            }
            String strategyCode = normalizeStrategy(live.getCopyStrategyCode());
            String scopeType = normalizeScopeType(live.getScopeType());
            String scopeValue = normalizeScopeValue(live.getScopeValue(), strategyCode);
            ShadowCopyAllocationEntity shadow = shadowAllocationRepository
                    .findActiveStrategy(idUser, live.getWalletId(), strategyCode, scopeType, scopeValue, shadowVersion)
                    .orElse(null);
            if (shadow == null) {
                continue;
            }
            boolean firstPromotion = shadow.getLinkedLiveAllocationId() == null;
            shadow.setLinkedLiveAllocationId(live.getId());
            shadow.setStatus("PROMOTED_TO_LIVE");
            if (shadow.getPromotedToLiveAt() == null) {
                shadow.setPromotedToLiveAt(now);
            }
            shadow.setUpdatedAt(now);
            shadowAllocationRepository.save(shadow);
            live.setLinkedShadowAllocationId(shadow.getId());
            if (live.getPromotedFromShadowAt() == null) {
                live.setPromotedFromShadowAt(now);
            }
            if (live.getStrategyKey() == null || live.getStrategyKey().isBlank()) {
                live.setStrategyKey(shadow.getStrategyKey());
            }
            if (firstPromotion) {
                log.info("event=shadow_promoted_to_live userId={} walletId={} strategyCode={} shadowAllocationId={} liveAllocationId={} strategyKey={} reasonCode=shadow_validated_for_live copyImpact=live_open_allowed",
                        idUser, live.getWalletId(), strategyCode, shadow.getId(), live.getId(), shadow.getStrategyKey());
            } else {
                log.info("event=shadow_continues_after_live userId={} walletId={} strategyCode={} shadowAllocationId={} liveAllocationId={} strategyKey={} reasonCode=shadow_already_linked copyImpact=shadow_monitoring_continues",
                        idUser, live.getWalletId(), strategyCode, shadow.getId(), live.getId(), shadow.getStrategyKey());
            }
        }
    }

    @Override
    @Transactional
    public int recordShadowEvent(OperacionEvent event) {
        if (!separateShadowEnabled || event == null || event.getOperacion() == null) {
            return 0;
        }
        OperacionDto operation = event.getOperacion();
        String walletId = normalizeWallet(operation.getIdCuenta());
        String symbol = normalizeSymbol(operation.getParSymbol());
        if (walletId == null || symbol == null || operation.getIdOperacion() == null) {
            log.warn("event=shadow_event_skipped reasonCode=shadow_payload_incomplete walletId={} symbol={} originId={} copyImpact=no_shadow_event",
                    walletId, symbol, operation.getIdOperacion());
            return 0;
        }
        CopyJobAction action = event.getTipo() == OperacionEvent.Tipo.CERRADA ? CopyJobAction.CLOSE : CopyJobAction.OPEN;
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(event.getDeltaType());
        String side = operation.getTipoOperacion() == null ? null : operation.getTipoOperacion().name();
        List<ShadowCopyAllocationEntity> allocations = shadowAllocationRepository.findRuntimeActiveByWallet(walletId);
        if (allocations.isEmpty()) {
            return 0;
        }

        int recorded = 0;
        for (ShadowCopyAllocationEntity allocation : allocations) {
            if (!copyStrategyRuntimeRouter.strategyCodeAppliesToEvent(allocation.getCopyStrategyCode(), action, deltaType, side)) {
                continue;
            }
            recordShadowForAllocation(allocation, event, action, deltaType);
            recorded++;
        }
        if (recorded > 0) {
            log.info("event=shadow_event_recorded originId={} walletId={} symbol={} action={} deltaType={} allocations={} copyImpact=shadow_event_tracked",
                    operation.getIdOperacion(), walletId, operation.getParSymbol(), action, deltaType, recorded);
        }
        return recorded;
    }

    @Override
    public boolean isSeparateShadowEnabled() {
        return separateShadowEnabled;
    }

    @Override
    public boolean isLivePromotable(MetricaWalletDto candidate) {
        if (!separateShadowEnabled) {
            return true;
        }
        if (candidate == null || !copyStrategyRuntimeRouter.isCopyableJoyasCandidate(candidate)) {
            return false;
        }
        MetricaWalletDto.CopyGuardDto guard = copyGuard(candidate);
        String action = copyGuardAction(candidate);
        String status = copyGuardStatus(candidate);
        if (guard != null && Boolean.FALSE.equals(guard.getAllowNewEntries())) {
            return false;
        }
        if ("SHADOW_ONLY".equals(action) || "PAUSE_OPEN".equals(action) || "DISABLED".equals(action)) {
            return false;
        }
        if ("SHADOW_ONLY".equals(status) || "DATA_RISK".equals(status) || "DISABLED".equals(status)) {
            return false;
        }
        if (!requiredWindowsPositive(candidate)) {
            return false;
        }
        if (!slippageValidationPasses(candidate)) {
            return false;
        }
        return !"SHADOW_REJECTED".equals(shadowStatus(candidate));
    }

    private void recordShadowForAllocation(
            ShadowCopyAllocationEntity allocation,
            OperacionEvent event,
            CopyJobAction action,
            HyperliquidDeltaType deltaType
    ) {
        OperacionDto op = event.getOperacion();
        String originId = op.getIdOperacion().toString();
        String typeOperation = op.getTipoOperacion() == null ? "BOTH" : op.getTipoOperacion().name();
        OffsetDateTime eventTime = eventTime(op, action);
        BigDecimal qty = firstNonNull(op.getSizeQty(), op.getSize(), ZERO).abs();
        BigDecimal notional = firstNonNull(op.getNotionalUsd(), op.getMarginUsedUsd(), ZERO).abs();
        BigDecimal price = firstNonNull(op.getPrecioEntrada(), op.getPrecioMercado(), op.getPrecioCierre());
        BigDecimal slippageUsd = slippageUsd(notional);
        String eventType = shadowEventType(action, deltaType);

        ShadowCopyOperationEntity shadowOperation = null;
        if (action != CopyJobAction.CLOSE) {
            shadowOperation = shadowOperationRepository
                    .findFirstByShadowAllocationIdAndIdOrderOriginAndTypeOperationAndActiveTrue(allocation.getId(), originId, typeOperation)
                    .orElseGet(() -> ShadowCopyOperationEntity.builder()
                            .idOperation(UUID.randomUUID())
                            .shadowAllocationId(allocation.getId())
                            .linkedLiveAllocationId(allocation.getLinkedLiveAllocationId())
                            .idUser(allocation.getIdUser().toString())
                            .idOrderOrigin(originId)
                            .idWalletOrigin(allocation.getWalletId())
                            .copyStrategyCode(allocation.getCopyStrategyCode())
                            .scopeType(allocation.getScopeType())
                            .scopeValue(allocation.getScopeValue())
                            .strategyKey(allocation.getStrategyKey())
                            .parsymbol(op.getParSymbol())
                            .typeOperation(typeOperation)
                            .dateCreation(eventTime)
                            .active(true)
                            .status("OPEN")
                            .build());
            shadowOperation.setSizePar(qty);
            shadowOperation.setSizeUsd(notional);
            shadowOperation.setPriceEntry(price);
            shadowOperation.setSimulatedSlippageUsd(slippageUsd);
            shadowOperation.setSimulatedFeeUsd(ZERO);
            shadowOperationRepository.save(shadowOperation);
        } else {
            shadowOperation = shadowOperationRepository
                    .findFirstByShadowAllocationIdAndIdOrderOriginAndTypeOperationAndActiveTrue(allocation.getId(), originId, typeOperation)
                    .orElse(null);
            if (shadowOperation != null) {
                shadowOperation.setActive(false);
                shadowOperation.setStatus("CLOSED");
                shadowOperation.setDateClose(eventTime);
                shadowOperation.setPriceClose(price);
                shadowOperationRepository.save(shadowOperation);
            }
        }

        updateShadowPositionState(allocation, op, action, deltaType, eventTime, qty, notional, price, slippageUsd, originId, typeOperation);

        ShadowCopyOperationEventEntity shadowEvent = ShadowCopyOperationEventEntity.builder()
                .shadowOperationId(shadowOperation == null ? null : shadowOperation.getIdOperation())
                .shadowAllocationId(allocation.getId())
                .linkedLiveAllocationId(allocation.getLinkedLiveAllocationId())
                .idOrderOrigin(originId)
                .idUser(allocation.getIdUser().toString())
                .idWalletOrigin(allocation.getWalletId())
                .copyStrategyCode(allocation.getCopyStrategyCode())
                .scopeType(allocation.getScopeType())
                .scopeValue(allocation.getScopeValue())
                .strategyKey(allocation.getStrategyKey())
                .parsymbol(op.getParSymbol())
                .typeOperation(typeOperation)
                .eventType(eventType)
                .positionSide(typeOperation)
                .qtyRequested(qty)
                .qtyExecuted(qty)
                .price(price)
                .notionalUsd(notional)
                .resultingQty(action == CopyJobAction.CLOSE ? ZERO : qty)
                .feeUsd(ZERO)
                .slippageBps(BigDecimal.valueOf(shadowSlippageBps).setScale(6, RoundingMode.HALF_UP))
                .slippageUsd(slippageUsd)
                .decision("SIMULATED")
                .decisionReason("shadow_separate_table")
                .source("shadow_copy")
                .reasonCode("shadow_" + eventType.toLowerCase(Locale.ROOT))
                .eventTime(eventTime)
                .dateCreation(OffsetDateTime.now())
                .build();
        shadowEventRepository.save(shadowEvent);
        log.info("event=shadow_allocation_event_recorded originId={} userId={} shadowAllocationId={} liveAllocationId={} walletId={} strategyCode={} executionMode=SHADOW intent={} symbol={} side={} reasonCode={} copyImpact=shadow_event_tracked qty={} notional={} slippageUsd={}",
                originId,
                allocation.getIdUser(),
                allocation.getId(),
                allocation.getLinkedLiveAllocationId(),
                allocation.getWalletId(),
                allocation.getCopyStrategyCode(),
                eventType,
                op.getParSymbol(),
                typeOperation,
                shadowEvent.getReasonCode(),
                qty,
                notional,
                slippageUsd);
    }

    private void updateShadowPositionState(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            OffsetDateTime eventTime,
            BigDecimal qty,
            BigDecimal notional,
            BigDecimal price,
            BigDecimal slippageUsd,
            String originId,
            String positionSide
    ) {
        if (deltaType == HyperliquidDeltaType.FLIP && action != CopyJobAction.CLOSE) {
            closeOtherOpenShadowSides(allocation, op, eventTime, originId, positionSide);
        }

        ShadowPositionStateEntity state = shadowPositionStateRepository
                .findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatus(
                        allocation.getId(),
                        op.getParSymbol(),
                        positionSide,
                        "OPEN"
                )
                .orElse(null);
        if (action == CopyJobAction.CLOSE) {
            if (state == null) {
                return;
            }
            state.setStatus("CLOSED");
            state.setClosedAt(eventTime);
            state.setQty(ZERO);
            state.setMarkPrice(price);
            state.setLastSourceEventId(originId);
            shadowPositionStateRepository.save(state);
            return;
        }
        if (state == null) {
            state = ShadowPositionStateEntity.builder()
                    .id(UUID.randomUUID())
                    .shadowAllocationId(allocation.getId())
                    .linkedLiveAllocationId(allocation.getLinkedLiveAllocationId())
                    .idUser(allocation.getIdUser().toString())
                    .walletId(allocation.getWalletId())
                    .copyStrategyCode(allocation.getCopyStrategyCode())
                    .scopeType(allocation.getScopeType())
                    .scopeValue(allocation.getScopeValue())
                    .strategyKey(allocation.getStrategyKey())
                    .parsymbol(op.getParSymbol())
                    .positionSide(positionSide)
                    .openedAt(eventTime)
                    .status("OPEN")
                    .build();
        }
        state.setQty(qty);
        state.setEntryPrice(price);
        state.setMarkPrice(firstNonNull(op.getPrecioMercado(), price));
        state.setNotionalUsd(notional);
        state.setSlippageUsd(firstNonNull(state.getSlippageUsd(), ZERO).add(slippageUsd));
        state.setLastSourceEventId(originId);
        shadowPositionStateRepository.save(state);
    }

    private void closeOtherOpenShadowSides(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            OffsetDateTime eventTime,
            String originId,
            String newPositionSide
    ) {
        List<ShadowPositionStateEntity> openStates = shadowPositionStateRepository
                .findAllByShadowAllocationIdAndParsymbolAndStatus(allocation.getId(), op.getParSymbol(), "OPEN");
        for (ShadowPositionStateEntity state : openStates) {
            if (state == null || Objects.equals(state.getPositionSide(), newPositionSide)) {
                continue;
            }
            state.setStatus("CLOSED");
            state.setClosedAt(eventTime);
            state.setQty(ZERO);
            state.setMarkPrice(firstNonNull(op.getPrecioMercado(), op.getPrecioEntrada(), op.getPrecioCierre()));
            state.setLastSourceEventId(originId);
            shadowPositionStateRepository.save(state);
            log.info("event=shadow_flip_closed_previous_side shadowAllocationId={} walletId={} strategyCode={} symbol={} previousSide={} newSide={} originId={} reasonCode=shadow_flip_close_previous copyImpact=shadow_position_reconciled",
                    allocation.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), state.getPositionSide(), newPositionSide, originId);
        }
    }

    private int pauseInactiveUnlinkedShadow(UUID idUser, Set<String> activeKeys, OffsetDateTime now) {
        int paused = 0;
        for (ShadowCopyAllocationEntity existing : shadowAllocationRepository.findActiveByUser(idUser)) {
            if (existing == null || existing.isLinkedToLive()) {
                continue;
            }
            if (activeKeys.contains(existing.getStrategyKey())) {
                continue;
            }
            existing.setStatus("SHADOW_PAUSED");
            existing.setEndsAt(now);
            existing.setActive(false);
            existing.setUpdatedAt(now);
            shadowAllocationRepository.save(existing);
            paused++;
        }
        return paused;
    }

    private boolean isRelevantForShadow(MetricaWalletDto dto) {
        if (dto == null || walletId(dto) == null) {
            return false;
        }
        String status = copyGuardStatus(dto);
        String action = copyGuardAction(dto);
        if ("DISABLED".equals(status) || "DISABLED".equals(action)) {
            return false;
        }
        Integer score = decisionScore(dto);
        return score == null || score >= 0;
    }

    private String shadowStatus(MetricaWalletDto dto) {
        String action = copyGuardAction(dto);
        String status = copyGuardStatus(dto);
        if ("SHADOW_ONLY".equals(action) || "SHADOW_ONLY".equals(status)) return "SHADOW_ONLY";
        if ("PAUSE_OPEN".equals(action) || "DISABLED".equals(action) || "DATA_RISK".equals(status)) return "SHADOW_REJECTED";
        if (!requiredWindowsPositive(dto)) return "SHADOW_ACTIVE";
        if (!slippageValidationPasses(dto)) return "SHADOW_ONLY";
        if ("WARNING".equals(action) || "REDUCE_CAPITAL".equals(action) || "HIGH_RISK".equals(status)) return "SHADOW_WARNING";
        return "SHADOW_VALIDATED";
    }

    private boolean requiredWindowsPositive(MetricaWalletDto dto) {
        List<String> windows = parseWindows(requirePositiveWindows);
        if (windows.isEmpty()) {
            return true;
        }
        for (String window : windows) {
            Double pnl = pnlWindow(dto, window);
            if (pnl == null || pnl <= 0.0) {
                return false;
            }
        }
        return true;
    }

    private String validationReason(MetricaWalletDto dto) {
        return validationReason(dto, isLivePromotable(dto));
    }

    private String validationReason(MetricaWalletDto dto, boolean livePromotable) {
        if (livePromotable) {
            return "shadow_filters_passed";
        }
        String action = copyGuardAction(dto);
        String status = copyGuardStatus(dto);
        if ("SHADOW_ONLY".equals(action) || "SHADOW_ONLY".equals(status)) return "shadow_only_by_copy_guard";
        if ("PAUSE_OPEN".equals(action)) return "pause_open_by_copy_guard";
        if ("DATA_RISK".equals(status)) return "data_risk_by_copy_guard";
        if (!requiredWindowsPositive(dto)) {
            return "required_shadow_windows_not_positive:" + String.join(",", parseWindows(requirePositiveWindows));
        }
        if (!slippageValidationPasses(dto)) return "slippage_simulation_failed";
        return "required_shadow_windows_not_positive:" + String.join(",", parseWindows(requirePositiveWindows));
    }

    private boolean slippageValidationPasses(MetricaWalletDto dto) {
        return slippageDecision(dto).passed();
    }

    private SlippageDecision slippageDecision(MetricaWalletDto dto) {
        if (shadowSlippageBps <= 0 || dto == null || dto.getCopySimulation() == null) {
            return SlippageDecision.notEvaluated(true);
        }
        Double totalNet = dto.getCopySimulation().getPnlCopyTotalNetUSDT();
        if (totalNet == null) {
            totalNet = dto.getCopySimulation().getPnlCopyTotalUSDT();
        }
        Double sourceNotional = dto.getTradeStats() == null ? null : dto.getTradeStats().getNotionalTradedUSDT();
        if (totalNet == null || sourceNotional == null || !Double.isFinite(totalNet) || !Double.isFinite(sourceNotional) || sourceNotional <= 0) {
            return SlippageDecision.notEvaluated(true);
        }
        double scale = 1.0;
        MetricaWalletDto.CopySizingDto sizing = dto.getCopySimulation().getCopySizing();
        if (sizing != null && sizing.getAppliedScaleFactor() != null && Double.isFinite(sizing.getAppliedScaleFactor())) {
            scale = Math.max(0.0, sizing.getAppliedScaleFactor());
        } else if (sizing != null && sizing.getAppliedScalePct() != null && Double.isFinite(sizing.getAppliedScalePct())) {
            scale = Math.max(0.0, sizing.getAppliedScalePct() / 100.0);
        }
        double estimatedSlippage = Math.abs(sourceNotional) * scale * shadowSlippageBps / 10_000.0;
        double adjusted = totalNet - estimatedSlippage;
        boolean passed = adjusted > 0.0;
        return new SlippageDecision(true, passed, totalNet, sourceNotional, scale, estimatedSlippage, adjusted);
    }

    private record SlippageDecision(
            boolean evaluated,
            boolean passed,
            Double totalNet,
            Double sourceNotional,
            double scale,
            double estimatedSlippage,
            double adjustedNet
    ) {
        static SlippageDecision notEvaluated(boolean passed) {
            return new SlippageDecision(false, passed, null, null, 1.0, 0.0, 0.0);
        }
    }

    private Double pnlWindow(MetricaWalletDto dto, String window) {
        if (dto == null || dto.getCopySimulation() == null || window == null) {
            return null;
        }
        if (dto.getCopySimulation().getPnlCopyNet() != null && dto.getCopySimulation().getPnlCopyNet().containsKey(window)) {
            return dto.getCopySimulation().getPnlCopyNet().get(window);
        }
        if (dto.getCopySimulation().getPnlCopy() != null && dto.getCopySimulation().getPnlCopy().containsKey(window)) {
            return dto.getCopySimulation().getPnlCopy().get(window);
        }
        if ("1w".equals(window)) return dto.getCopySimulation().getPnlCopyWeekUSDT();
        if ("1mo".equals(window)) return dto.getCopySimulation().getPnlCopyMonthUSDT();
        return null;
    }

    private static List<String> parseWindows(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .toList();
    }

    private String copyGuardStatus(MetricaWalletDto dto) {
        MetricaWalletDto.CopyGuardDto guard = copyGuard(dto);
        return normalizeStatus(guard == null ? null : guard.getStatus());
    }

    private String copyGuardAction(MetricaWalletDto dto) {
        MetricaWalletDto.CopyGuardDto guard = copyGuard(dto);
        return normalizeStatus(guard == null ? null : guard.getAction());
    }

    private String copyGuardReasons(MetricaWalletDto dto) {
        MetricaWalletDto.CopyGuardDto guard = copyGuard(dto);
        if (guard == null || guard.getReasons() == null || guard.getReasons().isEmpty()) {
            return null;
        }
        return guard.getReasons().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .limit(20)
                .collect(Collectors.joining("|"));
    }

    private static MetricaWalletDto.CopyGuardDto copyGuard(MetricaWalletDto dto) {
        if (dto == null) return null;
        if (dto.getRealJewel() != null && dto.getRealJewel().getCopyGuard() != null) return dto.getRealJewel().getCopyGuard();
        if (dto.getStrategy() != null && dto.getStrategy().getCopyGuard() != null) return dto.getStrategy().getCopyGuard();
        if (dto.getStrategy() != null
                && dto.getStrategy().getRiskAdjustedCapitalEfficiency() != null
                && dto.getStrategy().getRiskAdjustedCapitalEfficiency().getCopyGuard() != null) {
            return dto.getStrategy().getRiskAdjustedCapitalEfficiency().getCopyGuard();
        }
        return null;
    }

    private String walletId(MetricaWalletDto dto) {
        return dto == null || dto.getWallet() == null ? null : normalizeWallet(dto.getWallet().getIdWallet());
    }

    private String strategyCode(MetricaWalletDto dto) {
        return normalizeStrategy(copyStrategyRuntimeRouter.strategyCodeOf(dto));
    }

    private String strategySlug(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getStrategySlug();
    }

    private String strategyLabel(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getStrategyLabel();
    }

    private String copyMode(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getCopyMode();
    }

    private String sourceEndpoint(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getSourceEndpoint();
    }

    private Integer rankWithinStrategy(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getRankWithinStrategy();
    }

    private Integer globalRank(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getGlobalRank();
    }

    private Integer decisionScore(MetricaWalletDto dto) {
        return dto == null || dto.getScoring() == null ? null : dto.getScoring().getDecisionMetricConservative();
    }

    private double strategyScore(MetricaWalletDto dto) {
        if (dto == null || dto.getStrategy() == null || dto.getStrategy().getScore() == null) {
            return 0.0;
        }
        return dto.getStrategy().getScore();
    }

    private BigDecimal strategyScoreDecimal(MetricaWalletDto dto) {
        if (dto == null || dto.getStrategy() == null || dto.getStrategy().getScore() == null) {
            return null;
        }
        return BigDecimal.valueOf(dto.getStrategy().getScore()).setScale(6, RoundingMode.HALF_UP);
    }

    private String scopeType(MetricaWalletDto dto) {
        String fromBreakdown = dto != null && dto.getWallet() != null && dto.getWallet().getCountOperationBreakdown() != null
                ? dto.getWallet().getCountOperationBreakdown().getScopeType()
                : null;
        String fromJewel = dto != null && dto.getRealJewel() != null ? dto.getRealJewel().getScopeType() : null;
        return normalizeScopeType(firstNonBlank(fromBreakdown, fromJewel, "strategy"));
    }

    private String scopeValue(MetricaWalletDto dto, String strategyCode) {
        String fromBreakdown = dto != null && dto.getWallet() != null && dto.getWallet().getCountOperationBreakdown() != null
                ? dto.getWallet().getCountOperationBreakdown().getScopeValue()
                : null;
        String fromJewel = dto != null && dto.getRealJewel() != null ? dto.getRealJewel().getScopeValue() : null;
        return normalizeScopeValue(firstNonBlank(fromBreakdown, fromJewel, strategyCode), strategyCode);
    }

    private static String strategyKey(String walletId, String strategyCode, String scopeType, String scopeValue) {
        return normalizeWallet(walletId) + "|" + normalizeStrategy(strategyCode) + "|" + normalizeScopeType(scopeType) + "|" + normalizeScopeValue(scopeValue, strategyCode);
    }

    private static String shadowEventType(CopyJobAction action, HyperliquidDeltaType deltaType) {
        if (action == CopyJobAction.CLOSE || deltaType == HyperliquidDeltaType.CLOSE) return "CLOSE";
        if (deltaType == HyperliquidDeltaType.FLIP) return "FLIP";
        if (deltaType == HyperliquidDeltaType.RESIZE) return "INCREASE";
        if (deltaType == HyperliquidDeltaType.UPDATE) return "UPDATE";
        return "OPEN";
    }

    private static OffsetDateTime eventTime(OperacionDto op, CopyJobAction action) {
        java.time.Instant instant = action == CopyJobAction.CLOSE && op != null && op.getFechaCierre() != null
                ? op.getFechaCierre()
                : op == null ? null : op.getFechaCreacion();
        if (instant == null) {
            return OffsetDateTime.now();
        }
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private BigDecimal slippageUsd(BigDecimal notional) {
        if (notional == null || notional.signum() <= 0 || shadowSlippageBps <= 0) {
            return ZERO;
        }
        return notional.multiply(BigDecimal.valueOf(shadowSlippageBps))
                .divide(BigDecimal.valueOf(10_000), 12, RoundingMode.HALF_UP);
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values) {
        if (values == null) return null;
        for (T value : values) {
            if (value != null) return value;
        }
        return null;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    private static BigDecimal safePct(double value) {
        if (!Double.isFinite(value)) {
            return ZERO.setScale(6, RoundingMode.HALF_UP);
        }
        return BigDecimal.valueOf(Math.max(0.0, value)).setScale(6, RoundingMode.HALF_UP);
    }

    private static String normalizeWallet(String raw) {
        if (raw == null || raw.isBlank()) return null;
        return raw.trim().toLowerCase(Locale.ROOT);
    }

    private static String normalizeSymbol(String raw) {
        if (raw == null || raw.isBlank()) return null;
        return raw.trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizeStrategy(String raw) {
        if (raw == null || raw.isBlank()) return "MOVEMENT_ALL";
        return raw.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String normalizeScopeType(String raw) {
        if (raw == null || raw.isBlank()) return "strategy";
        return raw.trim().toLowerCase(Locale.ROOT);
    }

    private static String normalizeScopeValue(String raw, String strategyCode) {
        if (raw == null || raw.isBlank()) return strategyCode == null ? "default" : strategyCode;
        return raw.trim();
    }

    private static String normalizeStatus(String raw) {
        if (raw == null || raw.isBlank()) return null;
        return raw.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }
}
