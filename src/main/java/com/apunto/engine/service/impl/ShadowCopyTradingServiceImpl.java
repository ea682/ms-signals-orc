package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.CopyWalletProfileEntity;
import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import com.apunto.engine.entity.ShadowCopyOperationEntity;
import com.apunto.engine.entity.ShadowCopyOperationEventEntity;
import com.apunto.engine.entity.ShadowPositionStateEntity;
import com.apunto.engine.entity.ShadowWalletProfileValidationEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.repository.CopyWalletProfileRepository;
import com.apunto.engine.repository.ShadowCopyAllocationRepository;
import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowCopyOperationRepository;
import com.apunto.engine.repository.ShadowPositionStateRepository;
import com.apunto.engine.repository.ShadowWalletProfileValidationRepository;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.apunto.engine.service.copy.accounting.AccountingMode;
import com.apunto.engine.service.copy.accounting.CopyAccountingInput;
import com.apunto.engine.service.copy.accounting.CopyAccountingResult;
import com.apunto.engine.service.copy.accounting.CopyPositionAccountingService;
import com.apunto.engine.service.copy.accounting.PositionDeltaClassification;
import com.apunto.engine.service.copy.accounting.PositionDeltaClassificationInput;
import com.apunto.engine.service.copy.accounting.PositionDeltaType;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.service.copy.observability.CopyFlowTiming;
import com.apunto.engine.shared.enums.PositionSide;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ShadowCopyTradingServiceImpl implements ShadowCopyTradingService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final ThreadLocal<CopyFlowTiming> SHADOW_FLOW_TIMING = new ThreadLocal<>();

    private record ShadowPositionImpact(
            String reasonCode,
            String shadowImpact,
            BigDecimal realizedPnlUsd,
            UUID shadowPositionId,
            boolean positionUpdated,
            BigDecimal previousQty,
            BigDecimal resultingQty,
            BigDecimal qtyExecuted,
            BigDecimal eventNotionalUsd,
            BigDecimal eventSlippageUsd
    ) {
        private static ShadowPositionImpact of(
                String reasonCode,
                String shadowImpact,
                BigDecimal realizedPnlUsd,
                UUID shadowPositionId,
                boolean positionUpdated
        ) {
            return new ShadowPositionImpact(reasonCode, shadowImpact, realizedPnlUsd, shadowPositionId, positionUpdated, null, null, null, null, null);
        }

        private ShadowPositionImpact withAccounting(
                BigDecimal previousQty,
                BigDecimal resultingQty,
                BigDecimal qtyExecuted,
                BigDecimal eventNotionalUsd,
                BigDecimal eventSlippageUsd
        ) {
            return new ShadowPositionImpact(reasonCode, shadowImpact, realizedPnlUsd, shadowPositionId, positionUpdated,
                    previousQty, resultingQty, qtyExecuted, eventNotionalUsd, eventSlippageUsd);
        }

        static ShadowPositionImpact opened(UUID shadowPositionId) {
            return of("SHADOW_POSITION_OPENED", "POSITION_OPENED", null, shadowPositionId, true);
        }

        static ShadowPositionImpact openedByFlip(UUID shadowPositionId) {
            return of("SHADOW_POSITION_OPENED_BY_FLIP", "POSITION_OPENED_BY_FLIP", null, shadowPositionId, true);
        }

        static ShadowPositionImpact resized(UUID shadowPositionId) {
            return of("SHADOW_POSITION_RESIZED", "POSITION_RESIZED", null, shadowPositionId, true);
        }

        static ShadowPositionImpact reduced(BigDecimal pnl, UUID shadowPositionId) {
            return of("SHADOW_POSITION_REDUCED", "POSITION_REDUCED", pnl, shadowPositionId, true);
        }

        static ShadowPositionImpact resizeNoop(UUID shadowPositionId) {
            return of("SHADOW_RESIZE_NOOP", "NOOP", null, shadowPositionId, true);
        }

        static ShadowPositionImpact closed(BigDecimal pnl, UUID shadowPositionId) {
            return of("SHADOW_POSITION_CLOSED", "POSITION_CLOSED", pnl, shadowPositionId, true);
        }

        static ShadowPositionImpact resizeWithoutOpen() {
            return of("RESIZE_WITHOUT_SHADOW_OPEN", "NO_VALID_POSITION", null, null, false);
        }

        static ShadowPositionImpact warmupResizeWithoutOpen() {
            return of("WARMUP_RESIZE_WITHOUT_SHADOW_OPEN", "WARMUP_NO_VALID_POSITION", null, null, false);
        }

        static ShadowPositionImpact resizeAfterClosed(UUID shadowPositionId) {
            return of("RESIZE_AFTER_SHADOW_CLOSED", "NO_VALID_POSITION", null, shadowPositionId, false);
        }

        static ShadowPositionImpact closeWithoutOpen() {
            return of("CLOSE_WITHOUT_SHADOW_OPEN", "NO_VALID_PNL", null, null, false);
        }

        static ShadowPositionImpact warmupCloseWithoutOpen() {
            return of("WARMUP_CLOSE_WITHOUT_SHADOW_OPEN", "WARMUP_NO_VALID_PNL", null, null, false);
        }

        static ShadowPositionImpact flipWithoutOpen() {
            return of("FLIP_WITHOUT_SHADOW_OPEN", "NO_VALID_POSITION", null, null, false);
        }

        static ShadowPositionImpact warmupFlipWithoutOpen() {
            return of("WARMUP_FLIP_WITHOUT_SHADOW_OPEN", "WARMUP_NO_VALID_POSITION", null, null, false);
        }

        static ShadowPositionImpact priceMissing(String reasonCode) {
            return of(reasonCode, "PRICE_MISSING", null, null, false);
        }

        static ShadowPositionImpact entryPriceMissing(UUID shadowPositionId) {
            return of("ENTRY_PRICE_MISSING", "ENTRY_PRICE_MISSING", null, shadowPositionId, false);
        }

        static ShadowPositionImpact newExposureBlockedByRankingExit(UUID shadowPositionId) {
            return of("SHADOW_NEW_EXPOSURE_BLOCKED_BY_RANKING_EXIT", "NEW_EXPOSURE_BLOCKED", null, shadowPositionId, false);
        }
    }

    private final ShadowCopyAllocationRepository shadowAllocationRepository;
    private final ShadowCopyOperationRepository shadowOperationRepository;
    private final ShadowCopyOperationEventRepository shadowEventRepository;
    private final ShadowPositionStateRepository shadowPositionStateRepository;
    private final CopyWalletProfileRepository copyWalletProfileRepository;
    private final ShadowWalletProfileValidationRepository shadowProfileValidationRepository;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;
    private final CopyPositionAccountingService copyPositionAccountingService;

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

    @Value("${metric-wallet.shadow.require-validation-before-live:true}")
    private boolean requireShadowValidationBeforeLive;

    @Value("${metric-wallet.shadow.min-closed-operations-for-live:50}")
    private int minShadowClosedOperationsForLive;

    @Value("${metric-wallet.shadow.min-net-pnl-usdt-for-live:0}")
    private BigDecimal minShadowNetPnlUsdtForLive;

    @Value("${metric-wallet.shadow.warmup-minutes:60}")
    private long shadowWarmupMinutes;

    @Value("${copy.live-readiness.evidence-micro-threshold-a:55}")
    private double evidenceMicroThresholdA = 55.0;

    @Value("${copy.live-readiness.evidence-micro-threshold-b:60}")
    private double evidenceMicroThresholdB = 60.0;

    @Value("${copy.live-readiness.evidence-micro-threshold-c:70}")
    private double evidenceMicroThresholdC = 70.0;

    @Value("${copy.live-readiness.evidence-micro-threshold-d:80}")
    private double evidenceMicroThresholdD = 80.0;

    @Override
    @Transactional
    public void syncShadowAllocations(UUID idUser, List<MetricaWalletDto> candidates, int userMaxWallet, OffsetDateTime now) {
        if (!separateShadowEnabled || idUser == null || candidates == null || candidates.isEmpty()) {
            return;
        }

        OffsetDateTime effectiveNow = now == null ? OffsetDateTime.now() : now;
        candidates.stream()
                .filter(Objects::nonNull)
                .forEach(this::logMetricJewelItemSeen);

        List<MetricaWalletDto> relevant = candidates.stream()
                .filter(Objects::nonNull)
                .filter(copyStrategyRuntimeRouter::isShadowEligibleJoyasCandidate)
                .filter(this::isRelevantForShadow)
                .sorted(Comparator
                        .comparingDouble((MetricaWalletDto dto) -> safePct(dto.getCapitalShare()).doubleValue())
                        .thenComparingDouble(this::strategyScore)
                        .reversed())
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
            CopyWalletProfileEntity profile = upsertWalletProfile(
                    dto,
                    walletId,
                    strategyCode,
                    scopeType,
                    scopeValue,
                    strategyKey,
                    "SHADOW_TESTING",
                    "profile_seen",
                    effectiveNow
            );

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
                        .walletProfileId(profile.getId())
                        .shadowVersion(shadowVersion)
                        .active(true)
                        .createdAt(effectiveNow)
                        .build();
            }

            boolean livePromotable = isLivePromotable(idUser, dto);
            String status = shadowStatus(idUser, dto);
            String validationReason = validationReason(idUser, dto, livePromotable);
            String profileStatus = profileStatus(status, livePromotable);
            profile = upsertWalletProfile(
                    dto,
                    walletId,
                    strategyCode,
                    scopeType,
                    scopeValue,
                    strategyKey,
                    profileStatus,
                    validationReason,
                    effectiveNow
            );
            ShadowWalletProfileValidationEntity validation = upsertProfileValidation(profile.getId(), profileStatus, validationReason, effectiveNow);
            entity.setCopyStrategySlug(strategySlug(dto));
            entity.setCopyStrategyLabel(strategyLabel(dto));
            entity.setCopyMode(copyMode(dto));
            entity.setStrategySourceEndpoint(sourceEndpoint(dto));
            entity.setWalletProfileId(profile.getId());
            entity.setShadowValidationId(validation.getId());
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
            MetricaWalletDto.ActivityDto walletActivity = walletActivity(dto);
            MetricaWalletDto.ActivityDto strategyActivity = strategyActivity(dto);
            entity.setWalletLastActivityAt(firstNonNull(walletActivity == null ? null : walletActivity.getWalletLastActivityAt(), walletActivity == null ? null : walletActivity.getLastActivityAt()));
            entity.setWalletLastOpenedAt(firstNonNull(walletActivity == null ? null : walletActivity.getWalletLastOpenedAt(), walletActivity == null ? null : walletActivity.getLastOpenedAt()));
            entity.setWalletLastClosedAt(firstNonNull(walletActivity == null ? null : walletActivity.getWalletLastClosedAt(), walletActivity == null ? null : walletActivity.getLastClosedAt()));
            entity.setStrategyLastActivityAt(strategyLastActivityAt(dto));
            entity.setStrategyLastOpenedAt(strategyLastOpenedAt(dto));
            entity.setStrategyLastClosedAt(strategyLastClosedAt(dto));
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
                log.info("event=copy_profile_created userId={} walletId={} copyProfileCode={} scopeType={} scopeValue={} status={} profileKey={} reasonCode=PROFILE_CREATED shadowImpact=SHADOW_TRACKING",
                        idUser, walletId, strategyCode, scopeType, scopeValue, entity.getStatus(), strategyKey);
                log.info("event=copy_profile_shadow_started userId={} walletId={} copyProfileCode={} scopeType={} scopeValue={} status={} profileKey={} reasonCode=PROFILE_SENT_TO_SHADOW shadowImpact=SHADOW_TESTING",
                        idUser, walletId, strategyCode, scopeType, scopeValue, entity.getStatus(), strategyKey);
            } else {
                updated++;
                log.info("event=copy_profile_updated userId={} walletId={} copyProfileCode={} scopeType={} scopeValue={} status={} profileKey={} reasonCode=PROFILE_UPDATED shadowImpact=SHADOW_PROFILE_UPDATED",
                        idUser, walletId, strategyCode, scopeType, scopeValue, entity.getStatus(), strategyKey);
            }
            if (livePromotable) {
                validated++;
                log.info("event=copy_profile_validated userId={} walletId={} copyProfileCode={} status={} profileKey={} reasonCode={} liveImpact=LIVE_ELIGIBLE",
                        idUser, walletId, strategyCode, status, strategyKey, validationReason);
            }
            if (!livePromotable) {
                rejected++;
                log.info("event=copy_profile_rejected userId={} walletId={} copyProfileCode={} status={} reasonCode={} profileKey={} liveImpact=NO_LIVE_OPEN",
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
            live.setWalletProfileId(shadow.getWalletProfileId());
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

    private CopyWalletProfileEntity upsertWalletProfile(
            MetricaWalletDto dto,
            String walletId,
            String strategyCode,
            String scopeType,
            String scopeValue,
            String profileKey,
            String status,
            String validationReason,
            OffsetDateTime now
    ) {
        CopyWalletProfileEntity profile = copyWalletProfileRepository.findByProfileKey(profileKey)
                .orElseGet(() -> CopyWalletProfileEntity.builder()
                        .walletId(walletId)
                        .copyProfileCode(strategyCode)
                        .scopeType(scopeType)
                        .scopeValue(scopeValue)
                        .profileKey(profileKey)
                        .createdAt(now)
                        .build());
        profile.setWalletId(walletId);
        profile.setCopyProfileCode(strategyCode);
        profile.setCopyProfileCategory(copyStrategyRuntimeRouter.profileCategory(strategyCode).name());
        profile.setScopeType(scopeType);
        profile.setScopeValue(scopeValue);
        profile.setProfileConfigHash(profileConfigHash(strategyCode, scopeType, scopeValue));
        profile.setProfileKey(profileKey);
        profile.setStatus(status == null ? "SHADOW_TESTING" : status);
        profile.setHistoricalScore(strategyScoreDecimal(dto));
        profile.setValidatedRankingScore("LIVE_ELIGIBLE".equals(status) || "VALIDATED".equals(status) ? strategyScoreDecimal(dto) : null);
        profile.setCopyGuardStatus(copyGuardStatus(dto));
        profile.setCopyGuardAction(copyGuardAction(dto));
        MetricaWalletDto.ActivityDto walletActivity = walletActivity(dto);
        MetricaWalletDto.ActivityDto strategyActivity = strategyActivity(dto);
        profile.setWalletLastActivityAt(firstNonNull(walletActivity == null ? null : walletActivity.getWalletLastActivityAt(), walletActivity == null ? null : walletActivity.getLastActivityAt()));
        profile.setWalletLastOpenedAt(firstNonNull(walletActivity == null ? null : walletActivity.getWalletLastOpenedAt(), walletActivity == null ? null : walletActivity.getLastOpenedAt()));
        profile.setWalletLastClosedAt(firstNonNull(walletActivity == null ? null : walletActivity.getWalletLastClosedAt(), walletActivity == null ? null : walletActivity.getLastClosedAt()));
        profile.setStrategyLastActivityAt(strategyLastActivityAt(dto));
        profile.setStrategyLastOpenedAt(strategyLastOpenedAt(dto));
        profile.setStrategyLastClosedAt(strategyLastClosedAt(dto));
        profile.setLastValidationReason(validationReason);
        profile.setLastValidationReasonCode(reasonCode(validationReason));
        profile.setLastSeenAt(now);
        profile.setUpdatedAt(now);
        return copyWalletProfileRepository.save(profile);
    }

    private ShadowWalletProfileValidationEntity upsertProfileValidation(
            Long walletProfileId,
            String status,
            String validationReason,
            OffsetDateTime now
    ) {
        ShadowWalletProfileValidationEntity validation = shadowProfileValidationRepository
                .findFirstByWalletProfileIdOrderByStartedAtDesc(walletProfileId)
                .orElseGet(() -> ShadowWalletProfileValidationEntity.builder()
                        .walletProfileId(walletProfileId)
                        .startedAt(now)
                        .createdAt(now)
                        .build());
        BigDecimal net = firstNonNull(shadowPositionStateRepository.sumClosedRealizedPnlUsdByWalletProfileId(walletProfileId), ZERO);
        BigDecimal slippage = firstNonNull(shadowPositionStateRepository.sumSlippageUsdByWalletProfileId(walletProfileId), ZERO);
        long closed = shadowPositionStateRepository.countClosedPositionsByWalletProfileId(walletProfileId);
        long open = shadowPositionStateRepository.countOpenPositionsByWalletProfileId(walletProfileId);
        long simulated = shadowEventRepository.countByWalletProfileIdAndDecision(walletProfileId, "SIMULATED");
        long recorded = shadowEventRepository.countByWalletProfileIdAndDecision(walletProfileId, "RECORDED");
        long skipped = shadowEventRepository.countByWalletProfileIdAndDecision(walletProfileId, "SKIPPED");
        long duplicates = shadowEventRepository.countByWalletProfileIdAndDecision(walletProfileId, "DUPLICATE");
        long errors = shadowEventRepository.countByWalletProfileIdAndDecision(walletProfileId, "ERROR");
        validation.setStatus(status == null ? "SHADOW_TESTING" : status);
        validation.setClosedPositions(closed);
        validation.setOpenPositions(open);
        validation.setNetPnlUsd(net);
        validation.setGrossPnlUsd(net.add(slippage));
        validation.setFeesUsd(ZERO);
        validation.setSlippageUsd(slippage);
        validation.setSimulatedEvents(simulated);
        validation.setRecordedEvents(recorded);
        validation.setSkippedEvents(skipped);
        validation.setDuplicateEvents(duplicates);
        validation.setErrorEvents(errors);
        validation.setLastValidationReason(validationReason);
        validation.setLastValidationReasonCode(reasonCode(validationReason));
        validation.setUpdatedAt(now);
        if (isValidatedStatus(status) && validation.getValidatedAt() == null) {
            validation.setValidatedAt(now);
        }
        if (isRejectedStatus(status) && validation.getRejectedAt() == null) {
            validation.setRejectedAt(now);
        }
        return shadowProfileValidationRepository.save(validation);
    }

    private static boolean isValidatedStatus(String status) {
        return "VALIDATED".equals(status) || "LIVE_ELIGIBLE".equals(status) || "PROMOTED_TO_LIVE".equals(status);
    }

    private static boolean isRejectedStatus(String status) {
        return "SHADOW_REJECTED".equals(status) || "REJECTED".equals(status);
    }

    private static String profileStatus(String shadowStatus, boolean livePromotable) {
        if (livePromotable) {
            return "LIVE_ELIGIBLE";
        }
        if ("SHADOW_VALIDATED".equals(shadowStatus) || "VALIDATED".equals(shadowStatus)) {
            return "VALIDATED";
        }
        if (shadowStatus == null || shadowStatus.isBlank()) {
            return "SHADOW_TESTING";
        }
        return shadowStatus;
    }

    private static String reasonCode(String reason) {
        if (reason == null || reason.isBlank()) {
            return null;
        }
        String clean = reason.trim();
        int idx = clean.indexOf(':');
        if (idx > 0) {
            clean = clean.substring(0, idx);
        }
        return clean.length() > 120 ? clean.substring(0, 120) : clean;
    }

    @Override
    @Transactional
    public int recordShadowEvent(OperacionEvent event) {
        return recordShadowEvent(event, 0L);
    }

    @Override
    @Transactional
    public int recordShadowEvent(OperacionEvent event, long eventReceivedNs) {
        if (!separateShadowEnabled || event == null || event.getOperacion() == null) {
            return 0;
        }
        OperacionDto operation = event.getOperacion();
        String walletId = normalizeWallet(operation.getIdCuenta());
        String symbol = normalizeSymbol(operation.getParSymbol());
        if (walletId == null || symbol == null || operation.getIdOperacion() == null) {
            log.warn("event=shadow_ingest_failed reasonCode=SHADOW_PAYLOAD_INCOMPLETE walletId={} symbol={} originId={} copyImpact=NO_SHADOW_EVENT",
                    walletId, symbol, operation.getIdOperacion());
            return 0;
        }
        CopyJobAction action = event.getTipo() == OperacionEvent.Tipo.CERRADA ? CopyJobAction.CLOSE : CopyJobAction.OPEN;
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(event.getDeltaType());
        String side = operation.getTipoOperacion() == null ? null : operation.getTipoOperacion().name();
        String originId = operation.getIdOperacion().toString();

        log.info("event=shadow_ingest_started originId={} walletId={} symbol={} side={} action={} deltaType={} reasonCode=SHADOW_BEFORE_LIVE copyImpact=SHADOW_EVALUATING",
                originId, walletId, operation.getParSymbol(), side, action, deltaType);

        List<ShadowCopyAllocationEntity> allocations = runtimeProfileRepresentatives(walletId);
        if (allocations.isEmpty()) {
            log.info("event=shadow_ingest_skipped_no_active_profile originId={} walletId={} symbol={} side={} action={} deltaType={} reasonCode=NO_ACTIVE_SHADOW_PROFILE shadowImpact=NO_SHADOW_EVENT",
                    originId, walletId, operation.getParSymbol(), side, action, deltaType);
            return 0;
        }

        int recorded = 0;
        int filtered = 0;
        int duplicates = 0;
        for (ShadowCopyAllocationEntity allocation : allocations) {
            String strategyCode = allocation.getCopyStrategyCode();
            if (!copyStrategyRuntimeRouter.strategyCodeAppliesToEvent(strategyCode, allocation.getScopeValue(), action, deltaType, side, operation.getParSymbol())) {
                filtered++;
                String reasonCode = profileFilterReason(strategyCode, allocation.getScopeValue(), action, deltaType, side, operation.getParSymbol());
                log.info("event=hyperliquid_delta_profile_filtered originId={} shadowAllocationId={} userId={} walletId={} copyProfileCode={} scopeType={} scopeValue={} symbol={} side={} action={} deltaType={} reasonCode={} reasonMessage=\"El delta no aplica a esta estrategia shadow\" shadowImpact=NO_SHADOW_EVENT",
                        originId, allocation.getId(), allocation.getIdUser(), walletId, strategyCode, allocation.getScopeType(), allocation.getScopeValue(), operation.getParSymbol(), side, action, deltaType, reasonCode);
                continue;
            }
            log.info("event=hyperliquid_delta_profile_matched originId={} shadowAllocationId={} userId={} walletId={} copyProfileCode={} scopeType={} scopeValue={} symbol={} side={} action={} deltaType={} reasonCode=DELTA_MATCHED_STRATEGY reasonMessage=\"El delta aplica a esta estrategia y sera simulado en shadow\" shadowImpact=EVENT_WILL_BE_RECORDED",
                    originId, allocation.getId(), allocation.getIdUser(), walletId, strategyCode, allocation.getScopeType(), allocation.getScopeValue(), operation.getParSymbol(), side, action, deltaType);
            boolean saved = recordShadowForAllocation(allocation, event, action, deltaType, eventReceivedNs);
            if (saved) {
                recorded++;
            } else {
                duplicates++;
            }
        }
        log.info("event=shadow_ingest_completed originId={} walletId={} symbol={} side={} action={} deltaType={} activeProfiles={} recorded={} filtered={} duplicates={} reasonCode=SHADOW_INGEST_COMPLETED shadowImpact={}",
                originId, walletId, operation.getParSymbol(), side, action, deltaType, allocations.size(), recorded, filtered, duplicates, recorded > 0 ? "SHADOW_EVENT_RECORDED" : "NO_SHADOW_EVENT");
        return recorded;
    }

    private List<ShadowCopyAllocationEntity> runtimeProfileRepresentatives(String walletId) {
        List<ShadowCopyAllocationEntity> queried = shadowAllocationRepository.findRuntimeProfileRepresentativesByWallet(walletId);
        if (queried == null || queried.isEmpty()) {
            return List.of();
        }
        Map<String, ShadowCopyAllocationEntity> byProfile = new LinkedHashMap<>();
        for (ShadowCopyAllocationEntity allocation : queried) {
            if (allocation == null) {
                continue;
            }
            String key = runtimeProfileKey(allocation);
            if (key != null) {
                byProfile.putIfAbsent(key, allocation);
            }
        }
        return List.copyOf(byProfile.values());
    }

    private String runtimeProfileKey(ShadowCopyAllocationEntity allocation) {
        if (allocation == null) {
            return null;
        }
        if (allocation.getWalletProfileId() != null) {
            return "profile:" + allocation.getWalletProfileId();
        }
        if (allocation.getStrategyKey() != null && !allocation.getStrategyKey().isBlank()) {
            return "key:" + allocation.getStrategyKey();
        }
        return strategyKey(
                allocation.getWalletId(),
                allocation.getCopyStrategyCode(),
                allocation.getScopeType(),
                allocation.getScopeValue()
        );
    }

    private String profileFilterReason(String strategyCode, String scopeValue, CopyJobAction action, HyperliquidDeltaType deltaType, String side, String symbol) {
        CopyStrategyRuntimeRouter.CopyProfileCategory category = copyStrategyRuntimeRouter.profileCategory(strategyCode);
        if (category == CopyStrategyRuntimeRouter.CopyProfileCategory.SCORING_WINDOW) {
            return "SCORING_WINDOW_NOT_COPY_PROFILE";
        }
        if (category == CopyStrategyRuntimeRouter.CopyProfileCategory.ROBUSTNESS_CHECK) {
            return "ROBUSTNESS_CHECK_NOT_LIVE_PROFILE";
        }
        if (category == CopyStrategyRuntimeRouter.CopyProfileCategory.DIAGNOSTIC_ONLY) {
            return "DIAGNOSTIC_ONLY_NOT_LIVE_PROFILE";
        }
        if (category == CopyStrategyRuntimeRouter.CopyProfileCategory.UNKNOWN) {
            return "UNKNOWN_COPY_PROFILE";
        }
        String code = normalizeStrategy(strategyCode);
        String sideCode = normalizeStatus(side);
        if ("LONG_ONLY".equals(code) && sideCode != null && !"LONG".equals(sideCode)) {
            return "SIDE_NOT_ALLOWED_BY_STRATEGY";
        }
        if ("SHORT_ONLY".equals(code) && sideCode != null && !"SHORT".equals(sideCode)) {
            return "SIDE_NOT_ALLOWED_BY_STRATEGY";
        }
        if ("SYMBOL_SPECIALIST".equals(code)) {
            String expected = normalizeSymbol(scopeValue);
            String actual = normalizeSymbol(symbol);
            if (expected != null && actual != null && !"ALL".equals(expected) && !expected.equals(actual)) {
                return "SYMBOL_NOT_ALLOWED_BY_STRATEGY";
            }
        }
        if (deltaType == HyperliquidDeltaType.UPDATE || deltaType == HyperliquidDeltaType.NO_CHANGE || deltaType == HyperliquidDeltaType.UNKNOWN) {
            return "DELTA_TYPE_NOT_COPYABLE";
        }
        return "PROFILE_NOT_ALLOWED_BY_STRATEGY";
    }

    @Override
    public boolean isSeparateShadowEnabled() {
        return separateShadowEnabled;
    }

    @Override
    public boolean isLivePromotable(UUID idUser, MetricaWalletDto candidate) {
        if (!separateShadowEnabled) {
            return true;
        }
        if (candidate == null || !copyStrategyRuntimeRouter.isLiveEligibleJoyasCandidate(candidate)) {
            return false;
        }
        if (summaryDecisionNotFinal(candidate)) {
            log.warn(
                    "event=metric_candidate_summary_not_final reasonCode=SUMMARY_NOT_FINAL_LIVE_BLOCKED walletId={} strategyCode={} shadowImpact=SHADOW_ALLOWED liveImpact=LIVE_BLOCKED reason={}",
                    walletId(candidate),
                    strategyCode(candidate),
                    summaryNotFinalReason(candidate)
            );
            return false;
        }
        if (simulationAuditFailed(candidate)) {
            log.warn(
                    "event=metric_candidate_simulation_audit_failed reasonCode=SIMULATION_AUDIT_FAILED walletId={} strategyCode={} shadowImpact=SHADOW_ALLOWED liveImpact=LIVE_BLOCKED errors={}",
                    walletId(candidate),
                    strategyCode(candidate),
                    simulationAuditErrors(candidate)
            );
            return false;
        }
        if (hasRealJewelDataWarning(candidate, "DATA_STALE_BLOCKS_LIVE")
                || hasRealJewelDataWarning(candidate, "DATA_STALE_BLOCKS_REAL_OPEN")) {
            log.warn(
                    "event=metric_candidate_stale_live_blocked reasonCode=DATA_STALE_BLOCKS_LIVE walletId={} strategyCode={} shadowImpact=SHADOW_ALLOWED liveImpact=LIVE_BLOCKED",
                    walletId(candidate),
                    strategyCode(candidate)
            );
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
        if (!shadowValidationDecision(idUser, candidate).passed()) {
            return false;
        }
        if (!requiredWindowsPositive(candidate)) {
            return false;
        }
        if (!slippageValidationPasses(candidate)) {
            return false;
        }
        return !"SHADOW_REJECTED".equals(shadowStatus(idUser, candidate));
    }

    @Override
    public boolean isMicroLivePromotable(UUID idUser, MetricaWalletDto candidate) {
        if (!separateShadowEnabled) {
            return true;
        }
        if (candidate == null || !copyStrategyRuntimeRouter.isLiveEligibleJoyasCandidate(candidate)) {
            return false;
        }
        if (summaryDecisionNotFinal(candidate)) {
            return false;
        }
        if (simulationAuditFailed(candidate)) {
            return false;
        }
        if (hasRealJewelDataWarning(candidate, "DATA_STALE_BLOCKS_REAL_OPEN")) {
            return false;
        }
        if (hasRealJewelHardBlockers(candidate)) {
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
        String riskClass = realJewelRiskClass(candidate);
        if ("D".equals(riskClass)) {
            return false;
        }
        MetricaWalletDto.RealJewelDto realJewel = candidate.getRealJewel();
        String recommendedMode = normalizeStatus(realJewel == null ? null : realJewel.getRecommendedExecutionMode());
        if ("MICRO_LIVE".equals(recommendedMode) || Boolean.TRUE.equals(realJewel == null ? null : realJewel.getCanMicroLive())) {
            return true;
        }
        if ("REDUCE_CAPITAL".equals(action)) {
            return true;
        }
        Double evidenceScore = realJewel == null ? null : realJewel.getEvidenceScore();
        return evidenceScore != null && evidenceScore >= microLiveEvidenceThreshold(riskClass);
    }

    private boolean recordShadowForAllocation(
            ShadowCopyAllocationEntity allocation,
            OperacionEvent event,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            long eventReceivedNs
    ) {
        CopyFlowTiming timing = eventReceivedNs > 0
                ? CopyFlowTiming.fromEventReceivedNs(eventReceivedNs)
                : CopyFlowTiming.start();
        if (eventReceivedNs > 0) {
            timing.add(CopyFlowTiming.Stage.QUEUE, eventReceivedNs);
        }
        SHADOW_FLOW_TIMING.set(timing);
        try {
        OperacionDto op = event.getOperacion();
        String originId = op.getIdOperacion().toString();
        String typeOperation = op.getTipoOperacion() == null ? "BOTH" : op.getTipoOperacion().name();
        OffsetDateTime eventTime = eventTime(op, action);
        BigDecimal qty = firstNonNull(op.getSizeQty(), op.getSize(), ZERO).abs();
        long priceNs = timing.mark();
        BigDecimal price = resolveShadowExecutionPrice(op, action, deltaType);
        timing.add(CopyFlowTiming.Stage.PRICE_RESOLUTION, priceNs);
        BigDecimal notional = resolveShadowNotional(op, qty, price);
        String eventType = shadowEventType(action, deltaType);

        long dedupeNs = timing.mark();
        shadowEventRepository.lockShadowEventIdempotency(shadowEventIdempotencyKey(allocation, originId, eventType, typeOperation, eventTime));
        boolean duplicate = allocation.getWalletProfileId() != null
                ? shadowEventRepository.existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
                allocation.getWalletProfileId(), originId, eventType, typeOperation, eventTime)
                : shadowEventRepository.existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
                allocation.getId(), originId, eventType, typeOperation, eventTime);
        timing.add(CopyFlowTiming.Stage.DEDUPE, dedupeNs);
        if (duplicate) {
            String duplicateReasonCode = deltaType == HyperliquidDeltaType.FLIP
                    ? "DUPLICATE_SHADOW_FLIP_EVENT"
                    : "DUPLICATE_SHADOW_EVENT";
            if (deltaType == HyperliquidDeltaType.FLIP) {
                log.info("event=shadow_flip_duplicate_ignored sourceEventId={} originId={} shadowAllocationId={} walletProfileId={} wallet={} copyProfileCode={} symbol={} side={} action={} deltaType={} reasonCode={} copyImpact=noop_idempotent",
                        originId, originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), typeOperation, action, deltaType, duplicateReasonCode);
            } else {
                log.info("event=duplicate_shadow_event_skipped originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} action={} deltaType={} reasonCode={} shadowImpact=NO_DUPLICATE_EVENT",
                        originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), typeOperation, action, deltaType, duplicateReasonCode);
            }
            logShadowFlowLatency(timing, "skipped", eventType, null, duplicateReasonCode, op, allocation, typeOperation, originId);
            return false;
        }

        ShadowPositionImpact impact;
        if (requiresValidShadowPrice(action, deltaType) && !isPositive(price)) {
            String reasonCode = missingPriceReason(action, deltaType);
            impact = ShadowPositionImpact.priceMissing(reasonCode);
            log.warn("event=shadow_price_missing reasonCode={} wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} usage={} copyImpact={} sourceEventId={}",
                    reasonCode,
                    allocation.getWalletId(),
                    allocation.getWalletProfileId(),
                    allocation.getId(),
                    allocation.getShadowValidationId(),
                    op.getParSymbol(),
                    shadowPriceUsage(action, deltaType),
                    copyImpactForMissingPrice(action, deltaType),
                    originId);
            if ("PRICE_CLOSE_MISSING".equals(reasonCode)) {
                log.warn("event=shadow_close_rejected_missing_price reasonCode=PRICE_CLOSE_MISSING wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} deltaType={} sourceEventId={} copyImpact=shadow_close_skipped",
                        allocation.getWalletId(),
                        allocation.getWalletProfileId(),
                        allocation.getId(),
                        allocation.getShadowValidationId(),
                        op.getParSymbol(),
                        typeOperation,
                        deltaType,
                        originId);
            }
        } else {
            log.debug("event=shadow_price_resolved source=OPERATION symbol={} price={} usage={}",
                    op.getParSymbol(), price, shadowPriceUsage(action, deltaType));
            impact = updateShadowPositionState(allocation, op, action, deltaType, eventTime, qty, notional, price, originId, typeOperation, eventType);
        }
        BigDecimal eventQtyExecuted = firstNonNull(impact.qtyExecuted(), qty);
        BigDecimal eventNotional = firstNonNull(impact.eventNotionalUsd(), notional);
        BigDecimal eventSlippageUsd = firstNonNull(impact.eventSlippageUsd(), ZERO);
        ShadowCopyOperationEntity shadowOperation = null;
        if (impact.positionUpdated()) {
            if (isClosedImpact(impact)) {
                shadowOperation = closeShadowOperation(allocation, op, eventTime, originId, typeOperation, price, impact.realizedPnlUsd());
            } else if (isReducedImpact(impact)) {
                shadowOperation = reduceShadowOperation(allocation, op, eventTime, originId, typeOperation, impact.resultingQty(), price, eventSlippageUsd, impact.realizedPnlUsd());
            } else {
                shadowOperation = upsertOpenShadowOperation(allocation, op, deltaType, eventTime, originId, typeOperation, impact.resultingQty(), notional, price, eventSlippageUsd);
            }
        }

        ShadowCopyOperationEventEntity shadowEvent = ShadowCopyOperationEventEntity.builder()
                .shadowOperationId(shadowOperation == null ? null : shadowOperation.getIdOperation())
                .shadowPositionId(impact.shadowPositionId())
                .shadowAllocationId(allocation.getId())
                .linkedLiveAllocationId(allocation.getLinkedLiveAllocationId())
                .walletProfileId(allocation.getWalletProfileId())
                .shadowValidationId(allocation.getShadowValidationId())
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
                .qtyExecuted(eventQtyExecuted)
                .price(price)
                .notionalUsd(eventNotional)
                .previousQty(impact.previousQty())
                .resultingQty(firstNonNull(impact.resultingQty(), action == CopyJobAction.CLOSE ? ZERO : qty))
                .realizedPnlUsd(impact.realizedPnlUsd())
                .feeUsd(ZERO)
                .slippageBps(BigDecimal.valueOf(shadowSlippageBps).setScale(6, RoundingMode.HALF_UP))
                .slippageUsd(eventSlippageUsd)
                .decision(impact.positionUpdated() ? "SIMULATED" : "SKIPPED")
                .decisionReason("shadow_separate_table")
                .source("shadow_copy")
                .reasonCode(impact.reasonCode())
                .eventTime(eventTime)
                .dateCreation(OffsetDateTime.now())
                .build();
        long dbNs = timing.mark();
        shadowEventRepository.save(shadowEvent);
        updateRuntimeActivity(allocation, eventType, impact, eventTime);
        refreshProfileValidationAfterEvent(allocation, eventType, impact, eventTime);
        timing.add(CopyFlowTiming.Stage.DB_PERSIST, dbNs);
        log.info("event=shadow_event_recorded originId={} userId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} shadowOperationId={} liveAllocationId={} walletId={} copyProfileCode={} executionMode=SHADOW intent={} symbol={} side={} reasonCode={} shadowImpact={} qty={} notional={} slippageUsd={}",
                originId,
                allocation.getIdUser(),
                allocation.getId(),
                allocation.getWalletProfileId(),
                impact.shadowPositionId(),
                shadowOperation == null ? null : shadowOperation.getIdOperation(),
                allocation.getLinkedLiveAllocationId(),
                allocation.getWalletId(),
                allocation.getCopyStrategyCode(),
                eventType,
                op.getParSymbol(),
                typeOperation,
                shadowEvent.getReasonCode(),
                impact.shadowImpact(),
                qty,
                eventNotional,
                eventSlippageUsd);
        logShadowFlowLatency(timing, impact.positionUpdated() ? "success" : "skipped", eventType, computedDeltaTypeForImpact(impact), impact.reasonCode(), op, allocation, typeOperation, originId);
        return true;
        } finally {
            SHADOW_FLOW_TIMING.remove();
        }
    }

    private String shadowEventIdempotencyKey(
            ShadowCopyAllocationEntity allocation,
            String originId,
            String eventType,
            String typeOperation,
            OffsetDateTime eventTime
    ) {
        String scope = allocation.getWalletProfileId() == null
                ? "allocation:" + allocation.getId()
                : "profile:" + allocation.getWalletProfileId();
        return String.join("|",
                "shadow-event",
                scope,
                originId == null ? "NA" : originId,
                eventType == null ? "NA" : eventType,
                typeOperation == null ? "NA" : typeOperation,
                eventTime == null ? "NA" : eventTime.toInstant().toString()
        );
    }

    private ShadowCopyOperationEntity upsertOpenShadowOperation(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            HyperliquidDeltaType deltaType,
            OffsetDateTime eventTime,
            String originId,
            String typeOperation,
            BigDecimal qty,
            BigDecimal notional,
            BigDecimal price,
            BigDecimal slippageUsd
    ) {
        BigDecimal resultingQty = firstNonNull(qty, ZERO).abs();
        BigDecimal resultingNotional = positiveOrFallback(resultingQty.multiply(firstNonNull(price, ZERO)), notional);
        boolean lifecycleStart = deltaType == HyperliquidDeltaType.OPEN
                || deltaType == HyperliquidDeltaType.FLIP
                || deltaType == HyperliquidDeltaType.UNKNOWN;
        ShadowCopyOperationEntity shadowOperation = findOpenShadowOperationByOrigin(allocation, originId, typeOperation)
                .or(() -> findOpenShadowOperationForPosition(allocation, op.getParSymbol(), typeOperation))
                .orElseGet(() -> ShadowCopyOperationEntity.builder()
                        .idOperation(UUID.randomUUID())
                        .shadowAllocationId(allocation.getId())
                        .linkedLiveAllocationId(allocation.getLinkedLiveAllocationId())
                        .walletProfileId(allocation.getWalletProfileId())
                        .shadowValidationId(allocation.getShadowValidationId())
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
                        .status(lifecycleStart ? "OPEN" : "OPEN_RECOVERED")
                        .build());
        BigDecimal previousQty = firstNonNull(shadowOperation.getSizePar(), ZERO).abs();
        BigDecimal previousEntryPrice = shadowOperation.getPriceEntry();
        shadowOperation.setSizePar(resultingQty);
        shadowOperation.setSizeUsd(resultingNotional);
        if (shadowOperation.getPriceEntry() == null || lifecycleStart) {
            shadowOperation.setPriceEntry(price);
        } else if (resultingQty.compareTo(previousQty) > 0) {
            shadowOperation.setPriceEntry(copyPositionAccountingService.weightedAverageEntryPrice(previousQty, previousEntryPrice, resultingQty.subtract(previousQty), price, resultingQty));
        }
        shadowOperation.setSimulatedSlippageUsd(firstNonNull(shadowOperation.getSimulatedSlippageUsd(), ZERO).add(slippageUsd));
        shadowOperation.setSimulatedFeeUsd(firstNonNull(shadowOperation.getSimulatedFeeUsd(), ZERO));
        shadowOperation.setActive(true);
        if (shadowOperation.getStatus() == null || shadowOperation.getStatus().isBlank()) {
            shadowOperation.setStatus("OPEN");
        }
        return shadowOperationRepository.save(shadowOperation);
    }

    private ShadowCopyOperationEntity reduceShadowOperation(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            OffsetDateTime eventTime,
            String originId,
            String typeOperation,
            BigDecimal resultingQty,
            BigDecimal price,
            BigDecimal slippageUsd,
            BigDecimal realizedPnlUsd
    ) {
        ShadowCopyOperationEntity shadowOperation = findOpenShadowOperationForPosition(allocation, op.getParSymbol(), typeOperation)
                .or(() -> findOpenShadowOperationByOrigin(allocation, originId, typeOperation))
                .orElse(null);
        if (shadowOperation == null) {
            log.info("event=shadow_reduce_operation_missing originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} reasonCode=SHADOW_OPERATION_NOT_FOUND shadowImpact=POSITION_REDUCED_WITHOUT_OPERATION",
                    originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), typeOperation);
            return null;
        }
        BigDecimal safeResultingQty = firstNonNull(resultingQty, ZERO).abs();
        shadowOperation.setSizePar(safeResultingQty);
        shadowOperation.setSizeUsd(positiveOrFallback(safeResultingQty.multiply(firstNonNull(price, ZERO)), ZERO));
        shadowOperation.setSimulatedSlippageUsd(firstNonNull(shadowOperation.getSimulatedSlippageUsd(), ZERO).add(firstNonNull(slippageUsd, ZERO)));
        shadowOperation.setSimulatedFeeUsd(firstNonNull(shadowOperation.getSimulatedFeeUsd(), ZERO));
        shadowOperation.setRealizedPnlUsd(firstNonNull(shadowOperation.getRealizedPnlUsd(), ZERO).add(firstNonNull(realizedPnlUsd, ZERO)));
        shadowOperation.setActive(true);
        shadowOperation.setStatus("OPEN");
        if (shadowOperation.getDateCreation() == null) {
            shadowOperation.setDateCreation(eventTime);
        }
        return shadowOperationRepository.save(shadowOperation);
    }

    private ShadowCopyOperationEntity closeShadowOperation(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            OffsetDateTime eventTime,
            String originId,
            String typeOperation,
            BigDecimal price,
            BigDecimal realizedPnlUsd
    ) {
        ShadowCopyOperationEntity shadowOperation = findOpenShadowOperationForPosition(allocation, op.getParSymbol(), typeOperation)
                .or(() -> findOpenShadowOperationByOrigin(allocation, originId, typeOperation))
                .orElse(null);
        if (shadowOperation == null) {
            log.info("event=shadow_operation_close_missing originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} reasonCode=SHADOW_OPERATION_NOT_FOUND shadowImpact=POSITION_CLOSED_WITHOUT_OPERATION",
                    originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), typeOperation);
            return null;
        }
        if (!isPositive(price)) {
            log.warn("event=shadow_close_rejected_missing_price reasonCode=PRICE_CLOSE_MISSING wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} deltaType=CLOSE sourceEventId={} copyImpact=shadow_close_skipped",
                    allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), op.getParSymbol(), typeOperation, originId);
            return null;
        }
        shadowOperation.setActive(false);
        shadowOperation.setStatus("CLOSED");
        shadowOperation.setDateClose(eventTime);
        shadowOperation.setPriceClose(price);
        shadowOperation.setRealizedPnlUsd(firstNonNull(shadowOperation.getRealizedPnlUsd(), ZERO).add(firstNonNull(realizedPnlUsd, ZERO)));
        return shadowOperationRepository.save(shadowOperation);
    }

    private Optional<ShadowCopyOperationEntity> findOpenShadowOperationByOrigin(
            ShadowCopyAllocationEntity allocation,
            String originId,
            String typeOperation
    ) {
        Optional<ShadowCopyOperationEntity> byProfile = allocation.getWalletProfileId() == null
                ? Optional.empty()
                : shadowOperationRepository.findFirstByWalletProfileIdAndIdOrderOriginAndTypeOperationAndActiveTrue(allocation.getWalletProfileId(), originId, typeOperation);
        return byProfile.or(() -> shadowOperationRepository.findFirstByShadowAllocationIdAndIdOrderOriginAndTypeOperationAndActiveTrue(allocation.getId(), originId, typeOperation));
    }

    private Optional<ShadowCopyOperationEntity> findOpenShadowOperationForPosition(
            ShadowCopyAllocationEntity allocation,
            String symbol,
            String typeOperation
    ) {
        Optional<ShadowCopyOperationEntity> byProfile = allocation.getWalletProfileId() == null
                ? Optional.empty()
                : shadowOperationRepository.findFirstByWalletProfileIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc(
                allocation.getWalletProfileId(),
                symbol,
                typeOperation
        );
        return byProfile.or(() -> shadowOperationRepository.findFirstByShadowAllocationIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc(
                allocation.getId(),
                symbol,
                typeOperation
        ));
    }

    private Optional<ShadowPositionStateEntity> latestClosedShadowPosition(
            ShadowCopyAllocationEntity allocation,
            String symbol,
            String positionSide
    ) {
        Optional<ShadowPositionStateEntity> byProfile = allocation.getWalletProfileId() == null
                ? Optional.empty()
                : shadowPositionStateRepository.findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatusOrderByClosedAtDesc(
                allocation.getWalletProfileId(),
                symbol,
                positionSide,
                "CLOSED"
        );
        return byProfile.or(() -> shadowPositionStateRepository.findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatusOrderByClosedAtDesc(
                allocation.getId(),
                symbol,
                positionSide,
                "CLOSED"
        ));
    }

    private static boolean isClosedImpact(ShadowPositionImpact impact) {
        return impact != null && "SHADOW_POSITION_CLOSED".equals(impact.reasonCode());
    }

    private static boolean isReducedImpact(ShadowPositionImpact impact) {
        return impact != null && "SHADOW_POSITION_REDUCED".equals(impact.reasonCode());
    }

    private BigDecimal resolveAvgEntryPrice(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            String originId,
            String positionSide,
            ShadowPositionStateEntity state
    ) {
        BigDecimal stateEntry = state == null ? null : firstPositive(state.getEntryPrice());
        if (isPositive(stateEntry)) {
            return stateEntry;
        }
        if (allocation == null || op == null) {
            return null;
        }
        return findOpenShadowOperationForPosition(allocation, op.getParSymbol(), positionSide)
                .or(() -> findOpenShadowOperationByOrigin(allocation, originId, positionSide))
                .map(ShadowCopyOperationEntity::getPriceEntry)
                .filter(ShadowCopyTradingServiceImpl::isPositive)
                .orElse(null);
    }

    private boolean isWarmupEvent(ShadowCopyAllocationEntity allocation, OffsetDateTime eventTime) {
        if (allocation == null || eventTime == null || shadowWarmupMinutes <= 0) {
            return false;
        }
        OffsetDateTime startedAt = firstNonNull(allocation.getCreatedAt(), allocation.getLastSeenAt());
        return startedAt != null && !eventTime.isAfter(startedAt.plusMinutes(shadowWarmupMinutes));
    }

    private void updateRuntimeActivity(
            ShadowCopyAllocationEntity allocation,
            String eventType,
            ShadowPositionImpact impact,
            OffsetDateTime eventTime
    ) {
        if (allocation == null) {
            return;
        }
        OffsetDateTime effectiveTime = eventTime == null ? OffsetDateTime.now(ZoneOffset.UTC) : eventTime;
        allocation.setWalletLastActivityAt(maxTime(allocation.getWalletLastActivityAt(), effectiveTime));
        allocation.setStrategyLastActivityAt(maxTime(allocation.getStrategyLastActivityAt(), effectiveTime));
        if (impact != null && impact.positionUpdated()) {
            if ("OPEN".equals(eventType) || "FLIP".equals(eventType)) {
                allocation.setWalletLastOpenedAt(maxTime(allocation.getWalletLastOpenedAt(), effectiveTime));
                allocation.setStrategyLastOpenedAt(maxTime(allocation.getStrategyLastOpenedAt(), effectiveTime));
            }
            if ("CLOSE".equals(eventType) || isClosedImpact(impact)) {
                allocation.setWalletLastClosedAt(maxTime(allocation.getWalletLastClosedAt(), effectiveTime));
                allocation.setStrategyLastClosedAt(maxTime(allocation.getStrategyLastClosedAt(), effectiveTime));
            }
        }
        allocation.setLastSeenAt(maxTime(allocation.getLastSeenAt(), effectiveTime));
        allocation.setUpdatedAt(OffsetDateTime.now(ZoneOffset.UTC));
        shadowAllocationRepository.save(allocation);

        if (allocation.getWalletProfileId() == null) {
            return;
        }
        copyWalletProfileRepository.findById(allocation.getWalletProfileId()).ifPresent(profile -> {
            profile.setWalletLastActivityAt(maxTime(profile.getWalletLastActivityAt(), effectiveTime));
            profile.setStrategyLastActivityAt(maxTime(profile.getStrategyLastActivityAt(), effectiveTime));
            if (impact != null && impact.positionUpdated()) {
                if ("OPEN".equals(eventType) || "FLIP".equals(eventType)) {
                    profile.setWalletLastOpenedAt(maxTime(profile.getWalletLastOpenedAt(), effectiveTime));
                    profile.setStrategyLastOpenedAt(maxTime(profile.getStrategyLastOpenedAt(), effectiveTime));
                }
                if ("CLOSE".equals(eventType) || isClosedImpact(impact)) {
                    profile.setWalletLastClosedAt(maxTime(profile.getWalletLastClosedAt(), effectiveTime));
                    profile.setStrategyLastClosedAt(maxTime(profile.getStrategyLastClosedAt(), effectiveTime));
                }
            }
            profile.setLastSeenAt(maxTime(profile.getLastSeenAt(), effectiveTime));
            profile.setUpdatedAt(OffsetDateTime.now(ZoneOffset.UTC));
            copyWalletProfileRepository.save(profile);
        });
    }

    private void refreshProfileValidationAfterEvent(
            ShadowCopyAllocationEntity allocation,
            String eventType,
            ShadowPositionImpact impact,
            OffsetDateTime eventTime
    ) {
        if (allocation == null || allocation.getWalletProfileId() == null) {
            return;
        }
        if (!shouldRefreshProfileValidation(eventType, impact)) {
            return;
        }
        String status = profileStatus(allocation.getStatus(), false);
        String reason = "shadow_runtime_event:" + (impact == null ? "UNKNOWN" : impact.reasonCode());
        upsertProfileValidation(allocation.getWalletProfileId(), status, reason, eventTime == null ? OffsetDateTime.now(ZoneOffset.UTC) : eventTime);
    }

    private boolean shouldRefreshProfileValidation(String eventType, ShadowPositionImpact impact) {
        if (impact == null) {
            return false;
        }
        return "OPEN".equals(eventType)
                || "CLOSE".equals(eventType)
                || "FLIP".equals(eventType)
                || "INCREASE".equals(eventType)
                || "UPDATE".equals(eventType)
                || "SHADOW_POSITION_CLOSED".equals(eventType);
    }

    private static OffsetDateTime maxTime(OffsetDateTime current, OffsetDateTime candidate) {
        if (current == null) {
            return candidate;
        }
        if (candidate == null) {
            return current;
        }
        return candidate.isAfter(current) ? candidate : current;
    }

    private ShadowPositionImpact updateShadowPositionState(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            CopyJobAction action,
            HyperliquidDeltaType deltaType,
            OffsetDateTime eventTime,
            BigDecimal qty,
            BigDecimal notional,
            BigDecimal price,
            String originId,
            String positionSide,
            String originalEventType
    ) {
        boolean closedPreviousSideByFlip = false;
        if (deltaType == HyperliquidDeltaType.FLIP && action != CopyJobAction.CLOSE) {
            closedPreviousSideByFlip = closeOtherOpenShadowSides(allocation, op, eventTime, originId, positionSide);
        }

        Optional<ShadowPositionStateEntity> existingState = allocation.getWalletProfileId() == null
                ? Optional.empty()
                : shadowPositionStateRepository.findFirstByWalletProfileIdAndParsymbolAndPositionSideAndStatus(
                allocation.getWalletProfileId(),
                op.getParSymbol(),
                positionSide,
                "OPEN"
        );
        ShadowPositionStateEntity state = existingState
                .or(() -> shadowPositionStateRepository.findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatus(
                        allocation.getId(),
                        op.getParSymbol(),
                        positionSide,
                        "OPEN"
                ))
                .orElse(null);
        if (deltaType == HyperliquidDeltaType.FLIP
                && action != CopyJobAction.CLOSE
                && !closedPreviousSideByFlip
                && state == null) {
            if (isWarmupEvent(allocation, eventTime)) {
                log.info("event=shadow_warmup_flip_without_open originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} warmupMinutes={} reasonCode=WARMUP_FLIP_WITHOUT_SHADOW_OPEN shadowImpact=WARMUP_NO_VALID_POSITION",
                        originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, shadowWarmupMinutes);
                return ShadowPositionImpact.warmupFlipWithoutOpen();
            }
            log.info("event=shadow_flip_without_open originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} reasonCode=FLIP_WITHOUT_SHADOW_OPEN shadowImpact=NO_VALID_POSITION",
                    originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide);
            return ShadowPositionImpact.flipWithoutOpen();
        }
        if (action == CopyJobAction.CLOSE) {
            if (state == null) {
                if (isWarmupEvent(allocation, eventTime)) {
                    log.info("event=shadow_warmup_close_without_open originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} warmupMinutes={} reasonCode=WARMUP_CLOSE_WITHOUT_SHADOW_OPEN shadowImpact=WARMUP_NO_VALID_PNL",
                            originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, shadowWarmupMinutes);
                    return ShadowPositionImpact.warmupCloseWithoutOpen();
                }
                log.info("event=shadow_close_without_open originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} reasonCode=CLOSE_WITHOUT_SHADOW_OPEN shadowImpact=NO_VALID_PNL",
                        originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide);
                return ShadowPositionImpact.closeWithoutOpen();
            }
            BigDecimal previousQty = firstNonNull(state.getQty(), qty, ZERO).abs();
            BigDecimal closeNotional = positiveOrFallback(previousQty.multiply(price), notional);
            BigDecimal closeSlippageUsd = slippageUsd(closeNotional);
            BigDecimal avgEntryPrice = resolveAvgEntryPrice(allocation, op, originId, positionSide, state);
            CopyAccountingResult accounting = applyShadowAccounting(op.getParSymbol(), positionSide, previousQty, ZERO, avgEntryPrice, price, ZERO, closeSlippageUsd, eventTime, "SHADOW_POSITION_CLOSED");
            if (!accounting.accepted()) {
                log.warn("event=shadow_close_rejected_missing_entry_price reasonCode=ENTRY_PRICE_MISSING wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} previousQty={} resultingQty=0 copyImpact=shadow_close_skipped",
                        allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), op.getParSymbol(), positionSide, previousQty);
                return ShadowPositionImpact.entryPriceMissing(state.getId())
                        .withAccounting(previousQty, previousQty, ZERO, ZERO, ZERO);
            }
            BigDecimal grossRealizedPnl = accounting.grossRealizedPnlUsd();
            BigDecimal netRealizedPnl = accounting.netRealizedPnlUsd();
            BigDecimal totalSlippage = firstNonNull(state.getSlippageUsd(), ZERO).add(closeSlippageUsd);
            BigDecimal totalRealizedPnl = firstNonNull(state.getRealizedPnlUsd(), ZERO).add(netRealizedPnl).setScale(12, RoundingMode.HALF_UP);
            state.setStatus("CLOSED");
            state.setClosedAt(eventTime);
            state.setQty(ZERO);
            state.setEntryPrice(accounting.newAvgEntryPrice());
            state.setNotionalUsd(ZERO);
            state.setMarkPrice(price);
            state.setSlippageUsd(totalSlippage);
            state.setRealizedPnlUsd(totalRealizedPnl);
            state.setLastSourceEventId(originId);
            shadowPositionStateRepository.save(state);
            log.info("event=shadow_realized_pnl_calculated reasonCode=SHADOW_POSITION_CLOSED symbol={} side={} qtyClosed={} avgEntryPrice={} executionPrice={} formula={} grossRealizedPnlUsd={} netRealizedPnlUsd={}",
                    op.getParSymbol(), positionSide, accounting.deltaClosedQty(), accounting.newAvgEntryPrice(), price, pnlFormula(positionSide), grossRealizedPnl, netRealizedPnl);
            log.info("event=shadow_position_closed originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} realizedPnlUsd={} reasonCode=SHADOW_POSITION_CLOSED shadowImpact=POSITION_CLOSED",
                    originId, allocation.getId(), allocation.getWalletProfileId(), state.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, netRealizedPnl);
            return ShadowPositionImpact.closed(netRealizedPnl, state.getId())
                    .withAccounting(previousQty, ZERO, previousQty, closeNotional, closeSlippageUsd);
        }
        if (state == null && (deltaType == HyperliquidDeltaType.RESIZE || deltaType == HyperliquidDeltaType.UPDATE)) {
            Optional<ShadowPositionStateEntity> latestClosed = latestClosedShadowPosition(allocation, op.getParSymbol(), positionSide);
            if (latestClosed.isPresent()) {
                log.info("event=shadow_resize_after_closed originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} deltaType={} reasonCode=RESIZE_AFTER_SHADOW_CLOSED shadowImpact=NO_VALID_POSITION",
                        originId, allocation.getId(), allocation.getWalletProfileId(), latestClosed.get().getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, deltaType);
                return ShadowPositionImpact.resizeAfterClosed(latestClosed.get().getId());
            }
            if (isWarmupEvent(allocation, eventTime)) {
                log.info("event=shadow_warmup_resize_without_open originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} deltaType={} warmupMinutes={} reasonCode=WARMUP_RESIZE_WITHOUT_SHADOW_OPEN shadowImpact=WARMUP_NO_VALID_POSITION",
                        originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, deltaType, shadowWarmupMinutes);
                return ShadowPositionImpact.warmupResizeWithoutOpen();
            }
            log.info("event=shadow_resize_without_open originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} deltaType={} reasonCode=RESIZE_WITHOUT_SHADOW_OPEN shadowImpact=NO_VALID_POSITION",
                    originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, deltaType);
            return ShadowPositionImpact.resizeWithoutOpen();
        }
        if (state == null && blocksNewShadowExposure(allocation)) {
            log.info("event=shadow_new_exposure_blocked_by_ranking_exit originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} deltaType={} status={} reasonCode=SHADOW_NEW_EXPOSURE_BLOCKED_BY_RANKING_EXIT allowNewEntries=false allowReductions=true allowCloses=true",
                    originId,
                    allocation.getId(),
                    allocation.getWalletProfileId(),
                    allocation.getWalletId(),
                    allocation.getCopyStrategyCode(),
                    op.getParSymbol(),
                    positionSide,
                    deltaType,
                    allocation.getStatus());
            return ShadowPositionImpact.newExposureBlockedByRankingExit(null)
                    .withAccounting(ZERO, ZERO, ZERO, ZERO, ZERO);
        }
        boolean opened = false;
        if (state == null) {
            opened = true;
            state = ShadowPositionStateEntity.builder()
                    .id(UUID.randomUUID())
                    .shadowAllocationId(allocation.getId())
                    .linkedLiveAllocationId(allocation.getLinkedLiveAllocationId())
                    .walletProfileId(allocation.getWalletProfileId())
                    .shadowValidationId(allocation.getShadowValidationId())
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
        BigDecimal previousQty = firstNonNull(state.getQty(), ZERO).abs();
        BigDecimal resultingQty = qty.abs();
        CopyFlowTiming timing = SHADOW_FLOW_TIMING.get();
        long classificationNs = timing == null ? 0L : timing.mark();
        PositionDeltaClassification classification = classifyShadowPosition(
                originalEventType,
                deltaType,
                positionSide,
                previousQty,
                resultingQty,
                price,
                state.getEntryPrice(),
                op,
                allocation
        );
        logClassificationCorrectionIfNeeded(classification, originalEventType, deltaType, positionSide, previousQty, resultingQty, price, op, allocation, originId);
        if (timing != null) {
            timing.add(CopyFlowTiming.Stage.CLASSIFICATION, classificationNs);
        }
        PositionDeltaType computedDeltaType = classification.computedDeltaType();
        if (blocksNewShadowExposure(allocation)
                && (computedDeltaType == PositionDeltaType.OPEN || computedDeltaType == PositionDeltaType.INCREASE)) {
            log.info("event=shadow_new_exposure_blocked_by_ranking_exit originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} previousQty={} resultingQty={} computedDeltaType={} status={} reasonCode=SHADOW_NEW_EXPOSURE_BLOCKED_BY_RANKING_EXIT allowNewEntries=false allowReductions=true allowCloses=true",
                    originId,
                    allocation.getId(),
                    allocation.getWalletProfileId(),
                    state.getId(),
                    allocation.getWalletId(),
                    allocation.getCopyStrategyCode(),
                    op.getParSymbol(),
                    positionSide,
                    previousQty,
                    resultingQty,
                    computedDeltaType,
                    allocation.getStatus());
            return ShadowPositionImpact.newExposureBlockedByRankingExit(state.getId())
                    .withAccounting(previousQty, previousQty, ZERO, ZERO, ZERO);
        }
        boolean noopByMath = !opened && (computedDeltaType == PositionDeltaType.NOOP || computedDeltaType == PositionDeltaType.SNAPSHOT_NOOP);
        if (noopByMath) {
            state.setMarkPrice(price);
            state.setNotionalUsd(positiveOrFallback(resultingQty.multiply(price), state.getNotionalUsd()));
            state.setLastSourceEventId(originId);
            shadowPositionStateRepository.save(state);
            log.info("event=shadow_resize_noop originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} previousQty={} resultingQty={} reasonCode=SHADOW_RESIZE_NOOP shadowImpact=NOOP",
                    originId, allocation.getId(), allocation.getWalletProfileId(), state.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, previousQty, resultingQty);
            return ShadowPositionImpact.resizeNoop(state.getId())
                    .withAccounting(previousQty, resultingQty, ZERO, ZERO, ZERO);
        }
        if (!opened && (computedDeltaType == PositionDeltaType.REDUCE || computedDeltaType == PositionDeltaType.CLOSE_FULL)) {
            BigDecimal avgEntryPrice = resolveAvgEntryPrice(allocation, op, originId, positionSide, state);
            BigDecimal deltaClosedQty = previousQty.subtract(resultingQty).abs();
            BigDecimal eventNotional = positiveOrFallback(deltaClosedQty.multiply(price), ZERO);
            BigDecimal eventSlippageUsd = slippageUsd(eventNotional);
            CopyAccountingResult accounting = applyShadowAccounting(
                    op.getParSymbol(),
                    positionSide,
                    previousQty,
                    resultingQty,
                    avgEntryPrice,
                    price,
                    ZERO,
                    eventSlippageUsd,
                    eventTime,
                    computedDeltaType == PositionDeltaType.CLOSE_FULL ? "SHADOW_POSITION_CLOSED" : "SHADOW_POSITION_REDUCED"
            );
            if (!accounting.accepted()) {
                log.warn("event=shadow_reduce_rejected_missing_entry_price reasonCode=ENTRY_PRICE_MISSING wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} previousQty={} resultingQty={} copyImpact=shadow_reduce_skipped",
                        allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), op.getParSymbol(), positionSide, previousQty, resultingQty);
                return ShadowPositionImpact.entryPriceMissing(state.getId())
                        .withAccounting(previousQty, resultingQty, ZERO, ZERO, ZERO);
            }
            BigDecimal grossRealizedPnl = accounting.grossRealizedPnlUsd();
            BigDecimal netRealizedPnl = accounting.netRealizedPnlUsd();
            BigDecimal totalSlippage = firstNonNull(state.getSlippageUsd(), ZERO).add(eventSlippageUsd);
            BigDecimal totalRealizedPnl = firstNonNull(state.getRealizedPnlUsd(), ZERO).add(netRealizedPnl).setScale(12, RoundingMode.HALF_UP);
            if (computedDeltaType == PositionDeltaType.CLOSE_FULL) {
                state.setStatus("CLOSED");
                state.setClosedAt(eventTime);
                state.setQty(ZERO);
                state.setEntryPrice(accounting.newAvgEntryPrice());
                state.setNotionalUsd(ZERO);
                state.setMarkPrice(price);
                state.setSlippageUsd(totalSlippage);
                state.setRealizedPnlUsd(totalRealizedPnl);
                state.setLastSourceEventId(originId);
                shadowPositionStateRepository.save(state);
                log.info("event=shadow_realized_pnl_calculated reasonCode=SHADOW_POSITION_CLOSED symbol={} side={} qtyClosed={} avgEntryPrice={} executionPrice={} formula={} grossRealizedPnlUsd={} netRealizedPnlUsd={}",
                        op.getParSymbol(), positionSide, accounting.deltaClosedQty(), accounting.newAvgEntryPrice(), price, pnlFormula(positionSide), grossRealizedPnl, netRealizedPnl);
                log.info("event=shadow_position_closed originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} realizedPnlUsd={} reasonCode=SHADOW_POSITION_CLOSED shadowImpact=POSITION_CLOSED source=resize_to_zero",
                        originId, allocation.getId(), allocation.getWalletProfileId(), state.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, netRealizedPnl);
                return ShadowPositionImpact.closed(netRealizedPnl, state.getId())
                        .withAccounting(previousQty, ZERO, deltaClosedQty, eventNotional, eventSlippageUsd);
            }
            state.setQty(resultingQty);
            state.setEntryPrice(accounting.newAvgEntryPrice());
            state.setMarkPrice(price);
            state.setNotionalUsd(positiveOrFallback(resultingQty.multiply(price), ZERO));
            state.setSlippageUsd(totalSlippage);
            state.setRealizedPnlUsd(totalRealizedPnl);
            state.setStatus("OPEN");
            state.setLastSourceEventId(originId);
            shadowPositionStateRepository.save(state);
            log.info("event=shadow_realized_pnl_calculated reasonCode=SHADOW_POSITION_REDUCED symbol={} side={} qtyClosed={} avgEntryPrice={} executionPrice={} formula={} grossRealizedPnlUsd={} netRealizedPnlUsd={}",
                    op.getParSymbol(), positionSide, accounting.deltaClosedQty(), accounting.newAvgEntryPrice(), price, pnlFormula(positionSide), grossRealizedPnl, netRealizedPnl);
            log.info("event=shadow_position_reduced reasonCode=SHADOW_POSITION_REDUCED wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} previousQty={} resultingQty={} deltaClosedQty={} avgEntryPrice={} executionPrice={} grossRealizedPnlUsd={} feeUsd={} slippageUsd={} netRealizedPnlUsd={}",
                    allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), op.getParSymbol(), positionSide,
                    previousQty, resultingQty, accounting.deltaClosedQty(), accounting.newAvgEntryPrice(), price, grossRealizedPnl, accounting.feeUsd(), accounting.slippageUsd(), netRealizedPnl);
            return ShadowPositionImpact.reduced(netRealizedPnl, state.getId())
                    .withAccounting(previousQty, resultingQty, deltaClosedQty, eventNotional, eventSlippageUsd);
        }
        boolean increaseByMath = !opened && computedDeltaType == PositionDeltaType.INCREASE;
        BigDecimal eventQtyExecuted = increaseByMath ? resultingQty.subtract(previousQty).abs() : resultingQty;
        BigDecimal eventNotional = increaseByMath ? eventQtyExecuted.multiply(price).abs() : notional;
        eventNotional = positiveOrFallback(eventNotional, increaseByMath ? ZERO : notional);
        BigDecimal eventSlippageUsd = slippageUsd(eventNotional);
        CopyAccountingResult accounting = applyShadowAccounting(
                op.getParSymbol(),
                positionSide,
                previousQty,
                resultingQty,
                state.getEntryPrice(),
                price,
                ZERO,
                eventSlippageUsd,
                eventTime,
                opened
                        ? (deltaType == HyperliquidDeltaType.FLIP ? "SHADOW_POSITION_OPENED_BY_FLIP" : "SHADOW_POSITION_OPENED")
                        : "SHADOW_POSITION_RESIZED"
        );
        if (accounting.accepted()) {
            eventQtyExecuted = accounting.deltaType() == PositionDeltaType.OPEN || accounting.deltaType() == PositionDeltaType.INCREASE
                    ? accounting.deltaAddedQty()
                    : eventQtyExecuted;
            eventNotional = accounting.deltaNotionalUsd();
        }
        BigDecimal entryPrice = accounting.accepted()
                ? accounting.newAvgEntryPrice()
                : firstPositive(state.getEntryPrice(), price);

        state.setQty(resultingQty);
        state.setEntryPrice(entryPrice);
        state.setMarkPrice(price);
        state.setNotionalUsd(positiveOrFallback(resultingQty.multiply(price), notional));
        state.setSlippageUsd(firstNonNull(state.getSlippageUsd(), ZERO).add(eventSlippageUsd));
        state.setLastSourceEventId(originId);
        shadowPositionStateRepository.save(state);
        if (opened) {
            String reasonCode = deltaType == HyperliquidDeltaType.FLIP
                    ? "SHADOW_POSITION_OPENED_BY_FLIP"
                    : "SHADOW_POSITION_OPENED";
            String logEvent = deltaType == HyperliquidDeltaType.FLIP
                    ? "shadow_position_opened_by_flip"
                    : "shadow_position_opened";
            log.info("event={} originId={} shadowAllocationId={} walletProfileId={} shadowValidationId={} shadowPositionId={} wallet={} copyProfileCode={} symbol={} side={} qty={} price={} reasonCode={} copyImpact={}",
                    logEvent, originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getShadowValidationId(), state.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, qty, price, reasonCode, deltaType == HyperliquidDeltaType.FLIP ? "shadow_position_opened" : "POSITION_OPENED");
            return deltaType == HyperliquidDeltaType.FLIP
                    ? ShadowPositionImpact.openedByFlip(state.getId()).withAccounting(ZERO, resultingQty, eventQtyExecuted, eventNotional, eventSlippageUsd)
                    : ShadowPositionImpact.opened(state.getId()).withAccounting(ZERO, resultingQty, eventQtyExecuted, eventNotional, eventSlippageUsd);
        }
        if (increaseByMath) {
            log.info("event=shadow_resize_delta_cost_applied reasonCode=SHADOW_POSITION_RESIZED wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} previousQty={} resultingQty={} deltaQty={} executionPrice={} deltaNotional={} feeUsd={} slippageUsd={} costBasis=DELTA_NOTIONAL",
                    allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), op.getParSymbol(), positionSide,
                    previousQty, resultingQty, eventQtyExecuted, price, eventNotional, ZERO, eventSlippageUsd);
        }
        log.info("event=shadow_position_resized originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} qty={} price={} reasonCode=SHADOW_POSITION_RESIZED shadowImpact=POSITION_RESIZED",
                originId, allocation.getId(), allocation.getWalletProfileId(), state.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, qty, price);
        return ShadowPositionImpact.resized(state.getId())
                .withAccounting(previousQty, resultingQty, eventQtyExecuted, eventNotional, eventSlippageUsd);
    }

    private boolean closeOtherOpenShadowSides(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            OffsetDateTime eventTime,
            String originId,
            String newPositionSide
    ) {
        List<ShadowPositionStateEntity> openStates = allocation.getWalletProfileId() == null
                ? shadowPositionStateRepository.findAllByShadowAllocationIdAndParsymbolAndStatus(allocation.getId(), op.getParSymbol(), "OPEN")
                : shadowPositionStateRepository.findAllByWalletProfileIdAndParsymbolAndStatus(allocation.getWalletProfileId(), op.getParSymbol(), "OPEN");
        boolean closedAny = false;
        for (ShadowPositionStateEntity state : openStates) {
            if (state == null || Objects.equals(state.getPositionSide(), newPositionSide)) {
                continue;
            }
            BigDecimal closePrice = resolveShadowExecutionPrice(op, CopyJobAction.OPEN, HyperliquidDeltaType.FLIP);
            if (!isPositive(closePrice)) {
                log.warn("event=shadow_close_rejected_missing_price reasonCode=PRICE_CLOSE_MISSING wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} deltaType=FLIP sourceEventId={} copyImpact=shadow_close_skipped",
                        allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), op.getParSymbol(), state.getPositionSide(), originId);
                continue;
            }
            closedAny = true;
            BigDecimal previousQty = firstNonNull(state.getQty(), ZERO).abs();
            BigDecimal previousNotional = positiveOrFallback(previousQty.multiply(closePrice), firstNonNull(state.getNotionalUsd(), op.getNotionalUsd(), op.getMarginUsedUsd(), ZERO).abs());
            BigDecimal closeSlippageUsd = slippageUsd(previousNotional);
            BigDecimal avgEntryPrice = resolveAvgEntryPrice(allocation, op, originId, state.getPositionSide(), state);
            CopyAccountingResult accounting = applyShadowAccounting(op.getParSymbol(), state.getPositionSide(), previousQty, ZERO, avgEntryPrice, closePrice, ZERO, closeSlippageUsd, eventTime, "SHADOW_POSITION_CLOSED_BY_FLIP");
            if (!accounting.accepted()) {
                log.warn("event=shadow_close_rejected_missing_entry_price reasonCode=ENTRY_PRICE_MISSING wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} previousQty={} resultingQty=0 copyImpact=shadow_flip_close_skipped",
                        allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), op.getParSymbol(), state.getPositionSide(), previousQty);
                continue;
            }
            BigDecimal grossRealizedPnl = accounting.grossRealizedPnlUsd();
            BigDecimal realizedPnl = accounting.netRealizedPnlUsd();
            BigDecimal totalSlippage = firstNonNull(state.getSlippageUsd(), ZERO).add(closeSlippageUsd);
            BigDecimal totalRealizedPnl = firstNonNull(state.getRealizedPnlUsd(), ZERO).add(realizedPnl).setScale(12, RoundingMode.HALF_UP);
            state.setStatus("CLOSED");
            state.setClosedAt(eventTime);
            state.setQty(ZERO);
            state.setEntryPrice(accounting.newAvgEntryPrice());
            state.setMarkPrice(closePrice);
            state.setNotionalUsd(ZERO);
            state.setSlippageUsd(totalSlippage);
            state.setRealizedPnlUsd(totalRealizedPnl);
            state.setLastSourceEventId(originId);
            shadowPositionStateRepository.save(state);
            log.info("event=shadow_realized_pnl_calculated reasonCode=SHADOW_POSITION_CLOSED_BY_FLIP symbol={} side={} qtyClosed={} avgEntryPrice={} executionPrice={} formula={} grossRealizedPnlUsd={} netRealizedPnlUsd={}",
                    op.getParSymbol(), state.getPositionSide(), accounting.deltaClosedQty(), accounting.newAvgEntryPrice(), closePrice, pnlFormula(state.getPositionSide()), grossRealizedPnl, realizedPnl);
            ShadowCopyOperationEntity closedOperation = closeShadowOperationForPosition(
                    allocation,
                    op.getParSymbol(),
                    state.getPositionSide(),
                    eventTime,
                    originId,
                    closePrice,
                    realizedPnl
            );
            saveFlipClosedPreviousSideEvent(
                    allocation,
                    op,
                    state,
                    closedOperation,
                    eventTime,
                    originId,
                    closePrice,
                    previousQty,
                    previousNotional,
                    realizedPnl,
                    closeSlippageUsd
            );
            ShadowPositionImpact closedImpact = ShadowPositionImpact.closed(realizedPnl, state.getId());
            updateRuntimeActivity(allocation, "CLOSE", closedImpact, eventTime);
            refreshProfileValidationAfterEvent(allocation, "SHADOW_POSITION_CLOSED", closedImpact, eventTime);
            log.info("event=shadow_operation_closed_by_flip reasonCode=SHADOW_POSITION_CLOSED_BY_FLIP wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} previousSide={} newSide={} shadowOperationId={} shadowPositionId={} realizedPnlUsd={} copyImpact=shadow_operation_reconciled originId={}",
                    allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), op.getParSymbol(), state.getPositionSide(), newPositionSide, closedOperation == null ? null : closedOperation.getIdOperation(), state.getId(), realizedPnl, originId);
        }
        return closedAny;
    }

    private ShadowCopyOperationEntity closeShadowOperationForPosition(
            ShadowCopyAllocationEntity allocation,
            String symbol,
            String typeOperation,
            OffsetDateTime eventTime,
            String originId,
            BigDecimal price,
            BigDecimal realizedPnlUsd
    ) {
        ShadowCopyOperationEntity shadowOperation = findOpenShadowOperationForPosition(allocation, symbol, typeOperation)
                .or(() -> findOpenShadowOperationByOrigin(allocation, originId, typeOperation))
                .orElse(null);
        if (shadowOperation == null) {
            log.info("event=shadow_flip_operation_close_missing originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} reasonCode=SHADOW_OPERATION_NOT_FOUND_BY_FLIP shadowImpact=POSITION_CLOSED_WITHOUT_OPERATION",
                    originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), symbol, typeOperation);
            return null;
        }
        if (!isPositive(price)) {
            log.warn("event=shadow_close_rejected_missing_price reasonCode=PRICE_CLOSE_MISSING wallet={} walletProfileId={} allocationId={} shadowValidationId={} symbol={} side={} deltaType=FLIP sourceEventId={} copyImpact=shadow_close_skipped",
                    allocation.getWalletId(), allocation.getWalletProfileId(), allocation.getId(), allocation.getShadowValidationId(), symbol, typeOperation, originId);
            return null;
        }
        shadowOperation.setActive(false);
        shadowOperation.setStatus("CLOSED");
        shadowOperation.setDateClose(eventTime);
        shadowOperation.setPriceClose(price);
        shadowOperation.setRealizedPnlUsd(firstNonNull(shadowOperation.getRealizedPnlUsd(), ZERO).add(firstNonNull(realizedPnlUsd, ZERO)));
        return shadowOperationRepository.save(shadowOperation);
    }

    private void saveFlipClosedPreviousSideEvent(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            ShadowPositionStateEntity state,
            ShadowCopyOperationEntity closedOperation,
            OffsetDateTime eventTime,
            String originId,
            BigDecimal closePrice,
            BigDecimal previousQty,
            BigDecimal previousNotional,
            BigDecimal realizedPnl,
            BigDecimal closeSlippageUsd
    ) {
        String previousSide = state.getPositionSide();
        String eventType = "SHADOW_POSITION_CLOSED";
        boolean exists = allocation.getWalletProfileId() != null
                ? shadowEventRepository.existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
                allocation.getWalletProfileId(), originId, eventType, previousSide, eventTime)
                : shadowEventRepository.existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
                allocation.getId(), originId, eventType, previousSide, eventTime);
        if (exists) {
            log.info("event=duplicate_shadow_flip_close_event_skipped originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} symbol={} previousSide={} reasonCode=DUPLICATE_SHADOW_FLIP_CLOSE_EVENT shadowImpact=NO_DUPLICATE_EVENT",
                    originId, allocation.getId(), allocation.getWalletProfileId(), state.getId(), op.getParSymbol(), previousSide);
            return;
        }

        ShadowCopyOperationEventEntity closeEvent = ShadowCopyOperationEventEntity.builder()
                .shadowOperationId(closedOperation == null ? null : closedOperation.getIdOperation())
                .shadowPositionId(state.getId())
                .shadowAllocationId(allocation.getId())
                .linkedLiveAllocationId(allocation.getLinkedLiveAllocationId())
                .walletProfileId(allocation.getWalletProfileId())
                .shadowValidationId(allocation.getShadowValidationId())
                .idOrderOrigin(originId)
                .idUser(allocation.getIdUser().toString())
                .idWalletOrigin(allocation.getWalletId())
                .copyStrategyCode(allocation.getCopyStrategyCode())
                .scopeType(allocation.getScopeType())
                .scopeValue(allocation.getScopeValue())
                .strategyKey(allocation.getStrategyKey())
                .parsymbol(op.getParSymbol())
                .typeOperation(previousSide)
                .eventType(eventType)
                .positionSide(previousSide)
                .qtyRequested(previousQty)
                .qtyExecuted(previousQty)
                .price(closePrice)
                .notionalUsd(previousNotional)
                .previousQty(previousQty)
                .resultingQty(ZERO)
                .realizedPnlUsd(realizedPnl)
                .feeUsd(ZERO)
                .slippageBps(BigDecimal.valueOf(shadowSlippageBps).setScale(6, RoundingMode.HALF_UP))
                .slippageUsd(firstNonNull(closeSlippageUsd, ZERO))
                .decision("SIMULATED")
                .decisionReason("shadow_flip_closed_previous_side")
                .source("shadow_copy")
                .reasonCode("SHADOW_POSITION_CLOSED_BY_FLIP")
                .eventTime(eventTime)
                .dateCreation(OffsetDateTime.now())
                .build();
        shadowEventRepository.save(closeEvent);
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
            long openPositions = countOpenShadowPositions(existing);
            existing.setStatus("SHADOW_PAUSED");
            existing.setCopyGuardAction("PAUSE_OPEN");
            existing.setLastValidationReason(openPositions > 0
                    ? "PAUSED_BY_RANKING_EXIT_OPEN_CYCLE"
                    : "PAUSED_BY_RANKING_EXIT");
            existing.setEndsAt(openPositions > 0 ? null : now);
            existing.setActive(openPositions > 0);
            existing.setUpdatedAt(now);
            shadowAllocationRepository.save(existing);
            paused++;
            log.info("event=shadow_allocation_ranking_exit_paused userId={} shadowAllocationId={} walletId={} strategyCode={} openPositions={} active={} reasonCode={} allowNewEntries=false allowReductions=true allowCloses=true",
                    idUser,
                    existing.getId(),
                    existing.getWalletId(),
                    existing.getCopyStrategyCode(),
                    openPositions,
                    existing.isActive(),
                    existing.getLastValidationReason());
        }
        return paused;
    }

    private long countOpenShadowPositions(ShadowCopyAllocationEntity allocation) {
        if (allocation == null) {
            return 0L;
        }
        if (allocation.getWalletProfileId() != null) {
            return Math.max(0L, shadowPositionStateRepository.countOpenPositionsByWalletProfileId(allocation.getWalletProfileId()));
        }
        if (allocation.getId() != null) {
            return Math.max(0L, shadowPositionStateRepository.countOpenPositions(allocation.getId()));
        }
        return 0L;
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

    private boolean blocksNewShadowExposure(ShadowCopyAllocationEntity allocation) {
        if (allocation == null || allocation.getStatus() == null) {
            return false;
        }
        String status = allocation.getStatus().trim().toUpperCase(Locale.ROOT).replace('-', '_');
        return "SHADOW_PAUSED".equals(status)
                || "SHADOW_OBSERVED".equals(status)
                || "PAUSED_BY_RANKING_EXIT".equals(status);
    }

    private String shadowStatus(UUID idUser, MetricaWalletDto dto) {
        String action = copyGuardAction(dto);
        String status = copyGuardStatus(dto);
        if ("SHADOW_ONLY".equals(action) || "SHADOW_ONLY".equals(status)) return "SHADOW_ONLY";
        if ("PAUSE_OPEN".equals(action) || "DISABLED".equals(action) || "DATA_RISK".equals(status)) return "SHADOW_REJECTED";
        if (!shadowValidationDecision(idUser, dto).passed()) return "SHADOW_ACTIVE";
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
            if (Boolean.FALSE.equals(windowComplete(dto, window))) {
                return false;
            }
            Double pnl = pnlWindow(dto, window);
            if (pnl == null || pnl <= 0.0) {
                return false;
            }
        }
        return true;
    }

    private String validationReason(MetricaWalletDto dto) {
        return validationReason(null, dto, false);
    }

    private String validationReason(MetricaWalletDto dto, boolean livePromotable) {
        return validationReason(null, dto, livePromotable);
    }

    private String validationReason(UUID idUser, MetricaWalletDto dto, boolean livePromotable) {
        if (livePromotable) {
            return "shadow_filters_passed";
        }
        String action = copyGuardAction(dto);
        String status = copyGuardStatus(dto);
        if (summaryDecisionNotFinal(dto)) return "SUMMARY_NOT_FINAL_LIVE_BLOCKED";
        if (simulationAuditFailed(dto)) return "SIMULATION_AUDIT_FAILED";
        if ("SHADOW_ONLY".equals(action) || "SHADOW_ONLY".equals(status)) return "shadow_only_by_copy_guard";
        if ("PAUSE_OPEN".equals(action)) return "pause_open_by_copy_guard";
        if ("DATA_RISK".equals(status)) return "data_risk_by_copy_guard";
        ShadowValidationDecision shadowValidation = shadowValidationDecision(idUser, dto);
        if (!shadowValidation.passed()) return shadowValidation.reason();
        String requiredWindowFailure = requiredWindowFailureReason(dto);
        if (requiredWindowFailure != null) return requiredWindowFailure;
        if (!slippageValidationPasses(dto)) return "slippage_simulation_failed";
        return "required_shadow_windows_not_positive:" + String.join(",", parseWindows(requirePositiveWindows));
    }

    private ShadowValidationDecision shadowValidationDecision(UUID idUser, MetricaWalletDto dto) {
        if (!requireShadowValidationBeforeLive) {
            return ShadowValidationDecision.passed("shadow_validation_not_required");
        }
        if (idUser == null) {
            return ShadowValidationDecision.blocked("shadow_validation_pending:user_missing");
        }
        String walletId = walletId(dto);
        String strategyCode = strategyCode(dto);
        if (walletId == null || strategyCode == null) {
            return ShadowValidationDecision.blocked("shadow_validation_pending:wallet_or_strategy_missing");
        }
        ShadowCopyAllocationEntity shadow = shadowAllocationRepository
                .findActiveStrategy(idUser, walletId, strategyCode, scopeType(dto), scopeValue(dto, strategyCode), shadowVersion)
                .orElse(null);
        if (shadow == null || shadow.getId() == null) {
            return ShadowValidationDecision.blocked("shadow_validation_pending:no_shadow_allocation");
        }

        long closed = shadow.getWalletProfileId() == null
                ? Math.max(0L, shadowPositionStateRepository.countClosedPositions(shadow.getId()))
                : Math.max(0L, shadowPositionStateRepository.countClosedPositionsByWalletProfileId(shadow.getWalletProfileId()));
        BigDecimal net = shadow.getWalletProfileId() == null
                ? firstNonNull(shadowPositionStateRepository.sumClosedRealizedPnlUsd(shadow.getId()), ZERO)
                : firstNonNull(shadowPositionStateRepository.sumClosedRealizedPnlUsdByWalletProfileId(shadow.getWalletProfileId()), ZERO);
        int minClosed = Math.max(0, minShadowClosedOperationsForLive);
        BigDecimal minNet = minShadowNetPnlUsdtForLive == null ? ZERO : minShadowNetPnlUsdtForLive;

        if (closed < minClosed) {
            return ShadowValidationDecision.blocked(
                    "shadow_validation_pending:closed=" + closed + "/min=" + minClosed + " net=" + net
            );
        }
        if (net.compareTo(minNet) <= 0) {
            return ShadowValidationDecision.blocked(
                    "shadow_validation_negative_pnl:closed=" + closed + " net=" + net + "/min_gt=" + minNet
            );
        }
        return ShadowValidationDecision.passed("shadow_validation_passed:closed=" + closed + " net=" + net);
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

    private record ShadowValidationDecision(boolean passed, String reason) {
        static ShadowValidationDecision passed(String reason) {
            return new ShadowValidationDecision(true, reason);
        }

        static ShadowValidationDecision blocked(String reason) {
            return new ShadowValidationDecision(false, reason);
        }
    }

    private String requiredWindowFailureReason(MetricaWalletDto dto) {
        List<String> windows = parseWindows(requirePositiveWindows);
        if (windows.isEmpty()) return null;
        for (String window : windows) {
            if (Boolean.FALSE.equals(windowComplete(dto, window))) {
                return "required_shadow_window_incomplete:" + window;
            }
            Double pnl = pnlWindow(dto, window);
            if (pnl == null) {
                return "required_shadow_window_missing:" + window;
            }
            if (pnl < 0.0) {
                return "NEGATIVE_REQUIRED_WINDOW_" + windowReasonCode(window);
            }
            if (pnl == 0.0) {
                return "NON_POSITIVE_REQUIRED_WINDOW_" + windowReasonCode(window);
            }
        }
        return null;
    }

    private static boolean simulationAuditFailed(MetricaWalletDto dto) {
        Map<String, Object> audit = simulationAudit(dto);
        if (audit == null || audit.isEmpty()) return false;
        Object valid = audit.get("valid");
        return Boolean.FALSE.equals(valid) || "false".equalsIgnoreCase(String.valueOf(valid));
    }

    private static String simulationAuditErrors(MetricaWalletDto dto) {
        Map<String, Object> audit = simulationAudit(dto);
        if (audit == null) return "";
        Object errors = audit.get("errors");
        return errors == null ? "" : String.valueOf(errors);
    }

    private static Map<String, Object> simulationAudit(MetricaWalletDto dto) {
        if (dto == null) return null;
        if (dto.getSimulationAudit() != null) return dto.getSimulationAudit();
        MetricaWalletDto.CopySimulationDto simulation = dto.getCopySimulation();
        if (simulation != null) {
            if (simulation.getSimulationAudit() != null) return simulation.getSimulationAudit();
            MetricaWalletDto.CopySizingDto sizing = simulation.getCopySizing();
            if (sizing != null && sizing.getSimulationAudit() != null) return sizing.getSimulationAudit();
        }
        return null;
    }

    private static Boolean windowComplete(MetricaWalletDto dto, String window) {
        MetricaWalletDto.CopySimulationDto simulation = dto == null ? null : dto.getCopySimulation();
        Map<String, Object> meta = windowMeta(simulation);
        if (meta == null || meta.isEmpty() || window == null || window.isBlank()) return null;
        Object item = meta.get(window.trim());
        if (item == null) item = meta.get(window.trim().toLowerCase(Locale.ROOT));
        if (!(item instanceof Map<?, ?> itemMap)) return null;
        Object complete = itemMap.get("complete");
        if (complete instanceof Boolean b) return b;
        if (complete == null) return null;
        if ("true".equalsIgnoreCase(String.valueOf(complete))) return true;
        if ("false".equalsIgnoreCase(String.valueOf(complete))) return false;
        return null;
    }

    private static Map<String, Object> windowMeta(MetricaWalletDto.CopySimulationDto simulation) {
        if (simulation == null) return null;
        if (simulation.getWindowMeta() != null) return simulation.getWindowMeta();
        MetricaWalletDto.CopySizingDto sizing = simulation.getCopySizing();
        return sizing == null ? null : sizing.getWindowMeta();
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

    private static String windowReasonCode(String window) {
        if (window == null || window.isBlank()) return "UNKNOWN";
        return window.trim().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]+", "_");
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

    private static boolean hasRealJewelHardBlockers(MetricaWalletDto dto) {
        MetricaWalletDto.RealJewelDto jewel = dto == null ? null : dto.getRealJewel();
        return hasNonBlank(jewel == null ? null : jewel.getHardBlockers())
                || hasNonBlank(jewel == null ? null : jewel.getHardRejectReasons());
    }

    private static boolean hasRealJewelDataWarning(MetricaWalletDto dto, String expected) {
        MetricaWalletDto.RealJewelDto jewel = dto == null ? null : dto.getRealJewel();
        String code = normalizeStatus(expected);
        if (jewel == null || jewel.getDataWarnings() == null || code == null) {
            return false;
        }
        return jewel.getDataWarnings().stream()
                .filter(Objects::nonNull)
                .map(ShadowCopyTradingServiceImpl::normalizeStatus)
                .anyMatch(code::equals);
    }

    private static boolean hasNonBlank(List<String> values) {
        return values != null && values.stream().anyMatch(value -> value != null && !value.isBlank());
    }

    private static String realJewelRiskClass(MetricaWalletDto dto) {
        MetricaWalletDto.RealJewelDto jewel = dto == null ? null : dto.getRealJewel();
        String riskClass = normalizeStatus(jewel == null ? null : jewel.getRiskClass());
        return riskClass == null ? "B" : riskClass;
    }

    private double microLiveEvidenceThreshold(String riskClass) {
        return switch (riskClass) {
            case "A" -> evidenceMicroThresholdA;
            case "C" -> evidenceMicroThresholdC;
            case "D" -> evidenceMicroThresholdD;
            default -> evidenceMicroThresholdB;
        };
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

    private static MetricaWalletDto.ActivityDto walletActivity(MetricaWalletDto dto) {
        if (dto == null) return null;
        return dto.getWalletActivity() == null ? dto.getActivity() : dto.getWalletActivity();
    }

    private static MetricaWalletDto.ActivityDto strategyActivity(MetricaWalletDto dto) {
        if (dto == null) return null;
        return dto.getStrategyActivity() == null ? dto.getActivity() : dto.getStrategyActivity();
    }

    private static OffsetDateTime strategyLastActivityAt(MetricaWalletDto dto) {
        if (dto == null) return null;
        MetricaWalletDto.ActivityDto explicit = dto.getStrategyActivity();
        if (explicit != null) {
            return firstNonNull(explicit.getStrategyLastActivityAt(), explicit.getLastActivityAt());
        }
        MetricaWalletDto.ActivityDto legacy = dto.getActivity();
        return legacy == null ? null : legacy.getStrategyLastActivityAt();
    }

    private static OffsetDateTime strategyLastOpenedAt(MetricaWalletDto dto) {
        if (dto == null) return null;
        MetricaWalletDto.ActivityDto explicit = dto.getStrategyActivity();
        if (explicit != null) {
            return firstNonNull(explicit.getStrategyLastOpenedAt(), explicit.getLastOpenedAt());
        }
        MetricaWalletDto.ActivityDto legacy = dto.getActivity();
        return legacy == null ? null : legacy.getStrategyLastOpenedAt();
    }

    private static OffsetDateTime strategyLastClosedAt(MetricaWalletDto dto) {
        if (dto == null) return null;
        MetricaWalletDto.ActivityDto explicit = dto.getStrategyActivity();
        if (explicit != null) {
            return firstNonNull(explicit.getStrategyLastClosedAt(), explicit.getLastClosedAt());
        }
        MetricaWalletDto.ActivityDto legacy = dto.getActivity();
        return legacy == null ? null : legacy.getStrategyLastClosedAt();
    }

    private static boolean summaryDecisionNotFinal(MetricaWalletDto dto) {
        if (dto == null) return true;
        if (Boolean.FALSE.equals(dto.getDecisionFinal())) return true;
        if (summaryConsistencySaysNotEquivalent(dto.getSummaryConsistency())) return true;

        MetricaWalletDto.CopySimulationDto simulation = dto.getCopySimulation();
        if (simulation != null) {
            if (Boolean.FALSE.equals(simulation.getDecisionFinal())) return true;
            if (Boolean.TRUE.equals(simulation.getRequiresSimulationFull())) return true;
            if (summaryConsistencySaysNotEquivalent(simulation.getSummaryConsistency())) return true;
            if (isCandidateOnlyDecisionUse(simulation.getDecisionUse())) return true;

            MetricaWalletDto.CopySizingDto sizing = simulation.getCopySizing();
            if (sizing != null) {
                if (Boolean.FALSE.equals(sizing.getDecisionFinal())) return true;
                if (Boolean.TRUE.equals(sizing.getRequiresSimulationFull())) return true;
                if (Boolean.FALSE.equals(sizing.getFactPayloadLoaded()) && Boolean.TRUE.equals(sizing.getSummaryMode())) return true;
                if (summaryConsistencySaysNotEquivalent(sizing.getSummaryConsistency())) return true;
                if (isCandidateOnlyDecisionUse(sizing.getDecisionUse())) return true;
            }
        }
        return false;
    }

    private static String summaryNotFinalReason(MetricaWalletDto dto) {
        if (dto == null) return "METRIC_NULL";
        Object topReason = reasonFromSummaryConsistency(dto.getSummaryConsistency());
        if (topReason != null) return String.valueOf(topReason);
        MetricaWalletDto.CopySimulationDto simulation = dto.getCopySimulation();
        if (simulation != null) {
            Object simulationReason = reasonFromSummaryConsistency(simulation.getSummaryConsistency());
            if (simulationReason != null) return String.valueOf(simulationReason);
            if (Boolean.TRUE.equals(simulation.getRequiresSimulationFull())) return "REQUIRES_FULL_SIMULATION";
            if (isCandidateOnlyDecisionUse(simulation.getDecisionUse())) return "SUMMARY_CANDIDATE_ONLY";
            MetricaWalletDto.CopySizingDto sizing = simulation.getCopySizing();
            if (sizing != null) {
                Object sizingReason = reasonFromSummaryConsistency(sizing.getSummaryConsistency());
                if (sizingReason != null) return String.valueOf(sizingReason);
                if (Boolean.TRUE.equals(sizing.getRequiresSimulationFull())) return "REQUIRES_FULL_SIMULATION";
                if (Boolean.FALSE.equals(sizing.getFactPayloadLoaded())) return "FACT_PAYLOAD_NOT_LOADED";
                if (isCandidateOnlyDecisionUse(sizing.getDecisionUse())) return "SUMMARY_CANDIDATE_ONLY";
            }
        }
        return "SUMMARY_NOT_FINAL";
    }

    private static boolean summaryConsistencySaysNotEquivalent(Map<String, Object> consistency) {
        if (consistency == null || consistency.isEmpty()) return false;
        Object equivalent = consistency.get("businessDecisionEquivalentToFull");
        return Boolean.FALSE.equals(equivalent) || "false".equalsIgnoreCase(String.valueOf(equivalent));
    }

    private static Object reasonFromSummaryConsistency(Map<String, Object> consistency) {
        if (!summaryConsistencySaysNotEquivalent(consistency)) return null;
        Object reason = consistency.get("reason");
        return reason == null ? "SUMMARY_NOT_EQUIVALENT_TO_FULL" : reason;
    }

    private static boolean isCandidateOnlyDecisionUse(String decisionUse) {
        if (decisionUse == null || decisionUse.isBlank()) return false;
        String value = decisionUse.trim().toLowerCase(Locale.ROOT);
        return value.contains("candidate_only") || value.contains("prefilter_only");
    }

    private void logMetricJewelItemSeen(MetricaWalletDto dto) {
        String walletId = walletId(dto);
        String strategyCode = strategyCode(dto);
        String scopeType = scopeType(dto);
        String scopeValue = scopeValue(dto, strategyCode);
        String profileKey = walletId == null || strategyCode == null ? null : strategyKey(walletId, strategyCode, scopeType, scopeValue);
        CopyStrategyRuntimeRouter.CopyProfileCategory category = copyStrategyRuntimeRouter.profileCategory(strategyCode);
        boolean realCopyProfile = category == CopyStrategyRuntimeRouter.CopyProfileCategory.CORE_COPY_PROFILE
                || category == CopyStrategyRuntimeRouter.CopyProfileCategory.ADVANCED_COPY_PROFILE;
        boolean scoringWindow = category == CopyStrategyRuntimeRouter.CopyProfileCategory.SCORING_WINDOW;
        boolean diagnosticOnly = category == CopyStrategyRuntimeRouter.CopyProfileCategory.DIAGNOSTIC_ONLY
                || category == CopyStrategyRuntimeRouter.CopyProfileCategory.ROBUSTNESS_CHECK;
        boolean shadowEligible = copyStrategyRuntimeRouter.isShadowEligibleJoyasCandidate(dto) && isRelevantForShadow(dto);
        boolean liveEligible = copyStrategyRuntimeRouter.isLiveEligibleJoyasCandidate(dto);
        log.info("event=metric_jewel_item_seen walletId={} copyProfileCode={} scopeType={} scopeValue={} profileKey={} category={} isRealCopyProfile={} isScoringWindow={} isDiagnosticOnly={} shadowEligible={} liveEligible={} actionTaken={} reasonCode=JEWEL_ITEM_NORMALIZED reasonMessage=\"Joya normalizada como perfil independiente wallet+estrategia\"",
                walletId, strategyCode, scopeType, scopeValue, profileKey, category, realCopyProfile, scoringWindow, diagnosticOnly, shadowEligible, liveEligible,
                scoringWindow ? "SCORING_WINDOW_ATTACHED_AS_EVIDENCE" : diagnosticOnly ? "DIAGNOSTIC_ATTACHED_AS_EVIDENCE" : shadowEligible ? "PROFILE_SENT_TO_SHADOW" : "PROFILE_BLOCKED");
        if (scoringWindow) {
            log.info("event=scoring_window_attached_as_evidence walletId={} copyProfileCode={} profileKey={} reasonCode=SCORING_WINDOW_NOT_COPY_PROFILE reasonMessage=\"{} se usa como evidencia historica, no como perfil LIVE\" liveImpact=LIVE_BLOCKED",
                    walletId, strategyCode, profileKey, strategyCode);
        } else if (diagnosticOnly) {
            log.info("event=diagnostic_attached_as_evidence walletId={} copyProfileCode={} profileKey={} category={} reasonCode=DIAGNOSTIC_NOT_COPY_PROFILE reasonMessage=\"Perfil usado para diagnostico/robustez, no para allocation LIVE completa\" liveImpact=LIVE_BLOCKED",
                    walletId, strategyCode, profileKey, category);
        }
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

    private static String profileConfigHash(String strategyCode, String scopeType, String scopeValue) {
        String code = normalizeStrategy(strategyCode);
        String type = normalizeScopeType(scopeType);
        if (!"TOP_SYMBOLS_ONLY".equals(code) && !"dynamic_symbol_set".equals(type)) {
            return null;
        }
        String canonical = canonicalSymbolSet(scopeValue);
        if (canonical == null) {
            canonical = normalizeScopeValue(scopeValue, code);
        }
        return "sha256:" + sha256Hex(canonical).substring(0, 32);
    }

    private static String canonicalSymbolSet(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        List<String> symbols = java.util.Arrays.stream(raw.split("[,;|\\s]+"))
                .map(ShadowCopyTradingServiceImpl::normalizeSymbol)
                .filter(Objects::nonNull)
                .distinct()
                .sorted()
                .toList();
        return symbols.isEmpty() ? null : String.join(",", symbols);
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                out.append(String.format("%02x", b));
            }
            return out.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available", ex);
        }
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

    private static BigDecimal resolveShadowExecutionPrice(OperacionDto op, CopyJobAction action, HyperliquidDeltaType deltaType) {
        if (op == null) {
            return null;
        }
        if (action == CopyJobAction.CLOSE || deltaType == HyperliquidDeltaType.CLOSE) {
            return firstPositive(op.getPrecioCierre(), op.getPrecioMercado(), op.getPrecioEntrada());
        }
        if (deltaType == HyperliquidDeltaType.FLIP) {
            return firstPositive(op.getPrecioMercado(), op.getPrecioEntrada(), op.getPrecioCierre());
        }
        if (deltaType == HyperliquidDeltaType.RESIZE || deltaType == HyperliquidDeltaType.UPDATE) {
            return firstPositive(op.getPrecioMercado(), op.getPrecioEntrada(), op.getPrecioCierre());
        }
        return firstPositive(op.getPrecioEntrada(), op.getPrecioMercado(), op.getPrecioCierre());
    }

    private static BigDecimal resolveShadowNotional(OperacionDto op, BigDecimal qty, BigDecimal price) {
        BigDecimal explicit = op == null ? null : firstPositive(op.getNotionalUsd(), op.getMarginUsedUsd(), op.getSize());
        if (isPositive(explicit)) {
            return explicit.abs();
        }
        BigDecimal safeQty = qty == null ? ZERO : qty.abs();
        return isPositive(price) && safeQty.signum() > 0 ? safeQty.multiply(price).abs() : ZERO;
    }

    private static boolean requiresValidShadowPrice(CopyJobAction action, HyperliquidDeltaType deltaType) {
        return action == CopyJobAction.CLOSE
                || deltaType == HyperliquidDeltaType.CLOSE
                || deltaType == HyperliquidDeltaType.FLIP
                || deltaType == HyperliquidDeltaType.OPEN
                || deltaType == HyperliquidDeltaType.RESIZE
                || deltaType == HyperliquidDeltaType.UPDATE;
    }

    private static String missingPriceReason(CopyJobAction action, HyperliquidDeltaType deltaType) {
        if (action == CopyJobAction.CLOSE || deltaType == HyperliquidDeltaType.CLOSE || deltaType == HyperliquidDeltaType.FLIP) {
            return "PRICE_CLOSE_MISSING";
        }
        return "PRICE_SOURCE_UNAVAILABLE";
    }

    private static String shadowPriceUsage(CopyJobAction action, HyperliquidDeltaType deltaType) {
        if (action == CopyJobAction.CLOSE || deltaType == HyperliquidDeltaType.CLOSE) {
            return "CLOSE";
        }
        if (deltaType == HyperliquidDeltaType.FLIP) {
            return "FLIP";
        }
        if (deltaType == HyperliquidDeltaType.RESIZE || deltaType == HyperliquidDeltaType.UPDATE) {
            return "RESIZE";
        }
        return "OPEN";
    }

    private static String copyImpactForMissingPrice(CopyJobAction action, HyperliquidDeltaType deltaType) {
        return "PRICE_CLOSE_MISSING".equals(missingPriceReason(action, deltaType))
                ? "shadow_close_rejected_missing_price"
                : "shadow_price_resolution_limited";
    }

    private static BigDecimal positiveOrFallback(BigDecimal value, BigDecimal fallback) {
        return isPositive(value) ? value.abs() : (fallback == null ? ZERO : fallback.abs());
    }

    private static boolean isPositive(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0;
    }

    private static BigDecimal firstPositive(BigDecimal... values) {
        if (values == null) {
            return null;
        }
        for (BigDecimal value : values) {
            if (isPositive(value)) {
                return value;
            }
        }
        return null;
    }

    private BigDecimal slippageUsd(BigDecimal notional) {
        if (notional == null || notional.signum() <= 0 || shadowSlippageBps <= 0) {
            return ZERO;
        }
        return notional.multiply(BigDecimal.valueOf(shadowSlippageBps))
                .divide(BigDecimal.valueOf(10_000), 12, RoundingMode.HALF_UP);
    }

    private PositionDeltaClassification classifyShadowPosition(
            String originalEventType,
            HyperliquidDeltaType originalDeltaType,
            String positionSide,
            BigDecimal previousQty,
            BigDecimal resultingQty,
            BigDecimal executionPrice,
            BigDecimal avgEntryPrice,
            OperacionDto op,
            ShadowCopyAllocationEntity allocation
    ) {
        return copyPositionAccountingService.classify(new PositionDeltaClassificationInput(
                originalEventType,
                originalDeltaType == null ? null : originalDeltaType.name(),
                positionSide,
                positionSide,
                previousQty,
                resultingQty,
                previousQty == null || resultingQty == null ? null : resultingQty.subtract(previousQty),
                executionPrice,
                avgEntryPrice,
                op == null ? null : op.getParSymbol(),
                allocation == null ? null : allocation.getWalletId(),
                runtimeProfileKey(allocation),
                "shadow"
        ));
    }

    private void logClassificationCorrectionIfNeeded(
            PositionDeltaClassification classification,
            String originalEventType,
            HyperliquidDeltaType originalDeltaType,
            String positionSide,
            BigDecimal previousQty,
            BigDecimal resultingQty,
            BigDecimal executionPrice,
            OperacionDto op,
            ShadowCopyAllocationEntity allocation,
            String originId
    ) {
        if (classification == null || !classification.corrected()) {
            return;
        }
        BigDecimal deltaQty = previousQty == null || resultingQty == null ? null : resultingQty.subtract(previousQty);
        log.info("event=copy_position.classification.corrected flow=shadow originalEventType={} originalDeltaType={} computedDeltaType={} reasonCode={} warningCode={} previousQty={} resultingQty={} deltaQty={} executionPrice={} symbol={} walletId={} profileKey={} strategyCode={} side={} originId={} correctionApplied=true confidence=POSITION_MATH",
                originalEventType,
                originalDeltaType,
                classification.computedDeltaType(),
                classification.warningCode(),
                classification.warningCode(),
                previousQty,
                resultingQty,
                deltaQty,
                executionPrice,
                op == null ? null : op.getParSymbol(),
                allocation == null ? null : allocation.getWalletId(),
                runtimeProfileKey(allocation),
                allocation == null ? null : allocation.getCopyStrategyCode(),
                positionSide,
                originId);
    }

    private void logShadowFlowLatency(
            CopyFlowTiming timing,
            String result,
            String originalEventType,
            String computedDeltaType,
            String reasonCode,
            OperacionDto op,
            ShadowCopyAllocationEntity allocation,
            String side,
            String originId
    ) {
        if (timing == null) {
            return;
        }
        log.info("{} originalEventType={} computedDeltaType={} reasonCode={} symbol={} side={} walletId={} profileKey={} strategyCode={} originId={} traceId={}",
                timing.logfmtCore("shadow", result),
                safeLog(originalEventType),
                safeLog(computedDeltaType),
                safeLog(reasonCode),
                op == null ? null : safeLog(op.getParSymbol()),
                safeLog(side),
                allocation == null ? null : safeLog(allocation.getWalletId()),
                safeLog(runtimeProfileKey(allocation)),
                allocation == null ? null : safeLog(allocation.getCopyStrategyCode()),
                safeLog(originId),
                org.slf4j.MDC.get("traceId"));
    }

    private String computedDeltaTypeForImpact(ShadowPositionImpact impact) {
        if (impact == null || impact.reasonCode() == null) {
            return "INVALID";
        }
        return switch (impact.reasonCode()) {
            case "SHADOW_POSITION_OPENED", "SHADOW_POSITION_OPENED_BY_FLIP" -> "OPEN";
            case "SHADOW_POSITION_RESIZED" -> "INCREASE";
            case "SHADOW_POSITION_REDUCED" -> "REDUCE";
            case "SHADOW_POSITION_CLOSED", "SHADOW_POSITION_CLOSED_BY_FLIP" -> "CLOSE_FULL";
            case "SHADOW_RESIZE_NOOP" -> "NOOP";
            default -> "INVALID";
        };
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', '_')
                .replace('\r', '_')
                .replace('\t', '_')
                .replace(' ', '_')
                .replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }

    private CopyAccountingResult applyShadowAccounting(
            String symbol,
            String side,
            BigDecimal previousQty,
            BigDecimal resultingQty,
            BigDecimal avgEntryPrice,
            BigDecimal executionPrice,
            BigDecimal feeUsd,
            BigDecimal slippageUsd,
            OffsetDateTime eventTime,
            String shadowReasonCode
    ) {
        CopyFlowTiming timing = SHADOW_FLOW_TIMING.get();
        long accountingNs = timing == null ? 0L : timing.mark();
        CopyAccountingResult result = copyPositionAccountingService.apply(new CopyAccountingInput(
                symbol,
                parsePositionSide(side),
                previousQty,
                resultingQty,
                avgEntryPrice,
                executionPrice,
                feeUsd,
                slippageUsd,
                eventTime == null ? null : eventTime.toInstant(),
                AccountingMode.SHADOW
        ));
        if (timing != null) {
            timing.add(CopyFlowTiming.Stage.ACCOUNTING, accountingNs);
        }
        if (result.accepted()) {
            log.info("event=shadow_copy_accounting_applied reasonCode={} symbol={} side={} deltaType={}",
                    shadowReasonCode, symbol, side, result.deltaType());
        }
        return result;
    }

    private PositionSide parsePositionSide(String side) {
        String normalized = normalizeStatus(side);
        if ("LONG".equals(normalized)) {
            return PositionSide.LONG;
        }
        if ("SHORT".equals(normalized)) {
            return PositionSide.SHORT;
        }
        return null;
    }

    private String pnlFormula(String side) {
        return copyPositionAccountingService.pnlFormula(parsePositionSide(side));
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
