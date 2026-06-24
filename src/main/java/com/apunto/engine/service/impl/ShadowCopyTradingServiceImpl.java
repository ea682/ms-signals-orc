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
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
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

    private record ShadowPositionImpact(
            String reasonCode,
            String shadowImpact,
            BigDecimal realizedPnlUsd,
            UUID shadowPositionId,
            boolean positionUpdated
    ) {
        static ShadowPositionImpact opened(UUID shadowPositionId) {
            return new ShadowPositionImpact("SHADOW_POSITION_OPENED", "POSITION_OPENED", null, shadowPositionId, true);
        }

        static ShadowPositionImpact resized(UUID shadowPositionId) {
            return new ShadowPositionImpact("SHADOW_POSITION_RESIZED", "POSITION_RESIZED", null, shadowPositionId, true);
        }

        static ShadowPositionImpact closed(BigDecimal pnl, UUID shadowPositionId) {
            return new ShadowPositionImpact("SHADOW_POSITION_CLOSED", "POSITION_CLOSED", pnl, shadowPositionId, true);
        }

        static ShadowPositionImpact resizeWithoutOpen() {
            return new ShadowPositionImpact("RESIZE_WITHOUT_SHADOW_OPEN", "NO_VALID_POSITION", null, null, false);
        }

        static ShadowPositionImpact warmupResizeWithoutOpen() {
            return new ShadowPositionImpact("WARMUP_RESIZE_WITHOUT_SHADOW_OPEN", "WARMUP_NO_VALID_POSITION", null, null, false);
        }

        static ShadowPositionImpact resizeAfterClosed(UUID shadowPositionId) {
            return new ShadowPositionImpact("RESIZE_AFTER_SHADOW_CLOSED", "NO_VALID_POSITION", null, shadowPositionId, false);
        }

        static ShadowPositionImpact closeWithoutOpen() {
            return new ShadowPositionImpact("CLOSE_WITHOUT_SHADOW_OPEN", "NO_VALID_PNL", null, null, false);
        }

        static ShadowPositionImpact warmupCloseWithoutOpen() {
            return new ShadowPositionImpact("WARMUP_CLOSE_WITHOUT_SHADOW_OPEN", "WARMUP_NO_VALID_PNL", null, null, false);
        }
    }

    private final ShadowCopyAllocationRepository shadowAllocationRepository;
    private final ShadowCopyOperationRepository shadowOperationRepository;
    private final ShadowCopyOperationEventRepository shadowEventRepository;
    private final ShadowPositionStateRepository shadowPositionStateRepository;
    private final CopyWalletProfileRepository copyWalletProfileRepository;
    private final ShadowWalletProfileValidationRepository shadowProfileValidationRepository;
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

    @Value("${metric-wallet.shadow.require-validation-before-live:true}")
    private boolean requireShadowValidationBeforeLive;

    @Value("${metric-wallet.shadow.min-closed-operations-for-live:50}")
    private int minShadowClosedOperationsForLive;

    @Value("${metric-wallet.shadow.min-net-pnl-usdt-for-live:0}")
    private BigDecimal minShadowNetPnlUsdtForLive;

    @Value("${metric-wallet.shadow.warmup-minutes:60}")
    private long shadowWarmupMinutes;

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
            MetricaWalletDto.ActivityDto activity = dto.getActivity();
            entity.setWalletLastActivityAt(firstNonNull(activity == null ? null : activity.getWalletLastActivityAt(), activity == null ? null : activity.getLastActivityAt()));
            entity.setWalletLastOpenedAt(firstNonNull(activity == null ? null : activity.getWalletLastOpenedAt(), activity == null ? null : activity.getLastOpenedAt()));
            entity.setWalletLastClosedAt(firstNonNull(activity == null ? null : activity.getWalletLastClosedAt(), activity == null ? null : activity.getLastClosedAt()));
            entity.setStrategyLastActivityAt(activity == null ? null : activity.getStrategyLastActivityAt());
            entity.setStrategyLastOpenedAt(activity == null ? null : activity.getStrategyLastOpenedAt());
            entity.setStrategyLastClosedAt(activity == null ? null : activity.getStrategyLastClosedAt());
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
        MetricaWalletDto.ActivityDto activity = dto == null ? null : dto.getActivity();
        profile.setWalletLastActivityAt(firstNonNull(activity == null ? null : activity.getWalletLastActivityAt(), activity == null ? null : activity.getLastActivityAt()));
        profile.setWalletLastOpenedAt(firstNonNull(activity == null ? null : activity.getWalletLastOpenedAt(), activity == null ? null : activity.getLastOpenedAt()));
        profile.setWalletLastClosedAt(firstNonNull(activity == null ? null : activity.getWalletLastClosedAt(), activity == null ? null : activity.getLastClosedAt()));
        profile.setStrategyLastActivityAt(activity == null ? null : activity.getStrategyLastActivityAt());
        profile.setStrategyLastOpenedAt(activity == null ? null : activity.getStrategyLastOpenedAt());
        profile.setStrategyLastClosedAt(activity == null ? null : activity.getStrategyLastClosedAt());
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
        validation.setStatus(status == null ? "SHADOW_TESTING" : status);
        validation.setClosedPositions(closed);
        validation.setOpenPositions(open);
        validation.setNetPnlUsd(net);
        validation.setGrossPnlUsd(net.add(slippage));
        validation.setFeesUsd(ZERO);
        validation.setSlippageUsd(slippage);
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
            boolean saved = recordShadowForAllocation(allocation, event, action, deltaType);
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

    private boolean recordShadowForAllocation(
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

        shadowEventRepository.lockShadowEventIdempotency(shadowEventIdempotencyKey(allocation, originId, eventType, typeOperation, eventTime));
        boolean duplicate = allocation.getWalletProfileId() != null
                ? shadowEventRepository.existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
                allocation.getWalletProfileId(), originId, eventType, typeOperation, eventTime)
                : shadowEventRepository.existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
                allocation.getId(), originId, eventType, typeOperation, eventTime);
        if (duplicate) {
            log.info("event=duplicate_shadow_event_skipped originId={} shadowAllocationId={} walletProfileId={} walletId={} copyProfileCode={} symbol={} side={} action={} deltaType={} reasonCode=DUPLICATE_SHADOW_EVENT shadowImpact=NO_DUPLICATE_EVENT",
                    originId, allocation.getId(), allocation.getWalletProfileId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), typeOperation, action, deltaType);
            return false;
        }

        ShadowPositionImpact impact = updateShadowPositionState(allocation, op, action, deltaType, eventTime, qty, notional, price, slippageUsd, originId, typeOperation);
        ShadowCopyOperationEntity shadowOperation = null;
        if (impact.positionUpdated()) {
            if (action == CopyJobAction.CLOSE) {
                shadowOperation = closeShadowOperation(allocation, op, eventTime, originId, typeOperation, price, impact.realizedPnlUsd());
            } else {
                shadowOperation = upsertOpenShadowOperation(allocation, op, deltaType, eventTime, originId, typeOperation, qty, notional, price, slippageUsd);
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
                .qtyExecuted(qty)
                .price(price)
                .notionalUsd(notional)
                .resultingQty(action == CopyJobAction.CLOSE ? ZERO : qty)
                .realizedPnlUsd(impact.realizedPnlUsd())
                .feeUsd(ZERO)
                .slippageBps(BigDecimal.valueOf(shadowSlippageBps).setScale(6, RoundingMode.HALF_UP))
                .slippageUsd(slippageUsd)
                .decision(impact.positionUpdated() ? "SIMULATED" : "SKIPPED")
                .decisionReason("shadow_separate_table")
                .source("shadow_copy")
                .reasonCode(impact.reasonCode())
                .eventTime(eventTime)
                .dateCreation(OffsetDateTime.now())
                .build();
        shadowEventRepository.save(shadowEvent);
        updateRuntimeActivity(allocation, eventType, impact, eventTime);
        refreshProfileValidationAfterEvent(allocation, eventType, impact, eventTime);
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
                notional,
                slippageUsd);
        return true;
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
        shadowOperation.setSizePar(qty);
        shadowOperation.setSizeUsd(notional);
        if (shadowOperation.getPriceEntry() == null || lifecycleStart) {
            shadowOperation.setPriceEntry(price);
        }
        shadowOperation.setSimulatedSlippageUsd(firstNonNull(shadowOperation.getSimulatedSlippageUsd(), ZERO).add(slippageUsd));
        shadowOperation.setSimulatedFeeUsd(ZERO);
        shadowOperation.setActive(true);
        if (shadowOperation.getStatus() == null || shadowOperation.getStatus().isBlank()) {
            shadowOperation.setStatus("OPEN");
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
        shadowOperation.setActive(false);
        shadowOperation.setStatus("CLOSED");
        shadowOperation.setDateClose(eventTime);
        shadowOperation.setPriceClose(price);
        shadowOperation.setRealizedPnlUsd(realizedPnlUsd);
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
            if ("CLOSE".equals(eventType)) {
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
                if ("CLOSE".equals(eventType)) {
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
        if (impact == null || !impact.positionUpdated()) {
            return false;
        }
        return "OPEN".equals(eventType) || "CLOSE".equals(eventType) || "FLIP".equals(eventType);
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
            BigDecimal slippageUsd,
            String originId,
            String positionSide
    ) {
        if (deltaType == HyperliquidDeltaType.FLIP && action != CopyJobAction.CLOSE) {
            closeOtherOpenShadowSides(allocation, op, eventTime, originId, positionSide);
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
            BigDecimal totalSlippage = firstNonNull(state.getSlippageUsd(), ZERO).add(slippageUsd);
            BigDecimal realizedPnl = shadowRealizedPnl(
                    state.getPositionSide(),
                    firstNonNull(state.getQty(), qty),
                    firstNonNull(state.getEntryPrice(), op.getPrecioEntrada()),
                    price
            ).subtract(firstNonNull(state.getFeesUsd(), ZERO))
                    .subtract(totalSlippage);
            state.setStatus("CLOSED");
            state.setClosedAt(eventTime);
            state.setQty(ZERO);
            state.setMarkPrice(price);
            state.setSlippageUsd(totalSlippage);
            state.setRealizedPnlUsd(realizedPnl);
            state.setLastSourceEventId(originId);
            shadowPositionStateRepository.save(state);
            log.info("event=shadow_position_closed originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} realizedPnlUsd={} reasonCode=SHADOW_POSITION_CLOSED shadowImpact=POSITION_CLOSED",
                    originId, allocation.getId(), allocation.getWalletProfileId(), state.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, realizedPnl);
            return ShadowPositionImpact.closed(realizedPnl, state.getId());
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
        state.setQty(qty);
        state.setEntryPrice(price);
        state.setMarkPrice(firstNonNull(op.getPrecioMercado(), price));
        state.setNotionalUsd(notional);
        state.setSlippageUsd(firstNonNull(state.getSlippageUsd(), ZERO).add(slippageUsd));
        state.setLastSourceEventId(originId);
        shadowPositionStateRepository.save(state);
        if (opened) {
            log.info("event=shadow_position_opened originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} qty={} price={} reasonCode=SHADOW_POSITION_OPENED shadowImpact=POSITION_OPENED",
                    originId, allocation.getId(), allocation.getWalletProfileId(), state.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, qty, price);
            return ShadowPositionImpact.opened(state.getId());
        }
        log.info("event=shadow_position_resized originId={} shadowAllocationId={} walletProfileId={} shadowPositionId={} walletId={} copyProfileCode={} symbol={} side={} qty={} price={} reasonCode=SHADOW_POSITION_RESIZED shadowImpact=POSITION_RESIZED",
                originId, allocation.getId(), allocation.getWalletProfileId(), state.getId(), allocation.getWalletId(), allocation.getCopyStrategyCode(), op.getParSymbol(), positionSide, qty, price);
        return ShadowPositionImpact.resized(state.getId());
    }

    private void closeOtherOpenShadowSides(
            ShadowCopyAllocationEntity allocation,
            OperacionDto op,
            OffsetDateTime eventTime,
            String originId,
            String newPositionSide
    ) {
        List<ShadowPositionStateEntity> openStates = allocation.getWalletProfileId() == null
                ? shadowPositionStateRepository.findAllByShadowAllocationIdAndParsymbolAndStatus(allocation.getId(), op.getParSymbol(), "OPEN")
                : shadowPositionStateRepository.findAllByWalletProfileIdAndParsymbolAndStatus(allocation.getWalletProfileId(), op.getParSymbol(), "OPEN");
        for (ShadowPositionStateEntity state : openStates) {
            if (state == null || Objects.equals(state.getPositionSide(), newPositionSide)) {
                continue;
            }
            BigDecimal closePrice = firstNonNull(op.getPrecioMercado(), op.getPrecioEntrada(), op.getPrecioCierre());
            BigDecimal realizedPnl = shadowRealizedPnl(
                    state.getPositionSide(),
                    state.getQty(),
                    state.getEntryPrice(),
                    closePrice
            ).subtract(firstNonNull(state.getFeesUsd(), ZERO))
                    .subtract(firstNonNull(state.getSlippageUsd(), ZERO));
            state.setStatus("CLOSED");
            state.setClosedAt(eventTime);
            state.setQty(ZERO);
            state.setMarkPrice(closePrice);
            state.setRealizedPnlUsd(realizedPnl);
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
        if ("SHADOW_ONLY".equals(action) || "SHADOW_ONLY".equals(status)) return "shadow_only_by_copy_guard";
        if ("PAUSE_OPEN".equals(action)) return "pause_open_by_copy_guard";
        if ("DATA_RISK".equals(status)) return "data_risk_by_copy_guard";
        ShadowValidationDecision shadowValidation = shadowValidationDecision(idUser, dto);
        if (!shadowValidation.passed()) return shadowValidation.reason();
        if (!requiredWindowsPositive(dto)) {
            return "required_shadow_windows_not_positive:" + String.join(",", parseWindows(requirePositiveWindows));
        }
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

    private BigDecimal slippageUsd(BigDecimal notional) {
        if (notional == null || notional.signum() <= 0 || shadowSlippageBps <= 0) {
            return ZERO;
        }
        return notional.multiply(BigDecimal.valueOf(shadowSlippageBps))
                .divide(BigDecimal.valueOf(10_000), 12, RoundingMode.HALF_UP);
    }

    private static BigDecimal shadowRealizedPnl(String side, BigDecimal qty, BigDecimal entryPrice, BigDecimal closePrice) {
        BigDecimal safeQty = qty == null ? ZERO : qty.abs();
        if (safeQty.signum() <= 0 || entryPrice == null || closePrice == null) {
            return ZERO;
        }
        String normalizedSide = normalizeStatus(side);
        if ("SHORT".equals(normalizedSide)) {
            return entryPrice.subtract(closePrice).multiply(safeQty).setScale(12, RoundingMode.HALF_UP);
        }
        return closePrice.subtract(entryPrice).multiply(safeQty).setScale(12, RoundingMode.HALF_UP);
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
