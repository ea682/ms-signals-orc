package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailService;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver;
import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver.CopyModeResolution;
import com.apunto.engine.service.copy.symbol.CopySymbolResolution;
import com.apunto.engine.service.copy.symbol.CopySymbolResolver;
import com.apunto.engine.shared.enums.FuturesCapitalAsset;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserCopyAllocationServiceImpl implements UserCopyAllocationService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final UserCopyAllocationRepository repository;
    private final UserDetailService userDetailService;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;
    private final ShadowCopyTradingService shadowCopyTradingService;
    private final CopySymbolResolver copySymbolResolver;
    @PersistenceContext
    private EntityManager entityManager;

    @Value("${metric-wallet.allocation.default-execution-mode:LIVE}")
    private String defaultExecutionMode;

    @Value("${metric-wallet.shadow.reentry-capital-multiplier:0.25}")
    private double shadowReentryCapitalMultiplier;

    @Value("${metric-wallet.allocation.runtime-cache-ttl:1500ms}")
    private Duration runtimeCacheTtl;

    @Value("${metric-wallet.allocation.runtime-cache-max-size:20000}")
    private long runtimeCacheMaxSize;

    @Value("${metric-wallet.allocation.max-profiles-per-wallet:1}")
    private int maxProfilesPerWallet;

    @Value("${metric-wallet.allocation.direct-live-policy:REQUIRE_MICRO_LIVE}")
    private String directLivePolicy;

    private volatile Cache<String, List<UserCopyAllocationEntity>> activeByWalletCache;
    private volatile Cache<UUID, List<UserCopyAllocationEntity>> activeByUserCache;

    @PostConstruct
    void initRuntimeCaches() {
        Duration ttl = runtimeCacheTtl == null || runtimeCacheTtl.isNegative() || runtimeCacheTtl.isZero()
                ? Duration.ofMillis(1500)
                : runtimeCacheTtl;
        long maxSize = Math.max(100L, runtimeCacheMaxSize);
        activeByWalletCache = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(maxSize)
                .recordStats()
                .build();
        activeByUserCache = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(maxSize)
                .recordStats()
                .build();
        log.info("event=user_copy_allocation.runtime_cache.config ttlMs={} maxSize={} copyImpact=reduces_hot_path_db_reads",
                ttl.toMillis(), maxSize);
    }

    @Override
    @Transactional
    public void syncDistribution(List<MetricaWalletDto> candidates) {
        syncDistribution(candidates, candidates);
    }

    @Override
    @Transactional
    public void syncDistribution(List<MetricaWalletDto> candidates, List<MetricaWalletDto> shadowCandidates) {

        final List<MetricaWalletDto> liveSource = candidates == null ? List.of() : candidates;
        final List<MetricaWalletDto> shadowSource = shadowCandidates == null ? liveSource : shadowCandidates;

        if (liveSource.isEmpty() && shadowSource.isEmpty()) {
            log.debug("event=user_copy_allocation.sync_skipped reason=empty_candidates");
            return;
        }

        final OffsetDateTime now = OffsetDateTime.now();

        final List<UserDetailDto> users = userDetailService.findAllActive();
        if (users == null || users.isEmpty()) {
            log.debug("event=user_copy_allocation.sync_skipped reason=no_active_users");
            return;
        }

        for (UserDetailDto user : users) {
            if (user == null || user.getDetail() == null || user.getUser() == null) continue;

            final UUID idUser = user.getUser().getId();
            if (idUser == null) continue;

            final Integer maxWalletBoxed = user.getDetail().getMaxWallet();
            final int maxWallet = (maxWalletBoxed == null) ? 0 : maxWalletBoxed;

            if (maxWallet <= 0) {
                continue;
            }

            final Map<String, CopySymbolResolution> symbolResolutionByAllocationKey = new HashMap<>();
            final List<MetricaWalletDto> userLiveSource = filterSymbolSpecialistByCapitalAsset(
                    liveSource,
                    user,
                    idUser,
                    "LIVE",
                    symbolResolutionByAllocationKey
            );
            final List<MetricaWalletDto> userShadowSource = shadowSource == liveSource
                    ? userLiveSource
                    : filterSymbolSpecialistByCapitalAsset(shadowSource, user, idUser, "SHADOW", null);

            shadowCopyTradingService.syncShadowAllocations(idUser, userShadowSource, maxWallet, now);

            entityManager.flush();
            entityManager.clear();

            final boolean separateShadowEnabled = shadowCopyTradingService.isSeparateShadowEnabled();
            final Map<String, CopyExecutionDecision> realExecutionByAllocationKey = new HashMap<>();
            final List<MetricaWalletDto> liveCandidates = separateShadowEnabled
                    ? userLiveSource.stream()
                    .filter(dto -> {
                        CopyExecutionDecision decision = resolveCopyExecutionDecision(idUser, dto);
                        String key = allocationKey(dto);
                        if (decision.openable() && key != null) {
                            realExecutionByAllocationKey.put(key, decision);
                        }
                        return decision.openable();
                    })
                    .toList()
                    : userLiveSource;
            final Map<String, LivePauseDecision> pauseByAllocationKey = shadowCopyTradingService.isSeparateShadowEnabled()
                    ? livePauseDecisions(idUser, userShadowSource)
                    : Map.of();
            final Set<String> liveCandidateKeys = liveCandidates.stream()
                    .map(this::allocationKey)
                    .filter(Objects::nonNull)
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
            final BigDecimal targetTotalPct = sumPositivePct(liveCandidates);

            final List<UserCopyAllocationEntity> existingActive =
                    repository.findAllByIdUserAndEndsAtIsNull(idUser);

            final Set<String> blockedAllocationKeys = new HashSet<>();
            for (UserCopyAllocationEntity e : existingActive) {
                if (e == null) continue;
                final String allocationKey = allocationKey(e);
                if (allocationKey == null) continue;
                if (!e.isActive()
                        || e.getStatus() == UserCopyAllocationEntity.Status.DISABLED_MANUAL
                        || (e.getStatus() != UserCopyAllocationEntity.Status.ACTIVE && !liveCandidateKeys.contains(allocationKey))) {
                        blockedAllocationKeys.add(allocationKey);
                }
            }

            final List<MetricaWalletDto> rankedForPersist = liveCandidates.stream()
                    .filter(Objects::nonNull)
                    .filter(dto -> dto.getWallet() != null)
                    .filter(dto -> normalize(dto.getWallet().getIdWallet()) != null)
                    .filter(dto -> !blockedAllocationKeys.contains(allocationKey(dto)))
                    .filter(dto -> safePct(dto.getCapitalShare()).signum() > 0)
                    .sorted(
                            Comparator
                                    .comparing(
                                            (MetricaWalletDto dto) -> safePct(dto.getCapitalShare()),
                                            Comparator.reverseOrder()
                                    )
                                    .thenComparing(
                                            UserCopyAllocationServiceImpl::safeScore,
                                            Comparator.nullsLast(Comparator.reverseOrder())
                                    )
                                    .thenComparing(
                                            dto -> normalize(dto.getWallet().getIdWallet()),
                                            Comparator.nullsLast(String::compareToIgnoreCase)
                                    )
                    )
                    .toList();

            final List<MetricaWalletDto> top = selectTopProfilesForUser(rankedForPersist, maxWallet, idUser);

            final Map<String, Dist> newDist = new LinkedHashMap<>();

            final List<MetricaWalletDto> validTop = new ArrayList<>();
            BigDecimal topTotalPct = ZERO;

            for (MetricaWalletDto dto : top) {
                if (dto == null || dto.getWallet() == null) continue;

                final String walletId = normalize(dto.getWallet().getIdWallet());
                final String allocationKey = allocationKey(dto);
                if (walletId == null || allocationKey == null) continue;

                final BigDecimal pct = safePct(dto.getCapitalShare());
                if (pct.signum() <= 0) continue;

                validTop.add(dto);
                topTotalPct = topTotalPct.add(pct);
            }

            topTotalPct = topTotalPct.setScale(6, RoundingMode.HALF_UP);

            if (validTop.isEmpty() || topTotalPct.signum() <= 0 || targetTotalPct.signum() <= 0) {
                final List<UserCopyAllocationEntity> toSave = new ArrayList<>();
                int closed = 0;
                int paused = 0;

                for (UserCopyAllocationEntity e : existingActive) {
                    if (e == null) continue;
                    if (!e.isActive()) continue;

                    final String allocationKey = allocationKey(e);
                    LivePauseDecision pause = allocationKey == null ? null : pauseByAllocationKey.get(allocationKey);
                    if (pause != null) {
                        applyLivePause(e, pause, now);
                        paused++;
                    } else {
                        e.setStatus(UserCopyAllocationEntity.Status.CLOSED);
                        e.setEndsAt(now);
                        closed++;
                    }
                    e.setUpdatedAt(now);
                    toSave.add(e);
                }

                if (!toSave.isEmpty()) {
                    repository.saveAll(toSave);
                }

                log.debug(
                        "event=user_copy_allocation.sync_ok reasonCode=empty_distribution userId={} closed={} paused={} blocked={} shadowSeparate={}",
                        idUser,
                        closed,
                        paused,
                        blockedAllocationKeys.size(),
                        shadowCopyTradingService.isSeparateShadowEnabled()
                );
                continue;
            }

            final BigDecimal scaleFactor = targetTotalPct.divide(topTotalPct, 18, RoundingMode.HALF_UP);

            BigDecimal accumulated = ZERO;

            for (int i = 0; i < validTop.size(); i++) {
                final MetricaWalletDto dto = validTop.get(i);
                final String walletId = normalize(dto.getWallet().getIdWallet());
                final String allocationKey = allocationKey(dto);
                if (walletId == null || allocationKey == null) continue;
                final BigDecimal originalPct = safePct(dto.getCapitalShare());
                final Integer score = safeScore(dto);

                final boolean isLast = (i == validTop.size() - 1);

                BigDecimal scaledPct;
                if (isLast) {
                    scaledPct = targetTotalPct.subtract(accumulated).setScale(6, RoundingMode.HALF_UP);
                } else {
                    scaledPct = originalPct.multiply(scaleFactor).setScale(6, RoundingMode.HALF_UP);
                    accumulated = accumulated.add(scaledPct);
                }

                if (scaledPct.signum() <= 0) continue;

                final String strategyCode = strategyCode(dto);
                final String scopeType = scopeType(dto);
                final String scopeValue = scopeValue(dto, strategyCode);
                final CopySymbolResolution symbolResolution = symbolResolutionByAllocationKey.get(allocationKey);
                CopyExecutionDecision decision = realExecutionByAllocationKey.get(allocationKey);
                String resolvedExecutionMode = decision == null ? targetExecutionMode(dto) : decision.executionMode();
                newDist.put(allocationKey, new Dist(walletId, strategyCode, strategySlug(dto), strategyLabel(dto), copyMode(dto), sourceEndpoint(dto), rankWithinStrategy(dto), globalRank(dto), strategyScore(dto), scopeType, scopeValue, strategyKey(walletId, strategyCode, scopeType, scopeValue), resolvedExecutionMode, scaledPct, score, symbolResolution));
            }

            final List<String> newWalletIdList = newDist.values().stream()
                    .map(Dist::walletId)
                    .filter(Objects::nonNull)
                    .distinct()
                    .toList();

            final List<UserCopyAllocationEntity> existingForNewWallets =
                    repository.findAllByIdUserAndWalletIdIn(idUser, newWalletIdList);

            final Map<String, UserCopyAllocationEntity> existingByAllocationKey = new HashMap<>();
            for (UserCopyAllocationEntity e : existingForNewWallets) {
                if (e == null) continue;
                final String key = allocationKey(e);
                if (key != null) existingByAllocationKey.put(key, e);
            }

            final List<UserCopyAllocationEntity> toSave =
                    new ArrayList<>(newDist.size() + existingActive.size());

            for (Map.Entry<String, Dist> entry : newDist.entrySet()) {
                final String allocationKey = entry.getKey();
                final Dist d = entry.getValue();
                final String walletId = d.walletId();

                UserCopyAllocationEntity entity = existingByAllocationKey.get(allocationKey);
                if (entity != null
                        && (!entity.isActive()
                        || entity.getEndsAt() != null
                        || entity.getStatus() == UserCopyAllocationEntity.Status.CLOSED)) {
                    entity = null;
                }
                if (entity == null) {
                    final String targetMode = executionModeForTarget(d.targetExecutionMode());
                    entity = UserCopyAllocationEntity.builder()
                            .idUser(idUser)
                            .walletId(walletId)
                            .isActive(true)
                            .executionMode(shadowCopyTradingService.isSeparateShadowEnabled() && !"MICRO_LIVE".equals(targetMode) ? "LIVE" : targetMode)
                            .build();
                }

                if (!entity.isActive()) {
                    continue;
                }
                final boolean reentry = entity.getStatus() != null
                        && entity.getStatus() != UserCopyAllocationEntity.Status.ACTIVE
                        && entity.getStatus() != UserCopyAllocationEntity.Status.DISABLED_MANUAL
                        && entity.getStatus() != UserCopyAllocationEntity.Status.CLOSED;
                CopyModeResolution copyModeResolution = UserCopyAllocationCopyModeResolver.resolve(d.strategyCode(), d.copyMode());
                if (!copyModeResolution.valid()) {
                    log.warn(
                            "event=user_copy_allocation.copy_mode.rejected userId={} walletId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} reasonCode={} executionMode={} decision=SKIP",
                            idUser,
                            walletId,
                            d.strategyCode(),
                            d.scopeType(),
                            d.scopeValue(),
                            safeReason(d.copyMode()),
                            copyModeResolution.copyMode(),
                            copyModeResolution.reasonCode(),
                            d.targetExecutionMode()
                    );
                    continue;
                }
                log.info(
                        "event=user_copy_allocation.copy_mode.resolved userId={} walletId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} reasonCode={} constraintReasonCode={} executionMode={} decision=ALLOW",
                        idUser,
                        walletId,
                        d.strategyCode(),
                        d.scopeType(),
                        d.scopeValue(),
                        safeReason(d.copyMode()),
                        copyModeResolution.copyMode(),
                        copyModeResolution.reasonCode(),
                        copyModeResolution.constraintReasonCode(),
                        d.targetExecutionMode()
                );
                entity.setAllocationPct(reentry ? reentryPct(d.pct()) : d.pct());
                entity.setScore(d.score());
                entity.setCopyStrategyCode(d.strategyCode());
                entity.setCopyStrategySlug(d.strategySlug());
                entity.setCopyStrategyLabel(d.strategyLabel());
                entity.setCopyMode(copyModeResolution.copyMode());
                entity.setStrategySourceEndpoint(d.sourceEndpoint());
                entity.setRankWithinStrategy(d.rankWithinStrategy());
                entity.setGlobalRank(d.globalRank());
                entity.setStrategyScore(d.strategyScore());
                entity.setScopeType(d.scopeType());
                entity.setScopeValue(d.scopeValue());
                entity.setStrategyKey(d.strategyKey());
                entity.setSourceSymbol(d.sourceSymbol());
                entity.setTargetSymbol(d.targetSymbol());
                entity.setCapitalAsset(d.capitalAsset());
                entity.setResolvedQuoteAsset(d.resolvedQuoteAsset());
                entity.setSymbolResolutionStatus(d.symbolResolutionStatus());
                entity.setSymbolResolutionReason(d.symbolResolutionReason());
                if (d.sourceSymbol() != null || d.targetSymbol() != null) {
                    log.info(
                            "user_copy_allocation.created sourceSymbol={} targetSymbol={} executionMode={} userId={} walletId={} strategyCode={} allocationKey={}",
                            d.sourceSymbol(),
                            d.targetSymbol(),
                            d.targetExecutionMode(),
                            idUser,
                            walletId,
                            d.strategyCode(),
                            allocationKey
                    );
                }
                final String targetMode = executionModeForTarget(d.targetExecutionMode());
                if ("MICRO_LIVE".equals(targetMode) || "LIVE".equals(targetMode)) {
                    entity.setExecutionMode(targetMode);
                } else if (!shadowCopyTradingService.isSeparateShadowEnabled() && "SHADOW".equals(targetMode)) {
                    entity.setExecutionMode(targetMode);
                } else if (entity.getExecutionMode() == null || entity.getExecutionMode().isBlank()) {
                    entity.setExecutionMode(shadowCopyTradingService.isSeparateShadowEnabled() ? "LIVE" : normalizedDefaultExecutionMode());
                }
                if (entity.getStatus() == null || entity.getStatus() == UserCopyAllocationEntity.Status.ACTIVE || reentry) {
                    entity.setStatus(UserCopyAllocationEntity.Status.ACTIVE);
                    entity.setStatusReason(reentry ? "shadow_reentry_validated" : entity.getStatusReason());
                    entity.setStatusCooldownUntil(null);
                    entity.setStatusUpdatedAt(now);
                    entity.setEndsAt(null);
                }
                entity.setUpdatedAt(now);
                if (reentry) {
                    log.info("event=shadow_reentry_to_live userId={} walletId={} strategyCode={} allocationId={} allocationPct={} originalPct={} reentryMultiplier={} reasonCode=shadow_reentry_validated copyImpact=live_open_allowed_reduced_capital",
                            idUser, walletId, d.strategyCode(), entity.getId(), entity.getAllocationPct(), d.pct(), clamp01(shadowReentryCapitalMultiplier));
                }

                toSave.add(entity);
            }

            final Set<String> newAllocationKeys = new HashSet<>(newDist.keySet());
            int closed = 0;
            int paused = 0;

            for (UserCopyAllocationEntity e : existingActive) {
                if (e == null) continue;
                if (!e.isActive()) continue;
                if (e.getStatus() != UserCopyAllocationEntity.Status.ACTIVE) continue;

                final String allocationKey = allocationKey(e);
                if (allocationKey == null) continue;
                if (newAllocationKeys.contains(allocationKey)) continue;

                LivePauseDecision pause = pauseByAllocationKey.get(allocationKey);
                if (pause != null) {
                    applyLivePause(e, pause, now);
                    paused++;
                } else {
                    e.setStatus(UserCopyAllocationEntity.Status.CLOSED);
                    e.setEndsAt(now);
                    closed++;
                }
                e.setUpdatedAt(now);

                toSave.add(e);
            }

            repository.saveAllAndFlush(toSave);
            shadowCopyTradingService.linkLiveAllocations(
                    idUser,
                    toSave.stream()
                            .filter(Objects::nonNull)
                            .filter(UserCopyAllocationEntity::isActive)
                            .filter(e -> e.getStatus() == UserCopyAllocationEntity.Status.ACTIVE)
                            .filter(e -> isRealExecutionMode(e.getExecutionMode()))
                            .toList()
            );
            repository.saveAllAndFlush(toSave);

            final BigDecimal persistedTotal = newDist.values().stream()
                    .map(Dist::pct)
                    .reduce(ZERO, BigDecimal::add)
                    .setScale(6, RoundingMode.HALF_UP);

            log.debug(
                    "event=user_copy_allocation.sync_ok userId={} maxWallet={} candidates={} persisted={} closed={} paused={} blocked={} targetTotalPct={} persistedTotal={}",
                    idUser,
                    maxWallet,
                    liveSource.size(),
                    newDist.size(),
                    closed,
                    paused,
                    blockedAllocationKeys.size(),
                    targetTotalPct,
                    persistedTotal
            );
        }

        invalidateRuntimeCaches("sync_distribution");
    }

    @Override
    @Transactional(readOnly = true)
    public List<UserCopyAllocationEntity> getWalletUserId(UUID idUser) {
        if (idUser == null) return List.of();

        return activeByUserCache().get(idUser, this::loadActiveByUser)
                .stream()
                .filter(Objects::nonNull)
                .sorted(Comparator
                        .comparing(UserCopyAllocationEntity::getScore, Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(UserCopyAllocationEntity::getAllocationPct, Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(UserCopyAllocationEntity::getWalletId, Comparator.nullsLast(String::compareToIgnoreCase))
                )
                .toList();
    }


    @Override
    @Transactional(readOnly = true)
    public List<UserCopyAllocationEntity> getActiveAllocationsByWallet(String walletId) {
        final String normalizedWallet = normalize(walletId);
        if (normalizedWallet == null) {
            return List.of();
        }
        return activeByWalletCache().get(normalizedWallet, this::loadActiveByWallet)
                .stream()
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    @Transactional(readOnly = true)
    public List<UserCopyAllocationEntity> getActiveAllocationsForUserWallet(UUID idUser, String walletId) {
        final String normalizedWallet = normalize(walletId);
        if (idUser == null || normalizedWallet == null) {
            return List.of();
        }
        return getActiveAllocationsByWallet(normalizedWallet)
                .stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.equals(e.getIdUser(), idUser))
                .toList();
    }


    @Override
    @Transactional(readOnly = true)
    public Set<UUID> getActiveUserIdsByWallet(String walletId) {
        final String normalizedWallet = normalize(walletId);
        if (normalizedWallet == null) {
            return Set.of();
        }

        return getActiveAllocationsByWallet(normalizedWallet)
                .stream()
                .filter(Objects::nonNull)
                .map(UserCopyAllocationEntity::getIdUser)
                .filter(Objects::nonNull)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }


    @Override
    @Transactional(readOnly = true)
    public Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId, String strategyCode) {
        final String normalizedWallet = normalize(walletId);
        final String normalizedStrategy = copyStrategyRuntimeRouter.strategyCodeOf(UserCopyAllocationEntity.builder().copyStrategyCode(strategyCode).build());
        if (idUser == null || normalizedWallet == null || normalizedStrategy == null) {
            return Optional.empty();
        }
        return getActiveAllocationsForUserWallet(idUser, normalizedWallet).stream()
                .filter(e -> Objects.equals(normalizeStrategy(e.getCopyStrategyCode()), normalizedStrategy))
                .findFirst();
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId) {
        final String normalizedWallet = normalize(walletId);
        if (idUser == null || normalizedWallet == null) {
            return Optional.empty();
        }

        return getActiveAllocationsForUserWallet(idUser, normalizedWallet).stream().findFirst();
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<UserCopyAllocationEntity> findOpenAllocation(UUID idUser, String walletId, String strategyCode) {
        final String normalizedWallet = normalize(walletId);
        final String normalizedStrategy = copyStrategyRuntimeRouter.strategyCodeOf(UserCopyAllocationEntity.builder().copyStrategyCode(strategyCode).build());
        if (idUser == null || normalizedWallet == null || normalizedStrategy == null) {
            return Optional.empty();
        }
        return repository.findOpenAllocationForUserWalletStrategy(idUser, normalizedWallet, normalizedStrategy);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<UserCopyAllocationEntity> findOpenAllocation(UUID idUser, String walletId, String strategyCode, String scopeType, String scopeValue) {
        final String normalizedWallet = normalize(walletId);
        final String normalizedStrategy = copyStrategyRuntimeRouter.strategyCodeOf(UserCopyAllocationEntity.builder().copyStrategyCode(strategyCode).build());
        final String normalizedScopeType = normalizeScopeType(scopeType);
        final String normalizedScopeValue = normalizeScopeValue(scopeValue, normalizedStrategy);
        if (idUser == null || normalizedWallet == null || normalizedStrategy == null) {
            return Optional.empty();
        }
        return repository.findOpenAllocationForUserWalletStrategyScope(
                idUser,
                normalizedWallet,
                normalizedStrategy,
                normalizedScopeType,
                normalizedScopeValue
        );
    }

    @Override
    @Transactional
    public void markGuardBlocked(UUID idUser, String walletId, String strategyCode, String targetStatus, String reason, OffsetDateTime cooldownUntil) {
        final String normalizedWallet = normalize(walletId);
        final String normalizedStrategy = copyStrategyRuntimeRouter.strategyCodeOf(UserCopyAllocationEntity.builder().copyStrategyCode(strategyCode).build());
        if (idUser == null || normalizedWallet == null || normalizedStrategy == null) {
            return;
        }
        UserCopyAllocationEntity entity = repository.findOpenAllocationForUserWalletStrategy(idUser, normalizedWallet, normalizedStrategy).orElse(null);
        if (entity == null || entity.getStatus() == UserCopyAllocationEntity.Status.CLOSED || entity.getStatus() == UserCopyAllocationEntity.Status.DISABLED_MANUAL) {
            return;
        }
        UserCopyAllocationEntity.Status nextStatus = parseStatus(targetStatus);
        if (nextStatus == UserCopyAllocationEntity.Status.ACTIVE || nextStatus == UserCopyAllocationEntity.Status.CLOSED) {
            nextStatus = UserCopyAllocationEntity.Status.EXIT_ONLY;
        }
        final OffsetDateTime now = OffsetDateTime.now();
        if (entity.getStatus() == nextStatus && Objects.equals(entity.getStatusReason(), reason)) {
            return;
        }
        entity.setStatus(nextStatus);
        entity.setStatusReason(safeReason(reason));
        entity.setStatusCooldownUntil(cooldownUntil);
        entity.setStatusUpdatedAt(now);
        entity.setUpdatedAt(now);
        repository.saveAndFlush(entity);
        invalidateRuntimeCaches("guard_blocked");
        log.warn("event=user_copy_allocation.status_guard_blocked userId={} walletId={} strategyCode={} status={} reasonCode={} cooldownUntil={}",
                idUser, normalizedWallet, normalizedStrategy, nextStatus, safeReason(reason), cooldownUntil);
    }

    private List<UserCopyAllocationEntity> loadActiveByUser(UUID idUser) {
        if (idUser == null) {
            return List.of();
        }
        List<UserCopyAllocationEntity> loaded = repository.findAllByIdUserAndEndsAtIsNull(idUser)
                .stream()
                .filter(Objects::nonNull)
                .filter(UserCopyAllocationEntity::isActive)
                .filter(e -> e.getStatus() == UserCopyAllocationEntity.Status.ACTIVE)
                .toList();
        return List.copyOf(loaded);
    }

    private List<UserCopyAllocationEntity> loadActiveByWallet(String walletId) {
        String normalizedWallet = normalize(walletId);
        if (normalizedWallet == null) {
            return List.of();
        }
        List<UserCopyAllocationEntity> loaded = repository.findActiveByWalletId(normalizedWallet)
                .stream()
                .filter(Objects::nonNull)
                .toList();
        return List.copyOf(loaded);
    }

    private Cache<String, List<UserCopyAllocationEntity>> activeByWalletCache() {
        Cache<String, List<UserCopyAllocationEntity>> cache = activeByWalletCache;
        if (cache != null) {
            return cache;
        }
        synchronized (this) {
            if (activeByWalletCache == null || activeByUserCache == null) {
                initRuntimeCaches();
            }
            return activeByWalletCache;
        }
    }

    private Cache<UUID, List<UserCopyAllocationEntity>> activeByUserCache() {
        Cache<UUID, List<UserCopyAllocationEntity>> cache = activeByUserCache;
        if (cache != null) {
            return cache;
        }
        synchronized (this) {
            if (activeByWalletCache == null || activeByUserCache == null) {
                initRuntimeCaches();
            }
            return activeByUserCache;
        }
    }

    private void invalidateRuntimeCaches(String reason) {
        Cache<String, List<UserCopyAllocationEntity>> walletCache = activeByWalletCache;
        Cache<UUID, List<UserCopyAllocationEntity>> userCache = activeByUserCache;
        if (walletCache != null) {
            walletCache.invalidateAll();
        }
        if (userCache != null) {
            userCache.invalidateAll();
        }
        log.debug("event=user_copy_allocation.runtime_cache.invalidate reasonCode={}", reason);
    }

    private UserCopyAllocationEntity.Status parseStatus(String raw) {
        if (raw == null || raw.isBlank()) {
            return UserCopyAllocationEntity.Status.EXIT_ONLY;
        }
        try {
            return UserCopyAllocationEntity.Status.valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            return UserCopyAllocationEntity.Status.EXIT_ONLY;
        }
    }

    private static String safeReason(String reason) {
        if (reason == null || reason.isBlank()) return null;
        String clean = reason.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim();
        return clean.length() > 160 ? clean.substring(0, 160) : clean;
    }

    private List<MetricaWalletDto> filterSymbolSpecialistByCapitalAsset(
            List<MetricaWalletDto> source,
            UserDetailDto user,
            UUID idUser,
            String syncPlane,
            Map<String, CopySymbolResolution> resolvedByAllocationKey
    ) {
        if (source == null || source.isEmpty()) {
            return List.of();
        }
        final String capitalAsset = capitalAssetForResolution(user == null || user.getDetail() == null
                ? null
                : user.getDetail().getCapitalAsset());
        List<MetricaWalletDto> out = new ArrayList<>(source.size());
        for (MetricaWalletDto dto : source) {
            if (dto == null) {
                continue;
            }
            final String strategyCode = strategyCode(dto);
            if (!"SYMBOL_SPECIALIST".equals(strategyCode)) {
                out.add(dto);
                continue;
            }
            final String symbol = normalizeSymbolScope(scopeValue(dto, strategyCode));
            final CopySymbolResolution resolution = copySymbolResolver.resolve(symbol, capitalAsset);
            if (!resolution.resolved()) {
                log.warn(
                        "event=user_copy_allocation.symbol_specialist_skipped userId={} walletId={} strategyCode={} sourceSymbol={} targetSymbol={} capitalAsset={} reasonCode={} syncPlane={} copyImpact=allocation_skipped",
                        idUser,
                        dto.getWallet() == null ? null : dto.getWallet().getIdWallet(),
                        strategyCode,
                        symbol,
                        resolution.targetSymbol(),
                        capitalAsset,
                        resolution.reasonCode(),
                        syncPlane
                );
                continue;
            }
            final String allocationKey = allocationKey(dto);
            if (resolvedByAllocationKey != null && allocationKey != null) {
                resolvedByAllocationKey.put(allocationKey, resolution);
            }
            out.add(dto);
        }
        return out;
    }

    private Map<String, LivePauseDecision> livePauseDecisions(UUID idUser, List<MetricaWalletDto> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return Map.of();
        }
        Map<String, LivePauseDecision> out = new HashMap<>();
        for (MetricaWalletDto dto : candidates) {
            if (dto == null
                    || shadowCopyTradingService.isLivePromotable(idUser, dto)
                    || shadowCopyTradingService.isMicroLivePromotable(idUser, dto)) {
                continue;
            }
            String key = allocationKey(dto);
            if (key == null) {
                continue;
            }
            out.putIfAbsent(key, livePauseDecision(dto));
        }
        return out;
    }

    private CopyExecutionDecision resolveCopyExecutionDecision(UUID idUser, MetricaWalletDto dto) {
        if (dto == null) {
            return CopyExecutionDecision.blocked("NULL_CANDIDATE");
        }
        String target = targetExecutionMode(dto);
        if ("SHADOW".equals(target)) {
            return CopyExecutionDecision.blocked("TARGET_SHADOW");
        }
        if ("MICRO_LIVE".equals(target)) {
            return shadowCopyTradingService.isMicroLivePromotable(idUser, dto)
                    ? CopyExecutionDecision.open("MICRO_LIVE")
                    : CopyExecutionDecision.blocked("MICRO_LIVE_NOT_PROMOTABLE");
        }
        if ("LIVE".equals(target)) {
            boolean livePromotable = shadowCopyTradingService.isLivePromotable(idUser, dto);
            DirectLivePolicyDecision policyDecision = directLivePolicyDecision(idUser, dto, target, livePromotable);
            if (policyDecision.allowed()) {
                return CopyExecutionDecision.open("LIVE");
            }
            return shadowCopyTradingService.isMicroLivePromotable(idUser, dto)
                    ? CopyExecutionDecision.open("MICRO_LIVE")
                    : CopyExecutionDecision.blocked(policyDecision.reasonCode());
        }
        boolean livePromotable = shadowCopyTradingService.isLivePromotable(idUser, dto);
        DirectLivePolicyDecision policyDecision = directLivePolicyDecision(idUser, dto, target, livePromotable);
        if (policyDecision.allowed()) {
            return CopyExecutionDecision.open("LIVE");
        }
        return shadowCopyTradingService.isMicroLivePromotable(idUser, dto)
                ? CopyExecutionDecision.open("MICRO_LIVE")
                : CopyExecutionDecision.blocked("NO_REAL_EXECUTION_MODE");
    }

    private DirectLivePolicyDecision directLivePolicyDecision(UUID idUser, MetricaWalletDto dto, String requestedTarget, boolean livePromotable) {
        String policy = normalizeDirectLivePolicy(directLivePolicy);
        String walletId = walletId(dto);
        String strategyCode = strategyCode(dto);
        String scopeType = scopeType(dto);
        String scopeValue = scopeValue(dto, strategyCode);
        if (!"LIVE".equals(normalizeExecutionModeTarget(requestedTarget))) {
            return new DirectLivePolicyDecision(false, "MICRO_LIVE_REQUIRED_BY_POLICY", policy);
        }
        if (!livePromotable) {
            log.info(
                    "event=copy.promotion.direct_live.policy_checked userId={} walletId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=LIVE decision=REJECT reasonCode=LIVE_NOT_READY_FROM_SHADOW policy={} requestedTarget={}",
                    idUser,
                    walletId,
                    strategyCode,
                    scopeType,
                    scopeValue,
                    safeReason(copyMode(dto)),
                    null,
                    policy,
                    requestedTarget
            );
            return new DirectLivePolicyDecision(false, "LIVE_NOT_READY_FROM_SHADOW", policy);
        }
        if (!"ALLOW_DIRECT_LIVE_FOR_LIVE_READY".equals(policy)) {
            log.info(
                    "event=copy.promotion.direct_live.policy_checked userId={} walletId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=LIVE decision=REJECT reasonCode=MICRO_LIVE_REQUIRED_BY_POLICY policy={} requestedTarget={}",
                    idUser,
                    walletId,
                    strategyCode,
                    scopeType,
                    scopeValue,
                    safeReason(copyMode(dto)),
                    null,
                    policy,
                    requestedTarget
            );
            return new DirectLivePolicyDecision(false, "MICRO_LIVE_REQUIRED_BY_POLICY", policy);
        }
        log.info(
                "event=copy.promotion.direct_live.policy_checked userId={} walletId={} strategyCode={} scopeType={} scopeValue={} sourceCopyMode={} resolvedCopyMode={} executionMode=LIVE decision=ALLOW reasonCode=DIRECT_LIVE_ALLOWED_BY_POLICY policy={} requestedTarget={}",
                idUser,
                walletId,
                strategyCode,
                scopeType,
                scopeValue,
                safeReason(copyMode(dto)),
                null,
                policy,
                requestedTarget
        );
        return new DirectLivePolicyDecision(true, "DIRECT_LIVE_ALLOWED_BY_POLICY", policy);
    }

    private LivePauseDecision livePauseDecision(MetricaWalletDto dto) {
        MetricaWalletDto.CopyGuardDto guard = copyGuard(dto);
        String action = guard == null || guard.getAction() == null ? "SHADOW_VALIDATION_FAILED" : normalizeStrategy(guard.getAction());
        String status = guard == null || guard.getStatus() == null ? "" : normalizeStrategy(guard.getStatus());
        UserCopyAllocationEntity.Status target = action.contains("NEGATIVE")
                ? UserCopyAllocationEntity.Status.PAUSED_BY_NEGATIVE_PNL
                : UserCopyAllocationEntity.Status.PAUSED_BY_RISK;
        if ("PAUSE_OPEN".equals(action) || "SHADOW_ONLY".equals(action) || "DATA_RISK".equals(status)) {
            target = UserCopyAllocationEntity.Status.PAUSED_BY_RISK;
        }
        String reason = "shadow_live_validation_failed action=" + action + " status=" + status;
        return new LivePauseDecision(target, reason, action);
    }

    private List<MetricaWalletDto> selectTopProfilesForUser(List<MetricaWalletDto> rankedCandidates, int maxProfiles, UUID idUser) {
        if (rankedCandidates == null || rankedCandidates.isEmpty() || maxProfiles <= 0) {
            return List.of();
        }
        int perWalletLimit = Math.max(0, maxProfilesPerWallet);
        List<MetricaWalletDto> selected = new ArrayList<>(Math.min(maxProfiles, rankedCandidates.size()));
        Map<String, Integer> selectedByWallet = new HashMap<>();
        for (MetricaWalletDto dto : rankedCandidates) {
            if (dto == null || dto.getWallet() == null) {
                continue;
            }
            String walletId = normalize(dto.getWallet().getIdWallet());
            if (walletId == null) {
                continue;
            }
            int walletCount = selectedByWallet.getOrDefault(walletId, 0);
            if (perWalletLimit > 0 && walletCount >= perWalletLimit) {
                log.debug(
                        "event=copy_profile_strategy_live_blocked userId={} walletId={} strategyCode={} reasonCode=MAX_PROFILES_PER_WALLET maxProfilesPerWallet={} liveImpact=LIVE_SELECTION_SKIPPED",
                        idUser,
                        walletId,
                        strategyCode(dto),
                        perWalletLimit
                );
                continue;
            }
            selected.add(dto);
            selectedByWallet.put(walletId, walletCount + 1);
            if (selected.size() >= maxProfiles) {
                break;
            }
        }
        return selected;
    }

    private void applyLivePause(UserCopyAllocationEntity entity, LivePauseDecision pause, OffsetDateTime now) {
        String safeReason = safeReason(pause.reason());
        boolean changed = entity.getStatus() != pause.status() || !Objects.equals(entity.getStatusReason(), safeReason);
        entity.setStatus(pause.status());
        entity.setStatusReason(safeReason);
        entity.setStatusCooldownUntil(null);
        entity.setStatusUpdatedAt(now);
        entity.setEndsAt(null);
        if (changed) {
            log.warn(
                    "event=live_paused_shadow_continues reasonCode={} userId={} walletId={} strategyCode={} allocationId={} status={} copyImpact=no_new_live_open shadowContinues=true",
                    pause.reasonCode(),
                    entity.getIdUser(),
                    entity.getWalletId(),
                    entity.getCopyStrategyCode(),
                    entity.getId(),
                    pause.status()
            );
        }
    }

    private String allocationKey(MetricaWalletDto dto) {
        if (dto == null || dto.getWallet() == null) return null;
        String strategyCode = strategyCode(dto);
        return allocationKey(dto.getWallet().getIdWallet(), strategyCode, scopeType(dto), scopeValue(dto, strategyCode));
    }

    private static String walletId(MetricaWalletDto dto) {
        return dto == null || dto.getWallet() == null ? null : normalize(dto.getWallet().getIdWallet());
    }

    private String allocationKey(UserCopyAllocationEntity entity) {
        return copyStrategyRuntimeRouter.allocationKey(entity);
    }

    private String allocationKey(String walletId, String strategyCode, String scopeType, String scopeValue) {
        return copyStrategyRuntimeRouter.allocationKey(walletId, strategyCode, scopeType, scopeValue);
    }

    private String strategyCode(MetricaWalletDto dto) {
        return copyStrategyRuntimeRouter.strategyCodeOf(dto);
    }

    private static String strategySlug(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getStrategySlug();
    }

    private static String strategyLabel(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getStrategyLabel();
    }

    private static String copyMode(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getCopyMode();
    }

    private static String sourceEndpoint(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getSourceEndpoint();
    }

    private static Integer rankWithinStrategy(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getRankWithinStrategy();
    }

    private static Integer globalRank(MetricaWalletDto dto) {
        return dto == null || dto.getStrategy() == null ? null : dto.getStrategy().getGlobalRank();
    }

    private static BigDecimal strategyScore(MetricaWalletDto dto) {
        if (dto == null || dto.getStrategy() == null || dto.getStrategy().getScore() == null) return null;
        return BigDecimal.valueOf(dto.getStrategy().getScore()).setScale(6, RoundingMode.HALF_UP);
    }

    private static String scopeType(MetricaWalletDto dto) {
        String fromBreakdown = dto != null && dto.getWallet() != null && dto.getWallet().getCountOperationBreakdown() != null
                ? dto.getWallet().getCountOperationBreakdown().getScopeType()
                : null;
        String fromJewel = dto != null && dto.getRealJewel() != null ? dto.getRealJewel().getScopeType() : null;
        return normalizeScopeType(firstNonBlank(fromBreakdown, fromJewel, "strategy"));
    }

    private static String scopeValue(MetricaWalletDto dto, String strategyCode) {
        String fromBreakdown = dto != null && dto.getWallet() != null && dto.getWallet().getCountOperationBreakdown() != null
                ? dto.getWallet().getCountOperationBreakdown().getScopeValue()
                : null;
        String fromJewel = dto != null && dto.getRealJewel() != null ? dto.getRealJewel().getScopeValue() : null;
        return normalizeScopeValue(firstNonBlank(fromBreakdown, fromJewel, strategyCode), strategyCode);
    }

    private static String strategyKey(String walletId, String strategyCode, String scopeType, String scopeValue) {
        return normalize(walletId) + "|" + normalizeStrategy(strategyCode) + "|" + normalizeScopeType(scopeType) + "|" + normalizeScopeValue(scopeValue, strategyCode);
    }

    private static String targetExecutionMode(MetricaWalletDto dto) {
        String recommended = dto != null && dto.getRealJewel() != null
                ? dto.getRealJewel().getRecommendedExecutionMode()
                : null;
        String recommendedMode = normalizeExecutionModeTarget(recommended);
        if ("SHADOW".equals(recommendedMode) || "MICRO_LIVE".equals(recommendedMode) || "LIVE".equals(recommendedMode)) {
            return recommendedMode;
        }
        MetricaWalletDto.CopyGuardDto guard = copyGuard(dto);
        if (guard == null || guard.getTargetExecutionMode() == null) {
            return "KEEP";
        }
        String value = normalizeExecutionModeTarget(guard.getTargetExecutionMode());
        return "SHADOW".equals(value) || "MICRO_LIVE".equals(value) || "LIVE".equals(value) ? value : "KEEP";
    }

    private static MetricaWalletDto.CopyGuardDto copyGuard(MetricaWalletDto dto) {
        if (dto == null) {
            return null;
        }
        if (dto.getRealJewel() != null && dto.getRealJewel().getCopyGuard() != null) {
            return dto.getRealJewel().getCopyGuard();
        }
        if (dto.getStrategy() != null && dto.getStrategy().getCopyGuard() != null) {
            return dto.getStrategy().getCopyGuard();
        }
        if (dto.getStrategy() != null
                && dto.getStrategy().getRiskAdjustedCapitalEfficiency() != null
                && dto.getStrategy().getRiskAdjustedCapitalEfficiency().getCopyGuard() != null) {
            return dto.getStrategy().getRiskAdjustedCapitalEfficiency().getCopyGuard();
        }
        return null;
    }

    private static Integer safeScore(MetricaWalletDto dto) {
        if (dto == null || dto.getScoring() == null) return null;
        return dto.getScoring().getDecisionMetricConservative();
    }

    private static BigDecimal safePct(double capitalShare) {
        if (Double.isNaN(capitalShare) || Double.isInfinite(capitalShare)) {
            return ZERO;
        }
        BigDecimal bd = BigDecimal.valueOf(Math.max(0.0, capitalShare));
        return bd.setScale(6, RoundingMode.HALF_UP);
    }

    private static String normalize(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t.toLowerCase();
    }

    private static String normalizeStrategy(String s) {
        if (s == null || s.isBlank()) return "MOVEMENT_ALL";
        return s.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
    }

    private static String normalizeScopeType(String s) {
        if (s == null || s.isBlank()) return "strategy";
        return s.trim().toLowerCase(java.util.Locale.ROOT);
    }

    private static String normalizeScopeValue(String s, String strategyCode) {
        if (s == null || s.isBlank()) return normalizeStrategy(strategyCode);
        return s.trim();
    }

    private static String capitalAssetForResolution(String value) {
        if (value == null || value.isBlank()) {
            return FuturesCapitalAsset.defaultAsset().name();
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizeSymbolScope(String value) {
        if (value == null || value.isBlank()) return null;
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        if ("ALL".equals(normalized) || "DEFAULT".equals(normalized) || "SYMBOL_SPECIALIST".equals(normalized)) {
            return null;
        }
        return normalized;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    private String normalizedDefaultExecutionMode() {
        String value = defaultExecutionMode == null ? "" : defaultExecutionMode.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        if ("LIVE".equals(value)) return "LIVE";
        if ("MICRO_LIVE".equals(value)) return "MICRO_LIVE";
        return "SHADOW";
    }

    private String executionModeForTarget(String targetExecutionMode) {
        String value = normalizeExecutionModeTarget(targetExecutionMode);
        if ("SHADOW".equals(value) || "MICRO_LIVE".equals(value) || "LIVE".equals(value)) return value;
        return normalizedDefaultExecutionMode();
    }

    private static boolean isRealExecutionMode(String executionMode) {
        String value = normalizeExecutionModeTarget(executionMode);
        return "LIVE".equals(value) || "MICRO_LIVE".equals(value);
    }

    private static String normalizeExecutionModeTarget(String value) {
        if (value == null || value.isBlank()) return "KEEP";
        String normalized = value.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        if ("SHADOW".equals(normalized) || "MICRO_LIVE".equals(normalized) || "LIVE".equals(normalized)) {
            return normalized;
        }
        return "KEEP";
    }

    private static String normalizeDirectLivePolicy(String value) {
        if (value == null || value.isBlank()) {
            return "REQUIRE_MICRO_LIVE";
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        return "ALLOW_DIRECT_LIVE_FOR_LIVE_READY".equals(normalized)
                ? "ALLOW_DIRECT_LIVE_FOR_LIVE_READY"
                : "REQUIRE_MICRO_LIVE";
    }

    private BigDecimal reentryPct(BigDecimal pct) {
        if (pct == null) return ZERO.setScale(6, RoundingMode.HALF_UP);
        return pct.multiply(BigDecimal.valueOf(clamp01(shadowReentryCapitalMultiplier))).setScale(6, RoundingMode.HALF_UP);
    }

    private static double clamp01(double value) {
        if (!Double.isFinite(value)) return 0.0;
        return Math.max(0.0, Math.min(1.0, value));
    }

    private record Dist(
            String walletId,
            String strategyCode,
            String strategySlug,
            String strategyLabel,
            String copyMode,
            String sourceEndpoint,
            Integer rankWithinStrategy,
            Integer globalRank,
            BigDecimal strategyScore,
            String scopeType,
            String scopeValue,
            String strategyKey,
            String targetExecutionMode,
            BigDecimal pct,
            Integer score,
            CopySymbolResolution symbolResolution
    ) {
        Dist {
            Objects.requireNonNull(walletId, "walletId");
            Objects.requireNonNull(strategyCode, "strategyCode");
            Objects.requireNonNull(pct, "pct");
        }

        String sourceSymbol() {
            return symbolResolution == null ? null : symbolResolution.sourceSymbol();
        }

        String targetSymbol() {
            return symbolResolution == null ? null : symbolResolution.targetSymbol();
        }

        String capitalAsset() {
            return symbolResolution == null ? null : symbolResolution.capitalAsset();
        }

        String resolvedQuoteAsset() {
            return symbolResolution == null ? null : symbolResolution.quoteAsset();
        }

        String symbolResolutionStatus() {
            return symbolResolution == null ? null : (symbolResolution.resolved() ? "RESOLVED" : "SKIPPED");
        }

        String symbolResolutionReason() {
            return symbolResolution == null ? null : symbolResolution.reasonCode();
        }
    }

    private record CopyExecutionDecision(String executionMode, boolean openable, String reasonCode) {

        private static CopyExecutionDecision open(String executionMode) {
            return new CopyExecutionDecision(executionMode, true, null);
        }

        private static CopyExecutionDecision blocked(String reasonCode) {
            return new CopyExecutionDecision("SHADOW", false, reasonCode);
        }
    }

    private record DirectLivePolicyDecision(
            boolean allowed,
            String reasonCode,
            String policy
    ) {
    }

    private record LivePauseDecision(
            UserCopyAllocationEntity.Status status,
            String reason,
            String reasonCode
    ) {}

    private static BigDecimal sumPositivePct(List<MetricaWalletDto> wallets) {
        if (wallets == null || wallets.isEmpty()) return ZERO.setScale(6, RoundingMode.HALF_UP);

        return wallets.stream()
                .filter(Objects::nonNull)
                .filter(dto -> dto.getWallet() != null)
                .map(dto -> safePct(dto.getCapitalShare()))
                .filter(pct -> pct.signum() > 0)
                .reduce(ZERO, BigDecimal::add)
                .setScale(6, RoundingMode.HALF_UP);
    }
}
