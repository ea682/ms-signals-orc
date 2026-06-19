package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailService;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
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

        if (candidates == null || candidates.isEmpty()) {
            log.debug("event=user_copy_allocation.sync_skipped reason=empty_candidates");
            return;
        }

        final OffsetDateTime now = OffsetDateTime.now();

        final List<UserDetailDto> users = userDetailService.findAllActive();
        if (users == null || users.isEmpty()) {
            log.debug("event=user_copy_allocation.sync_skipped reason=no_active_users");
            return;
        }

        final List<MetricaWalletDto> liveCandidates = shadowCopyTradingService.isSeparateShadowEnabled()
                ? candidates.stream().filter(shadowCopyTradingService::isLivePromotable).toList()
                : candidates;
        final Map<String, LivePauseDecision> pauseByAllocationKey = shadowCopyTradingService.isSeparateShadowEnabled()
                ? livePauseDecisions(candidates)
                : Map.of();
        final Set<String> liveCandidateKeys = liveCandidates.stream()
                .map(this::allocationKey)
                .filter(Objects::nonNull)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        final BigDecimal targetTotalPct = sumPositivePct(liveCandidates);

        for (UserDetailDto user : users) {
            if (user == null || user.getDetail() == null || user.getUser() == null) continue;

            final UUID idUser = user.getUser().getId();
            if (idUser == null) continue;

            final Integer maxWalletBoxed = user.getDetail().getMaxWallet();
            final int maxWallet = (maxWalletBoxed == null) ? 0 : maxWalletBoxed;

            if (maxWallet <= 0) {
                continue;
            }

            shadowCopyTradingService.syncShadowAllocations(idUser, candidates, maxWallet, now);

            entityManager.flush();
            entityManager.clear();
            final List<UserCopyAllocationEntity> existingActive =
                    repository.findAllByIdUserAndEndsAtIsNull(idUser);

            final Set<String> blockedAllocationKeys = new HashSet<>();
            for (UserCopyAllocationEntity e : existingActive) {
                if (e == null) continue;
                final String allocationKey = allocationKey(e.getWalletId(), e.getCopyStrategyCode());
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

            final List<MetricaWalletDto> top = (rankedForPersist.size() <= maxWallet)
                    ? rankedForPersist
                    : rankedForPersist.subList(0, maxWallet);

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

                    final String allocationKey = allocationKey(e.getWalletId(), e.getCopyStrategyCode());
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
                newDist.put(allocationKey, new Dist(walletId, strategyCode, strategySlug(dto), strategyLabel(dto), copyMode(dto), sourceEndpoint(dto), rankWithinStrategy(dto), globalRank(dto), strategyScore(dto), scopeType, scopeValue, strategyKey(walletId, strategyCode, scopeType, scopeValue), targetExecutionMode(dto), scaledPct, score));
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
                final String key = allocationKey(e.getWalletId(), e.getCopyStrategyCode());
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
                    entity = UserCopyAllocationEntity.builder()
                            .idUser(idUser)
                            .walletId(walletId)
                            .isActive(true)
                            .executionMode(shadowCopyTradingService.isSeparateShadowEnabled() ? "LIVE" : executionModeForTarget(d.targetExecutionMode()))
                            .build();
                }

                if (!entity.isActive()) {
                    continue;
                }
                final boolean reentry = entity.getStatus() != null
                        && entity.getStatus() != UserCopyAllocationEntity.Status.ACTIVE
                        && entity.getStatus() != UserCopyAllocationEntity.Status.DISABLED_MANUAL
                        && entity.getStatus() != UserCopyAllocationEntity.Status.CLOSED;
                entity.setAllocationPct(reentry ? reentryPct(d.pct()) : d.pct());
                entity.setScore(d.score());
                entity.setCopyStrategyCode(d.strategyCode());
                entity.setCopyStrategySlug(d.strategySlug());
                entity.setCopyStrategyLabel(d.strategyLabel());
                entity.setCopyMode(d.copyMode());
                entity.setStrategySourceEndpoint(d.sourceEndpoint());
                entity.setRankWithinStrategy(d.rankWithinStrategy());
                entity.setGlobalRank(d.globalRank());
                entity.setStrategyScore(d.strategyScore());
                entity.setScopeType(d.scopeType());
                entity.setScopeValue(d.scopeValue());
                entity.setStrategyKey(d.strategyKey());
                if (!shadowCopyTradingService.isSeparateShadowEnabled() && "SHADOW".equals(d.targetExecutionMode())) {
                    entity.setExecutionMode("SHADOW");
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

                final String allocationKey = allocationKey(e.getWalletId(), e.getCopyStrategyCode());
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
                            .filter(e -> "LIVE".equalsIgnoreCase(e.getExecutionMode()))
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
                    candidates.size(),
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

    private Map<String, LivePauseDecision> livePauseDecisions(List<MetricaWalletDto> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return Map.of();
        }
        Map<String, LivePauseDecision> out = new HashMap<>();
        for (MetricaWalletDto dto : candidates) {
            if (dto == null || shadowCopyTradingService.isLivePromotable(dto)) {
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
        return allocationKey(dto.getWallet().getIdWallet(), strategyCode(dto));
    }

    private String allocationKey(String walletId, String strategyCode) {
        return copyStrategyRuntimeRouter.allocationKey(walletId, strategyCode);
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
        return normalizeScopeValue(firstNonBlank(fromBreakdown, fromJewel, strategyCode));
    }

    private static String strategyKey(String walletId, String strategyCode, String scopeType, String scopeValue) {
        return normalize(walletId) + "|" + normalizeStrategy(strategyCode) + "|" + normalizeScopeType(scopeType) + "|" + normalizeScopeValue(scopeValue);
    }

    private static String targetExecutionMode(MetricaWalletDto dto) {
        MetricaWalletDto.CopyGuardDto guard = copyGuard(dto);
        if (guard == null || guard.getTargetExecutionMode() == null) {
            return "KEEP";
        }
        String value = guard.getTargetExecutionMode().trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        return "SHADOW".equals(value) ? "SHADOW" : "KEEP";
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

    private static String normalizeScopeValue(String s) {
        if (s == null || s.isBlank()) return "default";
        return s.trim();
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
        return "LIVE".equals(value) ? "LIVE" : "SHADOW";
    }

    private String executionModeForTarget(String targetExecutionMode) {
        return "SHADOW".equals(targetExecutionMode) ? "SHADOW" : normalizedDefaultExecutionMode();
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
            Integer score
    ) {
        Dist {
            Objects.requireNonNull(walletId, "walletId");
            Objects.requireNonNull(strategyCode, "strategyCode");
            Objects.requireNonNull(pct, "pct");
        }
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
