package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailService;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
    @PersistenceContext
    private EntityManager entityManager;

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

        final BigDecimal targetTotalPct = sumPositivePct(candidates);

        for (UserDetailDto user : users) {
            if (user == null || user.getDetail() == null || user.getUser() == null) continue;

            final UUID idUser = user.getUser().getId();
            if (idUser == null) continue;

            final Integer maxWalletBoxed = user.getDetail().getMaxWallet();
            final int maxWallet = (maxWalletBoxed == null) ? 0 : maxWalletBoxed;

            if (maxWallet <= 0) {
                continue;
            }

            entityManager.flush();
            entityManager.clear();
            final List<UserCopyAllocationEntity> existingActive =
                    repository.findAllByIdUserAndEndsAtIsNull(idUser);

            final Set<String> blockedAllocationKeys = new HashSet<>();
            for (UserCopyAllocationEntity e : existingActive) {
                if (e == null) continue;
                if (!e.isActive()) {
                    final String allocationKey = allocationKey(e.getWalletId(), e.getCopyStrategyCode());
                    if (allocationKey != null) {
                        blockedAllocationKeys.add(allocationKey);
                    }
                }
            }

            final List<MetricaWalletDto> rankedForPersist = candidates.stream()
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
                final List<UserCopyAllocationEntity> toClose = new ArrayList<>();

                for (UserCopyAllocationEntity e : existingActive) {
                    if (e == null) continue;
                    if (!e.isActive()) continue;

                    e.setStatus(UserCopyAllocationEntity.Status.CLOSED);
                    e.setEndsAt(now);
                    e.setUpdatedAt(now);
                    toClose.add(e);
                }

                if (!toClose.isEmpty()) {
                    repository.saveAll(toClose);
                }

                log.debug(
                        "event=user_copy_allocation.sync_ok reason=empty_distribution user={} closed={} blocked={}",
                        idUser,
                        toClose.size(),
                        blockedAllocationKeys.size()
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

                newDist.put(allocationKey, new Dist(walletId, strategyCode(dto), strategySlug(dto), strategyLabel(dto), copyMode(dto), sourceEndpoint(dto), rankWithinStrategy(dto), globalRank(dto), strategyScore(dto), scaledPct, score));
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
                if (entity == null) {
                    entity = UserCopyAllocationEntity.builder()
                            .idUser(idUser)
                            .walletId(walletId)
                            .isActive(true)
                            .build();
                }

                if (!entity.isActive()) {
                    continue;
                }
                entity.setAllocationPct(d.pct());
                entity.setScore(d.score());
                entity.setCopyStrategyCode(d.strategyCode());
                entity.setCopyStrategySlug(d.strategySlug());
                entity.setCopyStrategyLabel(d.strategyLabel());
                entity.setCopyMode(d.copyMode());
                entity.setStrategySourceEndpoint(d.sourceEndpoint());
                entity.setRankWithinStrategy(d.rankWithinStrategy());
                entity.setGlobalRank(d.globalRank());
                entity.setStrategyScore(d.strategyScore());
                entity.setStatus(UserCopyAllocationEntity.Status.ACTIVE);
                entity.setEndsAt(null);
                entity.setUpdatedAt(now);

                toSave.add(entity);
            }

            final Set<String> newAllocationKeys = new HashSet<>(newDist.keySet());
            int closed = 0;

            for (UserCopyAllocationEntity e : existingActive) {
                if (e == null) continue;
                if (!e.isActive()) continue;

                final String allocationKey = allocationKey(e.getWalletId(), e.getCopyStrategyCode());
                if (allocationKey == null) continue;
                if (newAllocationKeys.contains(allocationKey)) continue;

                e.setStatus(UserCopyAllocationEntity.Status.CLOSED);
                e.setEndsAt(now);
                e.setUpdatedAt(now);

                toSave.add(e);
                closed++;
            }

            repository.saveAllAndFlush(toSave);

            final BigDecimal persistedTotal = newDist.values().stream()
                    .map(Dist::pct)
                    .reduce(ZERO, BigDecimal::add)
                    .setScale(6, RoundingMode.HALF_UP);

            log.debug(
                    "event=user_copy_allocation.sync_ok user={} maxWallet={} candidates={} persisted={} closed={} blocked={} targetTotalPct={} persistedTotal={}",
                    idUser,
                    maxWallet,
                    candidates.size(),
                    newDist.size(),
                    closed,
                    blockedAllocationKeys.size(),
                    targetTotalPct,
                    persistedTotal
            );
        }
    }

    @Override
    @Transactional(readOnly = true)
    public List<UserCopyAllocationEntity> getWalletUserId(UUID idUser) {
        if (idUser == null) return List.of();

        return repository.findAllByIdUserAndEndsAtIsNull(idUser)
                .stream()
                .filter(Objects::nonNull)
                .filter(UserCopyAllocationEntity::isActive)
                .filter(e -> e.getStatus() == UserCopyAllocationEntity.Status.ACTIVE)

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
        return repository.findActiveByWalletId(normalizedWallet)
                .stream()
                .filter(Objects::nonNull)
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
        return repository.findActiveAllocationForUserWalletStrategy(
                idUser,
                normalizedWallet,
                normalizedStrategy,
                UserCopyAllocationEntity.Status.ACTIVE
        );
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId) {
        final String normalizedWallet = normalize(walletId);
        if (idUser == null || normalizedWallet == null) {
            return Optional.empty();
        }

        return repository.findActiveAllocation(
                idUser,
                normalizedWallet,
                UserCopyAllocationEntity.Status.ACTIVE
        );
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
        log.warn("event=user_copy_allocation.status_guard_blocked user={} wallet={} strategy={} status={} reason={} cooldownUntil={}",
                idUser, normalizedWallet, normalizedStrategy, nextStatus, safeReason(reason), cooldownUntil);
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
            BigDecimal pct,
            Integer score
    ) {
        Dist {
            Objects.requireNonNull(walletId, "walletId");
            Objects.requireNonNull(strategyCode, "strategyCode");
            Objects.requireNonNull(pct, "pct");
        }
    }

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
