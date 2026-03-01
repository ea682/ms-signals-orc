package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailService;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserCopyAllocationServiceImpl implements UserCopyAllocationService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final UserCopyAllocationRepository repository;
    private final UserDetailService userDetailService;

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

        for (UserDetailDto user : users) {
            if (user == null || user.getDetail() == null) continue;

            final UUID idUser = user.getUser().getId();
            if (idUser == null) continue;

            final Integer maxWalletBoxed = user.getDetail().getMaxWallet();
            final int maxWallet = (maxWalletBoxed == null) ? 0 : maxWalletBoxed;

            if (maxWallet <= 0) {
                continue;
            }

            final List<MetricaWalletDto> top = (candidates.size() <= maxWallet)
                    ? candidates
                    : candidates.subList(0, maxWallet);

            final Map<String, Dist> newDist = new HashMap<>();
            for (MetricaWalletDto dto : top) {
                if (dto == null || dto.getWallet() == null) continue;

                final String walletId = normalize(dto.getWallet().getIdWallet());
                if (walletId == null) continue;

                final BigDecimal pct = safePct(dto.getCapitalShare());
                if (pct.signum() <= 0) continue;

                final Integer score = safeScore(dto);
                newDist.put(walletId, new Dist(pct, score));
            }

            final List<UserCopyAllocationEntity> existingActive =
                    repository.findAllByIdUserAndEndsAtIsNull(idUser);

            if (newDist.isEmpty()) {
                for (UserCopyAllocationEntity e : existingActive) {
                    if (e == null) continue;
                    e.setStatus(UserCopyAllocationEntity.Status.CLOSED);
                    e.setEndsAt(now);
                    e.setUpdatedAt(now);
                }
                repository.saveAll(existingActive);

                log.debug("event=user_copy_allocation.sync_ok reason=empty_distribution user={} closed={}",
                        idUser, existingActive.size());
                continue;
            }

            final List<String> newWalletIdList = new ArrayList<>(newDist.keySet());

            final List<UserCopyAllocationEntity> existingForNewWallets =
                    repository.findAllByIdUserAndWalletIdIn(idUser, newWalletIdList);

            final Map<String, UserCopyAllocationEntity> existingByWallet = new HashMap<>();
            for (UserCopyAllocationEntity e : existingForNewWallets) {
                if (e == null) continue;
                final String w = normalize(e.getWalletId());
                if (w != null) existingByWallet.put(w, e);
            }

            final List<UserCopyAllocationEntity> toSave =
                    new ArrayList<>(newDist.size() + existingActive.size());

            for (Map.Entry<String, Dist> entry : newDist.entrySet()) {
                final String walletId = entry.getKey();
                final Dist d = entry.getValue();

                UserCopyAllocationEntity entity = existingByWallet.get(walletId);
                if (entity == null) {
                    entity = UserCopyAllocationEntity.builder()
                            .idUser(idUser)
                            .walletId(walletId)
                            .build();
                }

                entity.setAllocationPct(d.pct);
                entity.setScore(d.score);
                entity.setStatus(UserCopyAllocationEntity.Status.ACTIVE);
                entity.setEndsAt(null);
                entity.setUpdatedAt(now);

                toSave.add(entity);
            }

            final Set<String> newWalletIds = new HashSet<>(newDist.keySet());
            int closed = 0;

            for (UserCopyAllocationEntity e : existingActive) {
                if (e == null) continue;

                final String walletId = normalize(e.getWalletId());
                if (walletId == null) continue;
                if (newWalletIds.contains(walletId)) continue;

                e.setStatus(UserCopyAllocationEntity.Status.CLOSED);
                e.setEndsAt(now);
                e.setUpdatedAt(now);

                toSave.add(e);
                closed++;
            }

            repository.saveAll(toSave);

            log.debug("event=user_copy_allocation.sync_ok user={} maxWallet={} candidates={} persisted={} closed={}",
                    idUser, maxWallet, candidates.size(), newDist.size(), closed);
        }
    }
    @Override
    @Transactional(readOnly = true)
    public List<UserCopyAllocationEntity> getWalletUserId(UUID idUser) {
        if (idUser == null) return List.of();

        // Fuente de verdad: SOLO asignaciones vigentes (endsAt IS NULL) y activas.
        // Esto permite pausar/cerrar sin afectar históricos.
        return repository.findAllByIdUserAndEndsAtIsNull(idUser)
                .stream()
                .filter(Objects::nonNull)
                .filter(e -> e.getStatus() == UserCopyAllocationEntity.Status.ACTIVE)
                // Orden estable para que la UI y el engine consuman siempre en el mismo orden.
                .sorted(Comparator
                        .comparing(UserCopyAllocationEntity::getScore, Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(UserCopyAllocationEntity::getAllocationPct, Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(UserCopyAllocationEntity::getWalletId, Comparator.nullsLast(String::compareToIgnoreCase))
                )
                .toList();
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
        return t.isEmpty() ? null : t;
    }

    private record Dist(BigDecimal pct, Integer score) {
        Dist {
            Objects.requireNonNull(pct, "pct");
        }
    }
}
