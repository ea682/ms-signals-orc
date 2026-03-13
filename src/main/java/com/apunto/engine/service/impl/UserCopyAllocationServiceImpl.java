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
import java.util.LinkedHashMap;
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

            final List<MetricaWalletDto> top = (candidates.size() <= maxWallet)
                    ? candidates
                    : candidates.subList(0, maxWallet);

            // Mantener orden para que el ajuste final caiga siempre en la última wallet válida.
            final Map<String, Dist> newDist = new LinkedHashMap<>();

            // Primero limpiar las wallets válidas del top.
            final List<MetricaWalletDto> validTop = new ArrayList<>();
            BigDecimal topTotalPct = ZERO;

            for (MetricaWalletDto dto : top) {
                if (dto == null || dto.getWallet() == null) continue;

                final String walletId = normalize(dto.getWallet().getIdWallet());
                if (walletId == null) continue;

                final BigDecimal pct = safePct(dto.getCapitalShare());
                if (pct.signum() <= 0) continue;

                validTop.add(dto);
                topTotalPct = topTotalPct.add(pct);
            }

            topTotalPct = topTotalPct.setScale(6, RoundingMode.HALF_UP);

            // Si no hay top válido, cerrar todo lo activo del usuario.
            final List<UserCopyAllocationEntity> existingActive =
                    repository.findAllByIdUserAndEndsAtIsNull(idUser);

            if (validTop.isEmpty() || topTotalPct.signum() <= 0 || targetTotalPct.signum() <= 0) {
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

            // Factor para reescalar el top al total original de candidates.
            // Ej: si top suma 0.657324 y candidates suma 0.950000 => factor ≈ 1.445253...
            final BigDecimal scaleFactor = targetTotalPct.divide(topTotalPct, 18, RoundingMode.HALF_UP);

            BigDecimal accumulated = ZERO;

            for (int i = 0; i < validTop.size(); i++) {
                final MetricaWalletDto dto = validTop.get(i);
                final String walletId = normalize(dto.getWallet().getIdWallet());
                final BigDecimal originalPct = safePct(dto.getCapitalShare());
                final Integer score = safeScore(dto);

                final boolean isLast = (i == validTop.size() - 1);

                BigDecimal scaledPct;
                if (isLast) {
                    // Ajuste final para cerrar exacto a 6 decimales.
                    scaledPct = targetTotalPct.subtract(accumulated).setScale(6, RoundingMode.HALF_UP);
                } else {
                    scaledPct = originalPct.multiply(scaleFactor).setScale(6, RoundingMode.HALF_UP);
                    accumulated = accumulated.add(scaledPct);
                }

                if (scaledPct.signum() <= 0) continue;

                newDist.put(walletId, new Dist(scaledPct, score));
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

                entity.setAllocationPct(d.pct());
                entity.setScore(d.score());
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

            final BigDecimal persistedTotal = newDist.values().stream()
                    .map(Dist::pct)
                    .reduce(ZERO, BigDecimal::add)
                    .setScale(6, RoundingMode.HALF_UP);

            log.debug("event=user_copy_allocation.sync_ok user={} maxWallet={} candidates={} persisted={} closed={} targetTotalPct={} persistedTotal={}",
                    idUser, maxWallet, candidates.size(), newDist.size(), closed, targetTotalPct, persistedTotal);
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
