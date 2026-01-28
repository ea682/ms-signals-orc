package com.apunto.engine.service.impl;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.UserCopyAllocationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserCopyAllocationServiceImpl implements UserCopyAllocationService {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    private final UserCopyAllocationRepository repository;

    @Override
    @Transactional
    public void syncDistribution(int maxWallet, List<MetricaWalletDto> candidates) {
        if (maxWallet <= 0) {
            return;
        }

        if (candidates == null || candidates.isEmpty()) {
            log.debug("event=user_copy_allocation.sync_skipped reason=empty_candidates maxWallet={}", maxWallet);
            return;
        }

        final OffsetDateTime now = OffsetDateTime.now();

        // newDist: walletId -> (pct, score)
        final Map<String, Dist> newDist = new HashMap<>();
        for (MetricaWalletDto dto : candidates) {
            if (dto == null || dto.getWallet() == null) continue;

            final String walletId = normalize(dto.getWallet().getIdWallet());
            if (walletId == null) continue;

            final BigDecimal pct = safePct(dto.getCapitalShare());
            if (pct.compareTo(ZERO) <= 0) continue;

            final Integer score = safeScore(dto);
            newDist.put(walletId, new Dist(pct, score));
        }

        if (newDist.isEmpty()) {
            log.debug("event=user_copy_allocation.sync_skipped reason=empty_distribution maxWallet={}", maxWallet);
            return;
        }

        final List<UserCopyAllocationEntity> existingActive = repository.findAllByMaxWalletAndEndsAtIsNull(maxWallet);

        final Map<String, UserCopyAllocationEntity> existingByWallet = new HashMap<>();
        for (UserCopyAllocationEntity e : existingActive) {
            if (e == null) continue;
            final String w = normalize(e.getWalletId());
            if (w != null) existingByWallet.put(w, e);
        }

        final List<UserCopyAllocationEntity> toSave = new ArrayList<>(newDist.size() + existingActive.size());

        for (Map.Entry<String, Dist> entry : newDist.entrySet()) {
            final String walletId = entry.getKey();
            final Dist d = entry.getValue();

            UserCopyAllocationEntity entity = existingByWallet.get(walletId);
            if (entity == null) {
                entity = UserCopyAllocationEntity.builder()
                        .maxWallet(maxWallet)
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

        log.debug("event=user_copy_allocation.sync_ok maxWallet={} candidates={} persisted={} closed={}",
                maxWallet,
                candidates.size(),
                newDist.size(),
                closed);
    }

    @Override
    @Transactional(readOnly = true)
    public List<UserCopyAllocationEntity> getActiveDistribution(int maxWallet) {
        if (maxWallet <= 0) {
            return List.of();
        }
        return repository.findAllByMaxWalletAndEndsAtIsNull(maxWallet);
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
