package com.apunto.engine.service.impl;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.MetricWalletService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
public class MetricWalletServiceImpl implements MetricWalletService {

    private final int historyLimit;
    private final int dayzLimit;
    private final int cacheMaxSize;
    private final Duration cacheRefreshAfter;
    private final Duration cacheExpireAfter;
    private final Duration slowThreshold;

    private final MetricWalletsInfoClient metricWalletsInfoClient;
    private final UserCopyAllocationService userCopyAllocationService;

    private final AtomicReference<List<MetricaWalletDto>> lastKnownGoodHistory = new AtomicReference<>(List.of());

    private final LoadingCache<Integer, List<MetricaWalletDto>> allPositionHistoryCache;

    public MetricWalletServiceImpl(
            MetricWalletsInfoClient metricWalletsInfoClient,
            UserCopyAllocationService userCopyAllocationService,
            @Value("${metric-wallet.history.limit:60}") int historyLimit,
            @Value("${metric-wallet.history.dayz:1}") int dayzLimit,
            @Value("${metric-wallet.history.cache.max-size:1}") int cacheMaxSize,
            @Value("${metric-wallet.history.cache.refresh-after:6m}") Duration cacheRefreshAfter,
            @Value("${metric-wallet.history.cache.expire-after:10m}") Duration cacheExpireAfter,
            @Value("${metric-wallet.history.slow-threshold:250ms}") Duration slowThreshold
    ) {
        this.metricWalletsInfoClient = Objects.requireNonNull(metricWalletsInfoClient, "metricWalletsInfoClient");
        this.userCopyAllocationService = Objects.requireNonNull(userCopyAllocationService, "userCopyAllocationService");
        this.dayzLimit = dayzLimit;

        if (historyLimit <= 0) throw new IllegalArgumentException("metric-wallet.history.limit must be > 0");
        if (cacheMaxSize <= 0) throw new IllegalArgumentException("metric-wallet.history.cache.max-size must be > 0");

        this.historyLimit = historyLimit;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheRefreshAfter = cacheRefreshAfter;
        this.cacheExpireAfter = cacheExpireAfter;
        this.slowThreshold = slowThreshold;

        this.allPositionHistoryCache = buildHistoryCache();

        log.info(
                "event=metric_wallets.config historyLimit={} cacheMaxSize={} refreshAfter={} expireAfter={} slowThreshold={}",
                this.historyLimit, this.cacheMaxSize, this.cacheRefreshAfter, this.cacheExpireAfter, this.slowThreshold
        );
    }

    @Override
    public List<MetricaWalletDto> getMetricWallets() {
        return getMetricWallets(0.95, 0.50);
    }

    @Override
    public List<MetricaWalletDto> getCandidatesUser(UUID idUser) {
        if (idUser == null) {
            log.warn("event=metric_wallets.candidates.invalid_request userId=null");
            return List.of();
        }

        final List<UserCopyAllocationEntity> allocations = userCopyAllocationService.getWalletUserId(idUser);
        if (allocations == null || allocations.isEmpty()) {
            log.warn("event=metric_wallets.candidates.empty_allocations userId={}", idUser);
            return List.of();
        }

        final HistoryResult history = getHistory(historyLimit);
        final String historySource = history == null ? "null" : history.source();
        final List<MetricaWalletDto> historyValues = history == null ? List.of() : history.values();

        final Map<String, MetricaWalletDto> metricByWalletId = new HashMap<>();
        for (MetricaWalletDto m : historyValues) {
            if (m == null || m.getWallet() == null || m.getWallet().getIdWallet() == null) {
                continue;
            }
            metricByWalletId.put(normalizeWalletId(m.getWallet().getIdWallet()), m);
        }

        log.info(
                "event=metric_wallets.candidates.lookup userId={} allocations={} historySource={} historySize={} sampleAllocations={} sampleHistory={}",
                idUser,
                allocations.size(),
                historySource,
                historyValues.size(),
                allocations.stream()
                        .map(UserCopyAllocationEntity::getWalletId)
                        .filter(Objects::nonNull)
                        .map(MetricWalletServiceImpl::normalizeWalletId)
                        .distinct()
                        .limit(8)
                        .toList(),
                historyValues.stream()
                        .map(MetricaWalletDto::getWallet)
                        .filter(Objects::nonNull)
                        .map(MetricaWalletDto.WalletDto::getIdWallet)
                        .filter(Objects::nonNull)
                        .map(MetricWalletServiceImpl::normalizeWalletId)
                        .distinct()
                        .limit(8)
                        .toList()
        );

        final List<MetricaWalletDto> result = allocations.stream()
                .map(a -> {
                    final String walletId = normalizeWalletId(a.getWalletId());
                    if (walletId == null) {
                        log.warn("event=metric_wallets.candidates.skip_allocation userId={} reason=wallet_null allocationId={}", idUser, a.getId());
                        return null;
                    }

                    final double allocationPct = Optional.ofNullable(a.getAllocationPct())
                            .map(java.math.BigDecimal::doubleValue)
                            .orElse(0.0);

                    final MetricaWalletDto cached = metricByWalletId.get(walletId);
                    if (cached != null) {
                        MetricaWalletDto out = new MetricaWalletDto();
                        BeanUtils.copyProperties(cached, out);
                        out.setCapitalShare(allocationPct);

                        log.info(
                                "event=metric_wallets.candidates.match userId={} wallet={} allocationPct={} hasScoring={} decisionMetricConservative={} historySource={}",
                                idUser,
                                walletId,
                                allocationPct,
                                out.getScoring() != null,
                                out.getScoring() != null ? out.getScoring().getDecisionMetricConservative() : null,
                                historySource
                        );
                        return out;
                    }

                    log.warn(
                            "event=metric_wallets.candidates.placeholder userId={} wallet={} allocationPct={} historySource={} note=allocation_present_but_metric_not_cached",
                            idUser,
                            walletId,
                            allocationPct,
                            historySource
                    );

                    return MetricaWalletDto.builder()
                            .wallet(MetricaWalletDto.WalletDto.builder().idWallet(walletId).build())
                            .capitalShare(allocationPct)
                            .build();
                })
                .filter(Objects::nonNull)
                .toList();

        final long withScoring = result.stream()
                .filter(Objects::nonNull)
                .filter(r -> r.getScoring() != null)
                .count();
        final long placeholders = result.size() - withScoring;

        log.info(
                "event=metric_wallets.candidates.result userId={} resultSize={} withScoring={} placeholders={} sampleResult={}",
                idUser,
                result.size(),
                withScoring,
                placeholders,
                result.stream()
                        .map(MetricaWalletDto::getWallet)
                        .filter(Objects::nonNull)
                        .map(MetricaWalletDto.WalletDto::getIdWallet)
                        .filter(Objects::nonNull)
                        .map(MetricWalletServiceImpl::normalizeWalletId)
                        .distinct()
                        .limit(8)
                        .toList()
        );

        return result;
    }

    private static String normalizeWalletId(String raw) {
        if (raw == null) return null;
        String t = raw.trim();
        if (t.isEmpty()) return null;
        return t.toLowerCase();
    }

    public List<MetricaWalletDto> getMetricWallets(double maxCapitalToUse, double maxPerWallet) {
        final long startNs = System.nanoTime();

        validateAllocationInputs(maxCapitalToUse, maxPerWallet);

        HistoryResult history = getHistory(historyLimit);

        if (history.values().isEmpty()) {
            log.warn("event=metric_wallets.empty_history source={}", history.source());
            return List.of();
        }

        List<MetricaWalletDto> candidates = selectCandidates(history.values(), dayzLimit);

        CapitalAllocator.allocate(candidates, maxCapitalToUse, maxPerWallet);
        validateLimits(candidates, maxPerWallet, maxCapitalToUse);

        if ("cache".equals(history.source())) {
            try {
                userCopyAllocationService.syncDistribution(candidates);
            } catch (Exception ex) {
                log.warn("event=user_copy_allocation.sync_failed err={}", safeErr(ex));
            }
        } else {
            log.warn("event=user_copy_allocation.sync_skipped reason=non_fresh_history source={}", history.source());
        }

        return candidates;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void primeHistoryCache() {
        try {
            List<MetricaWalletDto> v = allPositionHistoryCache.get(historyLimit);
            log.info("event=metric_wallets.cache_primed limit={} size={}", historyLimit, v.size());
        } catch (Exception ex) {
            log.warn("event=metric_wallets.cache_prime_failed limit={} err={}", historyLimit, safeErr(ex));
        }
    }

    @Scheduled(fixedDelayString = "${metric-wallet.history.cache.refresh-job:5m}")
    public void refreshHistoryCacheJob() {
        try {
            allPositionHistoryCache.refresh(historyLimit);
            log.debug("event=metric_wallets.cache_refresh_triggered limit={}", historyLimit);
        } catch (Exception ex) {
            log.warn("event=metric_wallets.cache_refresh_failed limit={} err={}", historyLimit, safeErr(ex));
        }
    }

    private LoadingCache<Integer, List<MetricaWalletDto>> buildHistoryCache() {
        return Caffeine.newBuilder()
                .maximumSize(cacheMaxSize)
                .refreshAfterWrite(cacheRefreshAfter)
                .expireAfterWrite(cacheExpireAfter)
                .recordStats()
                .build(this::loadAllPositionHistory);
    }

    private List<MetricaWalletDto> loadAllPositionHistory(Integer limit) {
        return loadAllPositionHistory(limit, dayzLimit);
    }

    private List<MetricaWalletDto> loadAllPositionHistory(Integer limit, Integer dayz) {
        final long startNs = System.nanoTime();

        List<MetricaWalletDto> resp;
        try {
            resp = metricWalletsInfoClient.allPositionHistory(limit, dayz);
        } catch (Exception ex) {
            long durationMs = elapsedMs(startNs);
            log.warn("event=metric_wallets.history_client_failed limit={} durationMs={} err={}", limit, durationMs, safeErr(ex));
            return List.of();
        }

        long durationMs = elapsedMs(startNs);
        int size = resp == null ? 0 : resp.size();

        if (resp == null) {
            log.warn("event=metric_wallets.history_null limit={} durationMs={}", limit, durationMs);
            return List.of();
        }

        if (!resp.isEmpty()) {
            lastKnownGoodHistory.set(List.copyOf(resp));
        } else {
            log.warn("event=metric_wallets.history_empty_response limit={} durationMs={}", limit, durationMs);
        }

        log.debug("event=metric_wallets.history_loaded limit={} size={} durationMs={}", limit, size, durationMs);
        return resp;
    }

    private HistoryResult getHistory(int limit) {
        try {
            List<MetricaWalletDto> v = Optional.ofNullable(allPositionHistoryCache.get(limit)).orElse(List.of());
            if (!v.isEmpty()) return new HistoryResult(v, "cache");
        } catch (Exception ex) {
            log.warn(
                    "event=metric_wallets.history_load_failed limit={} err={} cacheStats={}",
                    limit, safeErr(ex), allPositionHistoryCache.stats()
            );
        }

        List<MetricaWalletDto> present = allPositionHistoryCache.getIfPresent(limit);
        if (present != null && !present.isEmpty()) {
            return new HistoryResult(present, "cache_if_present");
        }

        List<MetricaWalletDto> lkg = lastKnownGoodHistory.get();
        if (lkg != null && !lkg.isEmpty()) {
            return new HistoryResult(lkg, "last_known_good");
        }

        return new HistoryResult(List.of(), "empty");
    }

    private List<MetricaWalletDto> selectCandidates(List<MetricaWalletDto> base, int dayzLimit) {
        return base.stream()
                .filter(Objects::nonNull)
                .filter(dto -> dto.getScoring() != null)
                .filter(MetricWalletServiceImpl::hasClosedHistory)
                .filter(dto -> dto.getWallet() != null && dto.getWallet().getHistoryDays() != null)
                .filter(dto -> {
                    var s = dto.getScoring();
                    return gte(s.getDecisionMetricConservative(), 55)
                            || gte(s.getDecisionMetricScalping(), 59)
                            || gte(s.getDecisionMetricAggressive(), 59);
                })
                .filter(dto -> {
                    double d = dto.getWallet().getHistoryDays();
                    return Double.isFinite(d) && d >= dayzLimit;
                })
                .sorted(Comparator.comparingDouble(MetricWalletServiceImpl::decisionScore).reversed())
                .map(this::copyForAllocation)
                .toList();
    }

    private static boolean gte(Integer value, int threshold) {
        return value != null && value >= threshold;
    }


    private MetricaWalletDto copyForAllocation(MetricaWalletDto src) {
        MetricaWalletDto dst = new MetricaWalletDto();
        BeanUtils.copyProperties(src, dst);
        dst.setCapitalShare(0.0);
        return dst;
    }

    private static double decisionScore(MetricaWalletDto dto) {
        if (dto == null || dto.getScoring() == null) return 0.0;

        Integer v = dto.getScoring().getDecisionMetricConservative();
        return v != null ? v.doubleValue() : 0.0;
    }

    private static void validateAllocationInputs(double maxCapitalToUse, double maxPerWallet) {
        if (maxCapitalToUse <= 0.0 || maxCapitalToUse > 1.0) {
            throw new IllegalArgumentException("maxCapitalToUse debe estar entre (0, 1]. Ej: 0.9 = 90%.");
        }
        if (maxPerWallet <= 0.0 || maxPerWallet > maxCapitalToUse) {
            throw new IllegalArgumentException("maxPerWallet debe estar entre (0, maxCapitalToUse].");
        }
    }

    private static void validateLimits(List<MetricaWalletDto> wallets, double maxPerWallet, double maxCapitalToUse) {
        double total = wallets.stream().mapToDouble(MetricaWalletDto::getCapitalShare).sum();
        double max = wallets.stream().mapToDouble(MetricaWalletDto::getCapitalShare).max().orElse(0.0);

        if (total - maxCapitalToUse > 1e-9) {
            throw new IllegalStateException(
                    String.format("Total capitalShare (%.6f) excede maxCapitalToUse (%.6f)", total, maxCapitalToUse)
            );
        }
        if (max - maxPerWallet > 1e-9) {
            throw new IllegalStateException(
                    String.format("Una wallet tiene capitalShare (%.6f) mayor a maxPerWallet (%.6f)", max, maxPerWallet)
            );
        }
    }

    private static long elapsedMs(long startNs) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
    }

    private static String safeErr(Throwable t) {
        return t.getClass().getSimpleName() + ": " + (t.getMessage() == null ? "" : t.getMessage());
    }

    private static final class HistoryResult {
        private final List<MetricaWalletDto> values;
        private final String source;

        private HistoryResult(List<MetricaWalletDto> values, String source) {
            this.values = values;
            this.source = source;
        }

        public List<MetricaWalletDto> values() {
            return values;
        }

        public String source() {
            return source;
        }
    }

    private static final class CapitalAllocator {

        private CapitalAllocator() {}

        static void allocate(List<MetricaWalletDto> wallets, double maxCapitalToUse, double maxPerWallet) {
            if (wallets == null || wallets.isEmpty()) return;

            wallets.forEach(w -> w.setCapitalShare(0.0));

            double remainingWeight = maxCapitalToUse;
            List<MetricaWalletDto> remaining = new ArrayList<>(wallets);

            while (!remaining.isEmpty() && remainingWeight > 0.0) {
                double sumScore = remaining.stream().mapToDouble(CapitalAllocator::decisionScore).sum();

                if (sumScore <= 0.0) {
                    remainingWeight = distributeEqually(remaining, remainingWeight, maxPerWallet);
                    break;
                }

                boolean anyCapped = false;

                Iterator<MetricaWalletDto> it = remaining.iterator();
                while (it.hasNext() && remainingWeight > 0.0) {
                    MetricaWalletDto w = it.next();

                    double score = decisionScore(w);
                    if (score <= 0.0) continue;

                    double proposed = remainingWeight * (score / sumScore);

                    double current = w.getCapitalShare();
                    double newShare = current + proposed;

                    if (newShare >= maxPerWallet) {
                        double inc = Math.max(0.0, maxPerWallet - current);
                        w.setCapitalShare(current + inc);
                        remainingWeight -= inc;
                        it.remove();
                        anyCapped = true;
                    }
                }

                if (!anyCapped && remainingWeight > 0.0) {
                    for (MetricaWalletDto w : remaining) {
                        double score = decisionScore(w);
                        if (score <= 0.0) continue;

                        double inc = remainingWeight * (score / sumScore);
                        double current = w.getCapitalShare();
                        w.setCapitalShare(Math.min(maxPerWallet, current + inc));
                    }
                    remainingWeight = 0.0;
                }
            }

            wallets.forEach(w -> {
                if (w.getCapitalShare() < 0.0) w.setCapitalShare(0.0);
            });
        }

        private static double decisionScore(MetricaWalletDto dto) {
            if (dto == null || dto.getScoring() == null) return 0.0;
            Integer v = dto.getScoring().getDecisionMetricConservative();
            return v != null ? v.doubleValue() : 0.0;
        }

        private static double distributeEqually(List<MetricaWalletDto> remaining, double remainingWeight, double maxPerWallet) {
            double equalShare = remainingWeight / remaining.size();

            Iterator<MetricaWalletDto> it = remaining.iterator();
            while (it.hasNext() && remainingWeight > 0.0) {
                MetricaWalletDto w = it.next();
                double current = w.getCapitalShare();
                double room = maxPerWallet - current;

                if (room <= 0.0) {
                    it.remove();
                    continue;
                }

                double inc = Math.min(equalShare, room);
                w.setCapitalShare(current + inc);
                remainingWeight -= inc;

                if (room <= inc) it.remove();
            }
            return remainingWeight;
        }
    }

    private static boolean hasClosedHistory(MetricaWalletDto dto) {
        if (dto == null) return false;

        if (dto.getActivity() != null && dto.getActivity().getLastClosedAt() != null) {
            return true;
        }

        Integer c = dto.getWallet() == null ? null : dto.getWallet().getCountOperation();
        return c != null && c > 0;
    }
}
