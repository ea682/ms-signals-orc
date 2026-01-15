package com.apunto.engine.service.impl;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.service.MetricWalletService;
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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MetricWalletServiceImpl implements MetricWalletService {

    private final int historyLimit;
    private final int cacheMaxSize;
    private final Duration cacheRefreshAfter;
    private final Duration cacheExpireAfter;
    private final Duration slowThreshold;

    private final MetricWalletsInfoClient metricWalletsInfoClient;

    private final AtomicReference<List<MetricaWalletDto>> lastKnownGoodHistory = new AtomicReference<>(List.of());

    private final LoadingCache<Integer, List<MetricaWalletDto>> allPositionHistoryCache;

    public MetricWalletServiceImpl(
            MetricWalletsInfoClient metricWalletsInfoClient,
            @Value("${metric-wallet.history.limit:60}") int historyLimit,
            @Value("${metric-wallet.history.cache.max-size:1}") int cacheMaxSize,
            @Value("${metric-wallet.history.cache.refresh-after:60s}") Duration cacheRefreshAfter,
            @Value("${metric-wallet.history.cache.expire-after:10m}") Duration cacheExpireAfter,
            @Value("${metric-wallet.history.slow-threshold:250ms}") Duration slowThreshold
    ) {
        this.metricWalletsInfoClient = Objects.requireNonNull(metricWalletsInfoClient, "metricWalletsInfoClient");

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
    public List<MetricaWalletDto> getMetricWallets(int maxWallets) {
        return getMetricWallets(maxWallets, 0.90, 0.50);
    }

    public List<MetricaWalletDto> getMetricWallets(int maxWallets, double maxCapitalToUse, double maxPerWallet) {
        final long startNs = System.nanoTime();

        if (maxWallets <= 0) return List.of();
        validateAllocationInputs(maxCapitalToUse, maxPerWallet);

        HistoryResult history = getHistory(historyLimit);

        if (history.values().isEmpty()) {
            long durationMs = elapsedMs(startNs);
            log.warn(
                    "event=metric_wallets.empty_history maxWallets={} limit={} durationMs={} source={} cacheStats={}",
                    maxWallets, historyLimit, durationMs, history.source(), allPositionHistoryCache.stats()
            );
            return List.of();
        }

        List<MetricaWalletDto> candidates = selectCandidates(history.values(), maxWallets);

        if (candidates.isEmpty()) {
            long durationMs = elapsedMs(startNs);
            log.info(
                    "event=metric_wallets.no_candidates maxWallets={} limit={} baseSize={} durationMs={} source={}",
                    maxWallets, historyLimit, history.values().size(), durationMs, history.source()
            );
            return List.of();
        }

        CapitalAllocator.allocate(candidates, maxCapitalToUse, maxPerWallet);
        validateLimits(candidates, maxPerWallet, maxCapitalToUse);

        double totalShare = candidates.stream().mapToDouble(MetricaWalletDto::getCapitalShare).sum();
        long durationMs = elapsedMs(startNs);

        if (durationMs >= slowThreshold.toMillis()) {
            log.warn(
                    "event=metric_wallets.slow maxWallets={} limit={} baseSize={} candidates={} totalShare={} durationMs={} source={} cacheStats={}",
                    maxWallets, historyLimit, history.values().size(), candidates.size(), totalShare, durationMs, history.source(),
                    allPositionHistoryCache.stats()
            );
        } else {
            log.debug(
                    "event=metric_wallets.ok maxWallets={} limit={} baseSize={} candidates={} totalShare={} durationMs={} source={}",
                    maxWallets, historyLimit, history.values().size(), candidates.size(), totalShare, durationMs, history.source()
            );
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

    @Scheduled(fixedDelayString = "${metric-wallet.history.cache.refresh-job:60s}")
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
        final long startNs = System.nanoTime();

        List<MetricaWalletDto> resp;
        try {
            resp = metricWalletsInfoClient.allPositionHistory(limit);
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

    private List<MetricaWalletDto> selectCandidates(List<MetricaWalletDto> base, int maxWallets) {
        return base.stream()
                .filter(Objects::nonNull)
                .filter((MetricaWalletDto dto) -> dto.getScoring() != null)
                .filter(dto -> Boolean.TRUE.equals(dto.getScoring().getPassesFilter()))
                .filter(dto -> Boolean.TRUE.equals(dto.getScoring().getPreCopiable()))
                .filter(dto ->  69 <= dto.getScoring().getDecisionMetricConservative())
                .sorted(Comparator.comparingDouble(MetricWalletServiceImpl::decisionScore).reversed())
                .limit(maxWallets)
                .map(this::copyForAllocation)
                .collect(Collectors.toList());
    }

    private MetricaWalletDto copyForAllocation(MetricaWalletDto src) {
        MetricaWalletDto dst = new MetricaWalletDto();
        BeanUtils.copyProperties(src, dst);
        dst.setCapitalShare(0.0);
        return dst;
    }

    private static double decisionScore(MetricaWalletDto dto) {
        if (dto == null || dto.getScoring() == null) return 0.0;

        MetricaWalletDto.ScoringDto s = dto.getScoring();

        Integer v = s.getDecisionMetricConservative();
        if (v != null) return v.doubleValue();

        return 0.0;
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

            MetricaWalletDto.ScoringDto s = dto.getScoring();

            Integer v = v = s.getDecisionMetricConservative();
            if (v != null) return v.doubleValue();

            v = s.getDecisionMetricConservative();
            if (v != null) return v.doubleValue();

            return 0.0;
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
}
