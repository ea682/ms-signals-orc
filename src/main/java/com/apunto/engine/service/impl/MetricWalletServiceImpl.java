package com.apunto.engine.service.impl;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.service.MetricWalletService;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class MetricWalletServiceImpl implements MetricWalletService {

    private static final int HISTORY_LIMIT = 60;
    private static final long REFRESH_SECONDS = 10;
    private static final long EXPIRE_SECONDS = 30;
    private static final long SLOW_THRESHOLD_MS = 250;

    private final MetricWalletsInfoClient metricWalletsInfoClient;

    private final LoadingCache<Integer, List<MetricaWalletDto>> allPositionHistoryCache =
            Caffeine.newBuilder()
                    .maximumSize(10)
                    .refreshAfterWrite(REFRESH_SECONDS, TimeUnit.SECONDS)
                    .expireAfterWrite(EXPIRE_SECONDS, TimeUnit.SECONDS)
                    .recordStats()
                    .build(this::loadAllPositionHistorySafely);

    @Override
    public List<MetricaWalletDto> getMetricWallets(int maxWallets) {
        double maxCapitalToUse = 0.90;
        double maxPerWallet = 0.50;
        return getMetricWallets(maxWallets, maxCapitalToUse, maxPerWallet);
    }

    public List<MetricaWalletDto> getMetricWallets(int maxWallets, double maxCapitalToUse, double maxPerWallet) {
        long startNs = System.nanoTime();

        if (maxWallets <= 0) {
            return List.of();
        }
        if (maxCapitalToUse <= 0.0 || maxCapitalToUse > 1.0) {
            throw new IllegalArgumentException("maxCapitalToUse debe estar entre 0 y 1 (ej: 0.9 = 90%).");
        }
        if (maxPerWallet <= 0.0 || maxPerWallet > maxCapitalToUse) {
            throw new IllegalArgumentException("maxPerWallet debe estar entre 0 y maxCapitalToUse.");
        }

        List<MetricaWalletDto> base;
        try {
            base = allPositionHistoryCache.get(HISTORY_LIMIT);
        } catch (Exception ex) {
            log.warn("metric_wallets.history_cache_error limit={} err={}", HISTORY_LIMIT, ex.toString());
            base = List.of();
        }

        if (base == null || base.isEmpty()) {
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            log.warn("metric_wallets.empty_history maxWallets={} durationMs={} cacheStats={}",
                    maxWallets, ms, allPositionHistoryCache.stats());
            return List.of();
        }

        List<MetricaWalletDto> candidates = base.stream()
                .filter(dto -> Boolean.TRUE.equals(dto.getPassesFilter()))
                .sorted(Comparator.comparingDouble(MetricaWalletDto::getDecisionMetric).reversed())
                .limit(maxWallets)
                .map(this::copyForAllocation)
                .collect(Collectors.toList());

        if (candidates.isEmpty()) {
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            log.info("metric_wallets.no_candidates maxWallets={} baseSize={} durationMs={}",
                    maxWallets, base.size(), ms);
            return List.of();
        }

        applyCapitalWeights(candidates, maxPerWallet, maxCapitalToUse);
        validateLimits(candidates, maxPerWallet, maxCapitalToUse);

        double total = candidates.stream().mapToDouble(MetricaWalletDto::getCapitalShare).sum();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

        if (durationMs >= SLOW_THRESHOLD_MS) {
            log.warn("metric_wallets.slow maxWallets={} baseSize={} candidates={} totalShare={} durationMs={} cacheStats={}",
                    maxWallets, base.size(), candidates.size(), total, durationMs, allPositionHistoryCache.stats());
        } else {
            log.debug("metric_wallets.ok maxWallets={} baseSize={} candidates={} totalShare={} durationMs={}",
                    maxWallets, base.size(), candidates.size(), total, durationMs);
        }

        return candidates;
    }

    private List<MetricaWalletDto> loadAllPositionHistorySafely(Integer limit) {
        long startNs = System.nanoTime();
        try {
            List<MetricaWalletDto> resp = metricWalletsInfoClient.allPositionHistory(limit);
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            int size = (resp == null) ? 0 : resp.size();
            log.debug("metric_wallets.history_loaded limit={} size={} durationMs={}", limit, size, ms);
            return resp != null ? resp : List.of();
        } catch (Exception ex) {
            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            log.warn("metric_wallets.history_load_failed limit={} durationMs={} err={}", limit, ms, ex.toString());
            return List.of();
        }
    }

    private MetricaWalletDto copyForAllocation(MetricaWalletDto src) {
        MetricaWalletDto dst = new MetricaWalletDto();
        BeanUtils.copyProperties(src, dst);
        dst.setCapitalShare(0.0);
        return dst;
    }

    private void applyCapitalWeights(List<MetricaWalletDto> wallets, double maxPerWallet, double maxCapitalToUse) {
        if (wallets == null || wallets.isEmpty()) {
            return;
        }

        wallets.forEach(w -> w.setCapitalShare(0.0));

        double remainingWeight = maxCapitalToUse;
        List<MetricaWalletDto> remaining = new ArrayList<>(wallets);

        while (!remaining.isEmpty() && remainingWeight > 0.0) {
            double sumScore = remaining.stream()
                    .mapToDouble(MetricaWalletDto::getDecisionMetric)
                    .sum();

            if (sumScore <= 0.0) {
                double equalShare = remainingWeight / remaining.size();
                Iterator<MetricaWalletDto> itEqual = remaining.iterator();
                while (itEqual.hasNext() && remainingWeight > 0.0) {
                    MetricaWalletDto w = itEqual.next();
                    double current = w.getCapitalShare();
                    double room = maxPerWallet - current;
                    if (room <= 0.0) {
                        itEqual.remove();
                        continue;
                    }
                    double increment = Math.min(equalShare, room);
                    w.setCapitalShare(current + increment);
                    remainingWeight -= increment;

                    if (room <= increment) {
                        itEqual.remove();
                    }
                }
                break;
            }

            boolean anyCapped = false;

            Iterator<MetricaWalletDto> it = remaining.iterator();
            while (it.hasNext() && remainingWeight > 0.0) {
                MetricaWalletDto w = it.next();
                double score = w.getDecisionMetric();

                double proposed = remainingWeight * (score / sumScore);
                double current = w.getCapitalShare();
                double newShare = current + proposed;

                if (newShare > maxPerWallet) {
                    double cappedIncrement = maxPerWallet - current;
                    if (cappedIncrement < 0.0) {
                        cappedIncrement = 0.0;
                    }

                    w.setCapitalShare(current + cappedIncrement);
                    remainingWeight -= cappedIncrement;
                    it.remove();
                    anyCapped = true;
                }
            }

            if (!anyCapped && remainingWeight > 0.0) {
                for (MetricaWalletDto w : remaining) {
                    double score = w.getDecisionMetric();
                    double shareIncrement = remainingWeight * (score / sumScore);
                    double current = w.getCapitalShare();
                    double newShare = current + shareIncrement;

                    if (newShare > maxPerWallet) {
                        shareIncrement = maxPerWallet - current;
                        newShare = maxPerWallet;
                    }
                    w.setCapitalShare(newShare);
                }
                remainingWeight = 0.0;
                break;
            }
        }

        wallets.forEach(w -> {
            if (w.getCapitalShare() < 0.0) {
                w.setCapitalShare(0.0);
            }
        });

        double total = wallets.stream()
                .mapToDouble(MetricaWalletDto::getCapitalShare)
                .sum();

        log.debug("metric_wallets.capital_assigned totalShare={} maxCapitalToUse={}", total, maxCapitalToUse);
    }

    private void validateLimits(List<MetricaWalletDto> wallets, double maxPerWallet, double maxCapitalToUse) {
        double total = wallets.stream()
                .mapToDouble(MetricaWalletDto::getCapitalShare)
                .sum();

        double max = wallets.stream()
                .mapToDouble(MetricaWalletDto::getCapitalShare)
                .max()
                .orElse(0.0);

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
}
