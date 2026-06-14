package com.apunto.engine.service.impl;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.MetricWalletService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.copy.CopyStrategyRuntimeRouter;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

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
    private final double maxPerWallet;
    private final double maxCapitalToUse;
    private final boolean syncDistributionEnabled;
    private final String historySource;
    private final int joyasLimit;
    private final int joyasDayz;
    private final String joyasSimulation;
    private final double minHistoryDays;
    private final boolean copyGuardEnabled;
    private final boolean copyGuardFailOpenOnMissingMetric;
    private final boolean copyGuardRequireWindowData;
    private final double copyGuardMinTotalPnlUsdt;
    private final double copyGuardMinWindowPnlUsdt;
    private final List<String> copyGuardWindows;

    private final MetricWalletsInfoClient metricWalletsInfoClient;
    private final UserCopyAllocationService userCopyAllocationService;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;

    private final AtomicReference<List<MetricaWalletDto>> lastKnownGoodHistory = new AtomicReference<>(List.of());

    private final LoadingCache<Integer, List<MetricaWalletDto>> allPositionHistoryCache;

    public MetricWalletServiceImpl(
            MetricWalletsInfoClient metricWalletsInfoClient,
            UserCopyAllocationService userCopyAllocationService,
            CopyStrategyRuntimeRouter copyStrategyRuntimeRouter,
            @Value("${metric-wallet.history.limit:300}") int historyLimit,
            @Value("${metric-wallet.history.dayz:20}") int dayzLimit,
            @Value("${metric-wallet.history.cache.max-size:1}") int cacheMaxSize,
            @Value("${metric-wallet.history.cache.refresh-after:6m}") Duration cacheRefreshAfter,
            @Value("${metric-wallet.history.cache.expire-after:10m}") Duration cacheExpireAfter,
            @Value("${metric-wallet.history.slow-threshold:250ms}") Duration slowThreshold,
            @Value("${metric-wallet.allocation.max-capital-to-use:0.90}") double maxCapitalToUse,
            @Value("${metric-wallet.allocation.max-per-wallet:0.90}") double maxPerWallet,
            @Value("${metric-wallet.allocation.sync-enabled:false}") boolean syncDistributionEnabled,
            @Value("${metric-wallet.history.source:joyas}") String historySource,
            @Value("${metric-wallet.joyas.limit:3}") int joyasLimit,
            @Value("${metric-wallet.joyas.dayz:30}") int joyasDayz,
            @Value("${metric-wallet.joyas.simulation:summary}") String joyasSimulation,
            @Value("${metric-wallet.history.min-history-days:1}") double minHistoryDays,
            @Value("${metric-wallet.copy-guard.enabled:true}") boolean copyGuardEnabled,
            @Value("${metric-wallet.copy-guard.fail-open-on-missing-metric:false}") boolean copyGuardFailOpenOnMissingMetric,
            @Value("${metric-wallet.copy-guard.require-window-data:true}") boolean copyGuardRequireWindowData,
            @Value("${metric-wallet.copy-guard.min-total-pnl-usdt:0}") double copyGuardMinTotalPnlUsdt,
            @Value("${metric-wallet.copy-guard.min-window-pnl-usdt:0}") double copyGuardMinWindowPnlUsdt,
            @Value("${metric-wallet.copy-guard.windows:2w,1mo}") String copyGuardWindows
    ) {
        this.metricWalletsInfoClient = Objects.requireNonNull(metricWalletsInfoClient, "metricWalletsInfoClient");
        this.userCopyAllocationService = Objects.requireNonNull(userCopyAllocationService, "userCopyAllocationService");
        this.copyStrategyRuntimeRouter = Objects.requireNonNull(copyStrategyRuntimeRouter, "copyStrategyRuntimeRouter");
        this.dayzLimit = dayzLimit;

        if (historyLimit <= 0) throw new IllegalArgumentException("metric-wallet.history.limit must be > 0");
        if (cacheMaxSize <= 0) throw new IllegalArgumentException("metric-wallet.history.cache.max-size must be > 0");

        this.historyLimit = historyLimit;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheRefreshAfter = cacheRefreshAfter;
        this.cacheExpireAfter = cacheExpireAfter;
        this.slowThreshold = slowThreshold;

        this.allPositionHistoryCache = buildHistoryCache();
        this.maxCapitalToUse = maxCapitalToUse;
        this.maxPerWallet = maxPerWallet;
        this.syncDistributionEnabled = syncDistributionEnabled;
        this.historySource = historySource == null || historySource.isBlank() ? "joyas" : historySource.trim().toLowerCase();
        this.joyasLimit = Math.max(1, joyasLimit);
        this.joyasDayz = Math.max(0, joyasDayz);
        this.joyasSimulation = joyasSimulation == null || joyasSimulation.isBlank() ? "summary" : joyasSimulation.trim();
        this.minHistoryDays = Math.max(0.0, minHistoryDays);
        this.copyGuardEnabled = copyGuardEnabled;
        this.copyGuardFailOpenOnMissingMetric = copyGuardFailOpenOnMissingMetric;
        this.copyGuardRequireWindowData = copyGuardRequireWindowData;
        this.copyGuardMinTotalPnlUsdt = copyGuardMinTotalPnlUsdt;
        this.copyGuardMinWindowPnlUsdt = copyGuardMinWindowPnlUsdt;
        this.copyGuardWindows = parseGuardWindows(copyGuardWindows);

        log.info(
                "event=metric_wallets.config historyLimit={} historySource={} joyasLimit={} joyasDayz={} joyasSimulation={} minHistoryDays={} copyGuardEnabled={} copyGuardWindows={} copyGuardMinWindowPnlUsdt={} copyGuardMinTotalPnlUsdt={} copyGuardRequireWindowData={} copyGuardFailOpenOnMissingMetric={} cacheMaxSize={} refreshAfter={} expireAfter={} slowThreshold={} syncDistributionEnabled={}",
                this.historyLimit, this.historySource, this.joyasLimit, this.joyasDayz, this.joyasSimulation, this.minHistoryDays, this.copyGuardEnabled, this.copyGuardWindows, this.copyGuardMinWindowPnlUsdt, this.copyGuardMinTotalPnlUsdt, this.copyGuardRequireWindowData, this.copyGuardFailOpenOnMissingMetric, this.cacheMaxSize, this.cacheRefreshAfter, this.cacheExpireAfter, this.slowThreshold, this.syncDistributionEnabled
        );
    }

    @Override
    public List<MetricaWalletDto> getMetricWallets() {
        return getMetricWallets(maxCapitalToUse, maxPerWallet);
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

        final Map<String, MetricaWalletDto> metricByAllocationKey = new HashMap<>();
        final Map<String, MetricaWalletDto> metricByWalletId = new HashMap<>();
        for (MetricaWalletDto m : historyValues) {
            if (m == null || m.getWallet() == null || m.getWallet().getIdWallet() == null) {
                continue;
            }
            final String walletId = normalizeWalletId(m.getWallet().getIdWallet());
            final String allocationKey = copyStrategyRuntimeRouter.allocationKey(walletId, copyStrategyRuntimeRouter.strategyCodeOf(m));
            if (allocationKey != null) {
                metricByAllocationKey.put(allocationKey, m);
            }
            metricByWalletId.putIfAbsent(walletId, m);
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
                    final String strategyCode = copyStrategyRuntimeRouter.strategyCodeOf(a);
                    final String allocationKey = copyStrategyRuntimeRouter.allocationKey(walletId, strategyCode);
                    if (walletId == null) {
                        log.warn("event=metric_wallets.candidates.skip_allocation userId={} reason=wallet_null allocationId={}", idUser, a.getId());
                        return null;
                    }

                    final double allocationPct = Optional.ofNullable(a.getAllocationPct())
                            .map(java.math.BigDecimal::doubleValue)
                            .orElse(0.0);

                    final MetricaWalletDto cached = allocationKey == null
                            ? metricByWalletId.get(walletId)
                            : metricByAllocationKey.get(allocationKey);
                    if (cached != null) {
                        final CopyStrategyGuardDecision guardDecision = evaluateCopyGuard(cached);
                        if (!guardDecision.allowed()) {
                            log.warn(
                                    "event=metric_wallets.candidates.skip_allocation userId={} wallet={} strategy={} reason={} detail={} allocationPct={} historySource={}",
                                    idUser, walletId, strategyCode, guardDecision.reason(), guardDecision.detail(), allocationPct, historySource
                            );
                            return null;
                        }

                        MetricaWalletDto out = new MetricaWalletDto();
                        BeanUtils.copyProperties(cached, out);
                        out.setCapitalShare(allocationPct);

                        log.info(
                                "event=metric_wallets.candidates.match userId={} wallet={} strategy={} allocationPct={} hasScoring={} decisionMetricConservative={} historySource={}",
                                idUser,
                                walletId,
                                strategyCode,
                                allocationPct,
                                out.getScoring() != null,
                                out.getScoring() != null ? out.getScoring().getDecisionMetricConservative() : null,
                                historySource
                        );
                        return out;
                    }

                    if (copyGuardEnabled && !copyGuardFailOpenOnMissingMetric) {
                        log.warn(
                                "event=metric_wallets.candidates.skip_allocation userId={} wallet={} strategy={} allocationPct={} historySource={} reason=metric_missing_fail_closed",
                                idUser,
                                walletId,
                                strategyCode,
                                allocationPct,
                                historySource
                        );
                        return null;
                    }

                    log.warn(
                            "event=metric_wallets.candidates.placeholder userId={} wallet={} strategy={} allocationPct={} historySource={} note=allocation_present_but_metric_not_cached failOpenOnMissingMetric={}",
                            idUser,
                            walletId,
                            strategyCode,
                            allocationPct,
                            historySource,
                            copyGuardFailOpenOnMissingMetric
                    );

                    return MetricaWalletDto.builder()
                            .wallet(MetricaWalletDto.WalletDto.builder().idWallet(walletId).build())
                            .strategy(MetricaWalletDto.StrategyDto.builder()
                                    .strategyCode(strategyCode)
                                    .strategySlug(a.getCopyStrategySlug())
                                    .strategyLabel(a.getCopyStrategyLabel())
                                    .copyMode(a.getCopyMode())
                                    .sourceEndpoint(a.getStrategySourceEndpoint())
                                    .rankWithinStrategy(a.getRankWithinStrategy())
                                    .globalRank(a.getGlobalRank())
                                    .score(a.getStrategyScore() == null ? null : a.getStrategyScore().doubleValue())
                                    .build())
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


    @Override
    public boolean isCopyStrategyHealthyForCopy(String walletId, String strategyCode) {
        return evaluateCopyStrategyForCopy(walletId, strategyCode).allowed();
    }

    @Override
    public CopyStrategyGuardDecision evaluateCopyStrategyForCopy(String walletId, String strategyCode) {
        if (!copyGuardEnabled) {
            return CopyStrategyGuardDecision.allow();
        }

        final String walletKey = normalizeWalletId(walletId);
        final String strategyKey = copyStrategyRuntimeRouter.strategyCodeOf(
                UserCopyAllocationEntity.builder().copyStrategyCode(strategyCode).build()
        );
        if (walletKey == null) {
            return CopyStrategyGuardDecision.blocked("WALLET_MISSING", "walletId is blank");
        }

        final HistoryResult history = getHistory(historyLimit);
        final List<MetricaWalletDto> values = history == null ? List.of() : history.values();
        final MetricaWalletDto metric = findMetric(values, walletKey, strategyKey);
        if (metric == null) {
            final boolean allowed = copyGuardFailOpenOnMissingMetric;
            log.warn(
                    "event=metric_wallets.copy_guard.missing_metric wallet={} strategy={} allowed={} historySource={} historySize={}",
                    walletKey,
                    strategyKey,
                    allowed,
                    history == null ? "null" : history.source(),
                    values.size()
            );
            return allowed
                    ? CopyStrategyGuardDecision.allow()
                    : CopyStrategyGuardDecision.blocked("METRIC_MISSING", "metric snapshot missing for wallet+strategy");
        }

        final CopyStrategyGuardDecision decision = evaluateCopyGuard(metric);
        if (!decision.allowed()) {
            log.warn(
                    "event=metric_wallets.copy_guard.block wallet={} strategy={} reason={} detail={}",
                    walletKey,
                    strategyKey,
                    decision.reason(),
                    decision.detail()
            );
        }
        return decision;
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

        syncDistributionIfEnabled(candidates, history.source());

        return candidates;
    }


    private void syncDistributionIfEnabled(List<MetricaWalletDto> candidates, String historySource) {
        if (!syncDistributionEnabled) {
            log.debug("event=user_copy_allocation.sync_skipped reason=external_allocator_enabled source={}", historySource);
            return;
        }
        if (!"cache".equals(historySource)) {
            log.warn("event=user_copy_allocation.sync_skipped reason=non_fresh_history source={}", historySource);
            return;
        }
        try {
            userCopyAllocationService.syncDistribution(candidates);
        } catch (EngineException | DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            log.warn("event=user_copy_allocation.sync_failed errClass={} errMsg=\"{}\"", ex.getClass().getSimpleName(), safeErr(ex));
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void primeHistoryCache() {
        try {
            List<MetricaWalletDto> v = allPositionHistoryCache.get(historyLimit);
            log.info("event=metric_wallets.cache_primed limit={} size={}", historyLimit, v.size());
        } catch (EngineException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            log.warn("event=metric_wallets.cache_prime_failed limit={} errClass={} errMsg=\"{}\"", historyLimit, ex.getClass().getSimpleName(), safeErr(ex));
        }
    }

    @Scheduled(fixedDelayString = "${metric-wallet.history.cache.refresh-job:5m}")
    public void refreshHistoryCacheJob() {
        try {
            allPositionHistoryCache.refresh(historyLimit);
            log.debug("event=metric_wallets.cache_refresh_triggered limit={}", historyLimit);
        } catch (EngineException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            log.warn("event=metric_wallets.cache_refresh_failed limit={} errClass={} errMsg=\"{}\"", historyLimit, ex.getClass().getSimpleName(), safeErr(ex));
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
            if ("joyas".equals(historySource)) {
                resp = metricWalletsInfoClient.joyas(limit, joyasLimit, joyasDayz, joyasSimulation);
            } else {
                resp = metricWalletsInfoClient.allPositionHistory(limit, dayz);
            }
        } catch (RestClientException | IllegalStateException | IllegalArgumentException ex) {
            long durationMs = elapsedMs(startNs);
            log.warn("event=metric_wallets.history_client_failed source={} limit={} dayz={} joyasLimit={} joyasDayz={} durationMs={} errClass={} errMsg=\"{}\"", historySource, limit, dayz, joyasLimit, joyasDayz, durationMs, ex.getClass().getSimpleName(), safeErr(ex));
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

        log.debug("event=metric_wallets.history_loaded source={} limit={} size={} durationMs={}", historySource, limit, size, durationMs);
        return resp;
    }

    private HistoryResult getHistory(int limit) {
        try {
            List<MetricaWalletDto> v = Optional.ofNullable(allPositionHistoryCache.get(limit)).orElse(List.of());
            if (!v.isEmpty()) return new HistoryResult(v, "cache");
        } catch (EngineException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            log.warn(
                    "event=metric_wallets.history_load_failed limit={} errClass={} errMsg=\"{}\" cacheStats={}",
                    limit, ex.getClass().getSimpleName(), safeErr(ex), allPositionHistoryCache.stats()
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
                .filter(copyStrategyRuntimeRouter::isCopyableJoyasCandidate)
                .filter(this::passesCopySimulationGuard)
                .filter(MetricWalletServiceImpl::hasClosedHistory)
                .filter(dto -> dto.getWallet() != null && dto.getWallet().getHistoryDays() != null)
                .filter(dto -> {
                    var s = dto.getScoring();
                    return gte(s.getDecisionMetricConservative(), 60)
                            || gte(s.getDecisionMetricScalping(), 65)
                            || gte(s.getDecisionMetricAggressive(), 65);
                })
                .filter(dto -> {
                    double d = dto.getWallet().getHistoryDays();
                    return Double.isFinite(d) && d >= Math.min(dayzLimit, minHistoryDays);
                })
                .sorted(Comparator.comparingDouble(MetricWalletServiceImpl::decisionScore).reversed())
                .map(this::copyForAllocation)
                .toList();
    }


    private boolean passesCopySimulationGuard(MetricaWalletDto dto) {
        CopyStrategyGuardDecision decision = evaluateCopyGuard(dto);
        if (!decision.allowed()) {
            log.info(
                    "event=metric_wallets.candidate_filtered reason={} detail={} wallet={} strategy={}",
                    decision.reason(),
                    decision.detail(),
                    dto == null || dto.getWallet() == null ? null : dto.getWallet().getIdWallet(),
                    copyStrategyRuntimeRouter.strategyCodeOf(dto)
            );
        }
        return decision.allowed();
    }

    private CopyStrategyGuardDecision evaluateCopyGuard(MetricaWalletDto dto) {
        if (!copyGuardEnabled) {
            return CopyStrategyGuardDecision.allow();
        }
        if (dto == null) {
            return CopyStrategyGuardDecision.blocked("METRIC_NULL", "metric dto is null");
        }

        MetricaWalletDto.CopySimulationDto simulation = dto.getCopySimulation();
        if (simulation == null) {
            return copyGuardRequireWindowData
                    ? CopyStrategyGuardDecision.blocked("SIMULATION_MISSING", "copySimulation is missing")
                    : CopyStrategyGuardDecision.allow();
        }

        Double totalNet = simulation.getPnlCopyTotalNetUSDT();
        if (totalNet != null && totalNet < copyGuardMinTotalPnlUsdt) {
            return CopyStrategyGuardDecision.blocked(
                    "NEGATIVE_TOTAL_NET_PNL",
                    "pnlCopyTotalNetUSDT=" + totalNet + " min=" + copyGuardMinTotalPnlUsdt
            );
        }

        for (String window : copyGuardWindows) {
            Double pnl = windowNetPnl(simulation, window);
            if (pnl == null) {
                if (copyGuardRequireWindowData) {
                    return CopyStrategyGuardDecision.blocked(
                            "SIMULATION_WINDOW_MISSING",
                            "window=" + window
                    );
                }
                continue;
            }
            if (pnl < copyGuardMinWindowPnlUsdt) {
                return CopyStrategyGuardDecision.blocked(
                        "NEGATIVE_WINDOW_NET_PNL",
                        "window=" + window + " pnl=" + pnl + " min=" + copyGuardMinWindowPnlUsdt
                );
            }
        }

        return CopyStrategyGuardDecision.allow();
    }

    private static MetricaWalletDto findMetric(List<MetricaWalletDto> values, String walletKey, String strategyCode) {
        if (values == null || values.isEmpty() || walletKey == null) {
            return null;
        }
        final String expectedStrategy = strategyCode == null ? CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE : strategyCode;
        for (MetricaWalletDto metric : values) {
            if (metric == null || metric.getWallet() == null) continue;
            String metricWallet = normalizeWalletId(metric.getWallet().getIdWallet());
            if (!Objects.equals(metricWallet, walletKey)) continue;

            String metricStrategy = strategyCodeFromMetric(metric);
            if (Objects.equals(metricStrategy, expectedStrategy)) {
                return metric;
            }
        }
        return null;
    }

    private static String strategyCodeFromMetric(MetricaWalletDto metric) {
        if (metric == null || metric.getStrategy() == null || metric.getStrategy().getStrategyCode() == null) {
            return CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE;
        }
        return metric.getStrategy().getStrategyCode().trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
    }

    private static Double windowNetPnl(MetricaWalletDto.CopySimulationDto simulation, String window) {
        if (simulation == null || window == null) return null;

        final String key = window.trim();
        if (key.isEmpty()) return null;

        final Map<String, Double> net = simulation.getPnlCopyNet();
        if (net != null) {
            Double exact = net.get(key);
            if (exact != null) return exact;
            Double lower = net.get(key.toLowerCase(java.util.Locale.ROOT));
            if (lower != null) return lower;
        }

        if ("1mo".equalsIgnoreCase(key) || "1m".equalsIgnoreCase(key) || "month".equalsIgnoreCase(key)) {
            return simulation.getPnlCopyMonthUSDT();
        }
        if ("1w".equalsIgnoreCase(key) || "week".equalsIgnoreCase(key)) {
            return simulation.getPnlCopyWeekUSDT();
        }
        if ("1d".equalsIgnoreCase(key) || "day".equalsIgnoreCase(key)) {
            return simulation.getPnlCopyDayUSDT();
        }
        return null;
    }

    private static List<String> parseGuardWindows(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of("2w", "1mo");
        }
        final List<String> windows = java.util.Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .distinct()
                .toList();
        return windows.isEmpty() ? List.of("2w", "1mo") : windows;
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
            throw new EngineException(
                    ErrorCode.INTERNAL_ERROR,
                    String.format("Total capitalShare (%.6f) excede maxCapitalToUse (%.6f)", total, maxCapitalToUse)
            );
        }
        if (max - maxPerWallet > 1e-9) {
            throw new EngineException(
                    ErrorCode.INTERNAL_ERROR,
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
