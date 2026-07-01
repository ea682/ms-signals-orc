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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
    private final double copyGuardPauseTotalPnlUsdt;
    private final double copyGuardPauseWindowPnlUsdt;
    private final double copyGuardOneWeekCapitalMultiplier;
    private final double copyGuardOneMonthCapitalMultiplier;
    private final List<String> copyGuardWindows;

    private final MetricWalletsInfoClient metricWalletsInfoClient;
    private final UserCopyAllocationService userCopyAllocationService;
    private final CopyStrategyRuntimeRouter copyStrategyRuntimeRouter;

    private final AtomicReference<HistorySnapshot> lastKnownGoodHistory = new AtomicReference<>(HistorySnapshot.empty());
    private final AtomicLong lastKnownGoodWarnAtMs = new AtomicLong(0L);

    private final LoadingCache<Integer, HistorySnapshot> allPositionHistoryCache;

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
            @Value("${metric-wallet.copy-guard.pause-total-pnl-usdt:-50}") double copyGuardPauseTotalPnlUsdt,
            @Value("${metric-wallet.copy-guard.pause-window-pnl-usdt:-25}") double copyGuardPauseWindowPnlUsdt,
            @Value("${metric-wallet.copy-guard.one-week-capital-multiplier:0.70}") double copyGuardOneWeekCapitalMultiplier,
            @Value("${metric-wallet.copy-guard.one-month-capital-multiplier:0.25}") double copyGuardOneMonthCapitalMultiplier,
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
        this.copyGuardPauseTotalPnlUsdt = copyGuardPauseTotalPnlUsdt;
        this.copyGuardPauseWindowPnlUsdt = copyGuardPauseWindowPnlUsdt;
        this.copyGuardOneWeekCapitalMultiplier = copyGuardOneWeekCapitalMultiplier;
        this.copyGuardOneMonthCapitalMultiplier = copyGuardOneMonthCapitalMultiplier;
        this.copyGuardWindows = parseGuardWindows(copyGuardWindows);

        log.info(
                "event=metric_wallets.config historyLimit={} historySource={} joyasLimit={} joyasDayz={} joyasSimulation={} minHistoryDays={} copyGuardEnabled={} copyGuardWindows={} copyGuardMinWindowPnlUsdt={} copyGuardMinTotalPnlUsdt={} copyGuardPauseWindowPnlUsdt={} copyGuardPauseTotalPnlUsdt={} copyGuardRequireWindowData={} copyGuardFailOpenOnMissingMetric={} cacheMaxSize={} refreshAfter={} expireAfter={} slowThreshold={} syncDistributionEnabled={}",
                this.historyLimit, this.historySource, this.joyasLimit, this.joyasDayz, this.joyasSimulation, this.minHistoryDays, this.copyGuardEnabled, this.copyGuardWindows, this.copyGuardMinWindowPnlUsdt, this.copyGuardMinTotalPnlUsdt, this.copyGuardPauseWindowPnlUsdt, this.copyGuardPauseTotalPnlUsdt, this.copyGuardRequireWindowData, this.copyGuardFailOpenOnMissingMetric, this.cacheMaxSize, this.cacheRefreshAfter, this.cacheExpireAfter, this.slowThreshold, this.syncDistributionEnabled
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
        final HistoryIndex historyIndex = history == null ? HistoryIndex.empty() : history.index();

        log.debug(
                "event=metric_wallets.candidates.lookup userId={} allocations={} historySource={} historySize={} indexedByAllocation={} indexedByWallet={} sampleAllocations={} sampleHistory={}",
                idUser,
                allocations.size(),
                historySource,
                historyValues.size(),
                historyIndex.allocationSize(),
                historyIndex.walletSize(),
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
                    final String allocationKey = copyStrategyRuntimeRouter.allocationKey(a);
                    if (walletId == null) {
                        log.warn("event=metric_wallets.candidates.skip_allocation userId={} reason=wallet_null allocationId={}", idUser, a.getId());
                        return null;
                    }

                    final double allocationPct = Optional.ofNullable(a.getAllocationPct())
                            .map(java.math.BigDecimal::doubleValue)
                            .orElse(0.0);

                    final MetricaWalletDto cached = allocationKey == null
                            ? historyIndex.findByWallet(walletId)
                            : historyIndex.findByAllocationKey(allocationKey);
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
                        final double effectiveAllocationPct = clamp01(allocationPct * guardDecision.capitalMultiplier());
                        if (effectiveAllocationPct <= 0.0) {
                            log.warn(
                                    "event=metric_wallets.candidates.skip_allocation userId={} wallet={} strategy={} reason=guard_capital_zero action={} allocationPct={} multiplier={} historySource={}",
                                    idUser, walletId, strategyCode, guardDecision.action(), allocationPct, guardDecision.capitalMultiplier(), historySource
                            );
                            return null;
                        }
                        out.setCapitalShare(effectiveAllocationPct);

                        log.debug(
                                "event=metric_wallets.candidates.match userId={} wallet={} strategy={} allocationPct={} effectiveAllocationPct={} guardAction={} guardMultiplier={} guardTargetExecutionMode={} hasScoring={} decisionMetricConservative={} historySource={}",
                                idUser,
                                walletId,
                                strategyCode,
                                allocationPct,
                                effectiveAllocationPct,
                                guardDecision.action(),
                                guardDecision.capitalMultiplier(),
                                guardDecision.targetExecutionMode(),
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

        log.debug(
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
        CopyStrategyGuardDecision decision = evaluateCopyStrategyForCopy(walletId, strategyCode);
        return decision.allowed() && !"SHADOW_ONLY".equals(decision.action());
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
        final HistoryIndex index = history == null ? HistoryIndex.empty() : history.index();
        final MetricaWalletDto metric = index.find(walletKey, strategyKey);
        if (metric == null) {
            final boolean allowed = copyGuardFailOpenOnMissingMetric;
            log.warn(
                    "event=metric_wallets.copy_guard.missing_metric walletId={} strategyCode={} allowed={} historySource={} historySize={} indexedByAllocation={} indexedByWallet={} reasonCode=METRIC_MISSING copyImpact={}",
                    walletKey,
                    strategyKey,
                    allowed,
                    history == null ? "null" : history.source(),
                    values.size(),
                    index.allocationSize(),
                    index.walletSize(),
                    allowed ? "fail_open_allowed" : "no_new_live_open"
            );
            return allowed
                    ? CopyStrategyGuardDecision.allow()
                    : CopyStrategyGuardDecision.blocked("METRIC_MISSING", "metric snapshot missing for wallet+strategy");
        }

        final CopyStrategyGuardDecision decision = evaluateCopyGuard(metric);
        if (!decision.allowed()) {
            log.warn(
                    "event=metric_wallets.copy_guard.block walletId={} strategyCode={} reasonCode={} detail={} copyImpact=no_new_live_open",
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
        applyGuardCapitalMultipliers(candidates);
        validateLimits(candidates, maxPerWallet, maxCapitalToUse);

        syncDistributionIfEnabled(candidates, history.values(), history.source());

        return candidates;
    }


    private void syncDistributionIfEnabled(List<MetricaWalletDto> liveCandidates, List<MetricaWalletDto> shadowCandidates, String historySource) {
        if (!syncDistributionEnabled) {
            log.debug("event=user_copy_allocation.sync_skipped reason=external_allocator_enabled source={}", historySource);
            return;
        }
        if (!"cache".equals(historySource)) {
            log.warn("event=user_copy_allocation.sync_skipped reason=non_fresh_history source={}", historySource);
            return;
        }
        try {
            userCopyAllocationService.syncDistribution(liveCandidates, shadowCandidates);
        } catch (EngineException | DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            log.warn("event=user_copy_allocation.sync_failed errClass={} errMsg=\"{}\"", ex.getClass().getSimpleName(), safeErr(ex));
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void primeHistoryCache() {
        try {
            HistorySnapshot snapshot = allPositionHistoryCache.get(historyLimit);
            log.info(
                    "event=metric_wallets.cache_primed limit={} size={} indexedByAllocation={} indexedByWallet={}",
                    historyLimit,
                    snapshot.size(),
                    snapshot.index().allocationSize(),
                    snapshot.index().walletSize()
            );
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

    private LoadingCache<Integer, HistorySnapshot> buildHistoryCache() {
        return Caffeine.newBuilder()
                .maximumSize(cacheMaxSize)
                .refreshAfterWrite(cacheRefreshAfter)
                .expireAfterWrite(cacheExpireAfter)
                .recordStats()
                .build(this::loadAllPositionHistory);
    }

    private HistorySnapshot loadAllPositionHistory(Integer limit) {
        return loadAllPositionHistory(limit, dayzLimit);
    }

    private HistorySnapshot loadAllPositionHistory(Integer limit, Integer dayz) {
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
            return HistorySnapshot.empty();
        }

        long durationMs = elapsedMs(startNs);
        int size = resp == null ? 0 : resp.size();

        if (resp == null) {
            log.warn("event=metric_wallets.history_null limit={} durationMs={}", limit, durationMs);
            return HistorySnapshot.empty();
        }

        if (!resp.isEmpty()) {
            HistorySnapshot snapshot = HistorySnapshot.from(resp, copyStrategyRuntimeRouter);
            lastKnownGoodHistory.set(snapshot);
            log.debug(
                    "event=metric_wallets.history_loaded source={} limit={} size={} indexedByAllocation={} indexedByWallet={} durationMs={}",
                    historySource,
                    limit,
                    snapshot.size(),
                    snapshot.index().allocationSize(),
                    snapshot.index().walletSize(),
                    durationMs
            );
            return snapshot;
        } else {
            log.warn("event=metric_wallets.history_empty_response limit={} durationMs={}", limit, durationMs);
        }

        log.debug("event=metric_wallets.history_loaded source={} limit={} size={} durationMs={}", historySource, limit, size, durationMs);
        return HistorySnapshot.empty();
    }

    private HistoryResult getHistory(int limit) {
        try {
            HistorySnapshot snapshot = Optional.ofNullable(allPositionHistoryCache.get(limit)).orElse(HistorySnapshot.empty());
            if (!snapshot.isEmpty()) return new HistoryResult(snapshot, "cache");
        } catch (EngineException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            log.warn(
                    "event=metric_wallets.history_load_failed limit={} errClass={} errMsg=\"{}\" cacheStats={}",
                    limit, ex.getClass().getSimpleName(), safeErr(ex), allPositionHistoryCache.stats()
            );
        }

        HistorySnapshot present = allPositionHistoryCache.getIfPresent(limit);
        if (present != null && !present.isEmpty()) {
            return new HistoryResult(present, "cache_if_present");
        }

        HistorySnapshot lkg = lastKnownGoodHistory.get();
        if (lkg != null && !lkg.isEmpty()) {
            warnLastKnownGoodIfDue(limit, lkg);
            return new HistoryResult(lkg, "last_known_good");
        }

        return new HistoryResult(HistorySnapshot.empty(), "empty");
    }

    private List<MetricaWalletDto> selectCandidates(List<MetricaWalletDto> base, int dayzLimit) {
        return base.stream()
                .filter(Objects::nonNull)
                .filter(dto -> dto.getScoring() != null)
                .filter(copyStrategyRuntimeRouter::isShadowEligibleJoyasCandidate)
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
                    "event=metric_wallets.candidate_filtered reasonCode={} detail={} walletId={} strategyCode={} copyImpact=no_live_candidate",
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
        String walletId = dto.getWallet() == null ? null : dto.getWallet().getIdWallet();
        String strategyCode = copyStrategyRuntimeRouter.strategyCodeOf(dto);

        CopyStrategyGuardDecision realJewelDecision = decisionFromMetricCopyGuard(dto);
        if (realJewelDecision != null) {
            return realJewelDecision;
        }

        MetricaWalletDto.CopySimulationDto simulation = dto.getCopySimulation();
        if (simulation == null) {
            return copyGuardRequireWindowData
                    ? CopyStrategyGuardDecision.blocked("SIMULATION_MISSING", "copySimulation is missing")
                    : CopyStrategyGuardDecision.allow();
        }
        if (simulationAuditFailed(dto)) {
            return CopyStrategyGuardDecision.blocked(
                    "SIMULATION_AUDIT_FAILED",
                    "simulationAudit.valid=false errors=" + simulationAuditErrors(dto)
            );
        }

        Double totalNet = simulation.getPnlCopyTotalNetUSDT();
        if (totalNet != null && totalNet < copyGuardMinTotalPnlUsdt) {
            String detail = "pnlCopyTotalNetUSDT=" + totalNet + " min=" + copyGuardMinTotalPnlUsdt + " pauseAt=" + copyGuardPauseTotalPnlUsdt;
            return totalNet <= copyGuardPauseTotalPnlUsdt
                    ? CopyStrategyGuardDecision.blocked("NEGATIVE_TOTAL_NET_PNL", detail)
                    : CopyStrategyGuardDecision.reduce("NEGATIVE_TOTAL_NET_PNL", detail, 0.5);
        }

        for (String window : copyGuardWindows) {
            Boolean complete = windowComplete(simulation, window);
            if (Boolean.FALSE.equals(complete) && copyGuardRequireWindowData) {
                return CopyStrategyGuardDecision.blocked(
                        "INCOMPLETE_REQUIRED_WINDOW_" + windowReasonCode(window),
                        "window=" + window + " complete=false"
                );
            }
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
                String detail = "window=" + window + " pnl=" + pnl + " min=" + copyGuardMinWindowPnlUsdt + " pauseAt=" + copyGuardPauseWindowPnlUsdt;
                String normalizedWindow = window == null ? "" : window.trim().toLowerCase(java.util.Locale.ROOT);
                String requiredWindowReason = requiredWindowReason(window, pnl);
                if ("1w".equals(normalizedWindow)) {
                    log.info("event=guard_1w_warning walletId={} strategyCode={} window={} pnl={} reasonCode=NEGATIVE_1W_NET_PNL action=REDUCE_CAPITAL copyImpact=capital_reduced",
                            walletId, strategyCode, window, pnl);
                    return CopyStrategyGuardDecision.reduce("NEGATIVE_1W_NET_PNL", detail, copyGuardOneWeekCapitalMultiplier);
                }
                if ("2w".equals(normalizedWindow)) {
                    log.warn("event=guard_2w_pause_open walletId={} strategyCode={} window={} pnl={} reasonCode={} action=PAUSE_OPEN copyImpact=no_new_live_open",
                            walletId, strategyCode, window, pnl, requiredWindowReason);
                    return CopyStrategyGuardDecision.blocked(requiredWindowReason, detail);
                }
                if ("1mo".equals(normalizedWindow)) {
                    log.warn("event=guard_1mo_shadow_only walletId={} strategyCode={} window={} pnl={} reasonCode={} action=SHADOW_ONLY copyImpact=shadow_only",
                            walletId, strategyCode, window, pnl, requiredWindowReason);
                    return CopyStrategyGuardDecision.shadowOnly(requiredWindowReason, detail, copyGuardOneMonthCapitalMultiplier);
                }
                return pnl <= copyGuardPauseWindowPnlUsdt
                        ? CopyStrategyGuardDecision.blocked(requiredWindowReason, detail)
                        : CopyStrategyGuardDecision.reduce(requiredWindowReason, detail, 0.5);
            }
        }

        return CopyStrategyGuardDecision.allow();
    }

    private String requiredWindowReason(String window, Double pnl) {
        String suffix = windowReasonCode(window);
        if (pnl == null || !Double.isFinite(pnl)) {
            return "SIMULATION_WINDOW_MISSING";
        }
        if (pnl < 0.0) {
            return "NEGATIVE_REQUIRED_WINDOW_" + suffix;
        }
        return "NON_POSITIVE_REQUIRED_WINDOW_" + suffix;
    }

    private CopyStrategyGuardDecision decisionFromMetricCopyGuard(MetricaWalletDto dto) {
        MetricaWalletDto.CopyGuardDto guard = metricCopyGuard(dto);
        if (guard == null || guard.getAction() == null || guard.getAction().isBlank()) {
            return null;
        }
        String action = guard.getAction().trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        String status = guard.getStatus() == null ? "" : guard.getStatus().trim().toUpperCase(java.util.Locale.ROOT);
        String detail = "status=" + status
                + " reasons=" + String.join("|", guard.getReasons() == null ? List.of() : guard.getReasons())
                + " severity=" + guard.getSeverityScore();
        double multiplier = guard.getCapitalMultiplier() == null ? 1.0 : guard.getCapitalMultiplier();
        boolean allowNewEntries = !Boolean.FALSE.equals(guard.getAllowNewEntries());

        if ("DISABLED".equals(action) || "DISABLED".equals(status)) {
            return CopyStrategyGuardDecision.disabled("METRIC_COPY_GUARD_DISABLED", detail);
        }
        if ("PAUSE_OPEN".equals(action) || !allowNewEntries) {
            return CopyStrategyGuardDecision.blocked("METRIC_COPY_GUARD_PAUSE_OPEN", detail);
        }
        if ("SHADOW_ONLY".equals(action) || "SHADOW_ONLY".equals(status)) {
            return CopyStrategyGuardDecision.shadowOnly("METRIC_COPY_GUARD_SHADOW_ONLY", detail, multiplier);
        }
        if ("REDUCE_CAPITAL".equals(action) || "REDUCE_CAPITAL".equals(status) || "HIGH_RISK".equals(status)) {
            return CopyStrategyGuardDecision.reduce("METRIC_COPY_GUARD_REDUCE_CAPITAL", detail, multiplier);
        }
        if ("WARNING".equals(action) || "WATCHLIST".equals(status) || "DATA_RISK".equals(status)) {
            return CopyStrategyGuardDecision.warn("METRIC_COPY_GUARD_WARNING", detail, multiplier);
        }
        return null;
    }

    private static MetricaWalletDto.CopyGuardDto metricCopyGuard(MetricaWalletDto dto) {
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

    private void applyGuardCapitalMultipliers(List<MetricaWalletDto> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return;
        }
        for (MetricaWalletDto candidate : candidates) {
            CopyStrategyGuardDecision decision = evaluateCopyGuard(candidate);
            double before = candidate == null ? 0.0 : candidate.getCapitalShare();
            if (candidate != null && decision.allowed() && decision.capitalMultiplier() < 1.0) {
                double after = clamp01(before * decision.capitalMultiplier());
                candidate.setCapitalShare(after);
                log.info("event=metric_wallets.capital_guard_applied walletId={} strategyCode={} action={} multiplier={} capitalShareBefore={} capitalShareAfter={} reasonCode={} detail={} copyImpact=capital_reduced",
                        candidate.getWallet() == null ? null : candidate.getWallet().getIdWallet(),
                        copyStrategyRuntimeRouter.strategyCodeOf(candidate),
                        decision.action(),
                        decision.capitalMultiplier(),
                        before,
                        after,
                        decision.reason(),
                        decision.detail());
            }
        }
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

    private static String scopeTypeFromMetric(MetricaWalletDto metric) {
        String fromBreakdown = metric != null && metric.getWallet() != null && metric.getWallet().getCountOperationBreakdown() != null
                ? metric.getWallet().getCountOperationBreakdown().getScopeType()
                : null;
        String fromJewel = metric != null && metric.getRealJewel() != null ? metric.getRealJewel().getScopeType() : null;
        String value = firstNonBlank(fromBreakdown, fromJewel, "strategy");
        return value == null ? "strategy" : value.trim().toLowerCase(java.util.Locale.ROOT);
    }

    private static String scopeValueFromMetric(MetricaWalletDto metric, String strategyCode) {
        String fromBreakdown = metric != null && metric.getWallet() != null && metric.getWallet().getCountOperationBreakdown() != null
                ? metric.getWallet().getCountOperationBreakdown().getScopeValue()
                : null;
        String fromJewel = metric != null && metric.getRealJewel() != null ? metric.getRealJewel().getScopeValue() : null;
        String effectiveStrategy = normalizeStrategyCode(strategyCode);
        String value = firstNonBlank(fromBreakdown, fromJewel, effectiveStrategy);
        return value == null || value.isBlank() ? effectiveStrategy : value.trim();
    }

    private static String normalizeStrategyCode(String strategyCode) {
        if (strategyCode == null || strategyCode.isBlank()) {
            return CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE;
        }
        return strategyCode.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
    }

    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    private static boolean simulationAuditFailed(MetricaWalletDto dto) {
        Map<String, Object> audit = simulationAudit(dto);
        if (audit == null || audit.isEmpty()) return false;
        Object valid = audit.get("valid");
        return Boolean.FALSE.equals(valid) || "false".equalsIgnoreCase(String.valueOf(valid));
    }

    private static String simulationAuditErrors(MetricaWalletDto dto) {
        Map<String, Object> audit = simulationAudit(dto);
        if (audit == null) return "";
        Object errors = audit.get("errors");
        return errors == null ? "" : String.valueOf(errors);
    }

    private static Map<String, Object> simulationAudit(MetricaWalletDto dto) {
        if (dto == null) return null;
        if (dto.getSimulationAudit() != null) return dto.getSimulationAudit();
        MetricaWalletDto.CopySimulationDto simulation = dto.getCopySimulation();
        if (simulation != null) {
            if (simulation.getSimulationAudit() != null) return simulation.getSimulationAudit();
            MetricaWalletDto.CopySizingDto sizing = simulation.getCopySizing();
            if (sizing != null && sizing.getSimulationAudit() != null) return sizing.getSimulationAudit();
        }
        return null;
    }

    private static Boolean windowComplete(MetricaWalletDto.CopySimulationDto simulation, String window) {
        Map<String, Object> meta = windowMeta(simulation);
        if (meta == null || meta.isEmpty() || window == null || window.isBlank()) return null;
        Object item = meta.get(window.trim());
        if (item == null) item = meta.get(window.trim().toLowerCase(java.util.Locale.ROOT));
        if (!(item instanceof Map<?, ?> itemMap)) return null;
        Object complete = itemMap.get("complete");
        if (complete instanceof Boolean b) return b;
        if (complete == null) return null;
        if ("true".equalsIgnoreCase(String.valueOf(complete))) return true;
        if ("false".equalsIgnoreCase(String.valueOf(complete))) return false;
        return null;
    }

    private static Map<String, Object> windowMeta(MetricaWalletDto.CopySimulationDto simulation) {
        if (simulation == null) return null;
        if (simulation.getWindowMeta() != null) return simulation.getWindowMeta();
        MetricaWalletDto.CopySizingDto sizing = simulation.getCopySizing();
        return sizing == null ? null : sizing.getWindowMeta();
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

    private static String windowReasonCode(String window) {
        if (window == null || window.isBlank()) return "UNKNOWN";
        return window.trim().toUpperCase(java.util.Locale.ROOT).replaceAll("[^A-Z0-9]+", "_");
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

    private static double clamp01(double value) {
        if (!Double.isFinite(value)) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(1.0, value));
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

    private void warnLastKnownGoodIfDue(int limit, HistorySnapshot snapshot) {
        long now = System.currentTimeMillis();
        long prev = lastKnownGoodWarnAtMs.get();
        if (now - prev < 60_000L || !lastKnownGoodWarnAtMs.compareAndSet(prev, now)) {
            return;
        }
        log.warn(
                "event=metric_wallets.history_lkg_used limit={} size={} indexedByAllocation={} indexedByWallet={} reasonCode=cache_empty_or_refresh_failed copyImpact=uses_stale_metric_snapshot",
                limit,
                snapshot.size(),
                snapshot.index().allocationSize(),
                snapshot.index().walletSize()
        );
    }

    private static final class HistorySnapshot {
        private static final HistorySnapshot EMPTY = new HistorySnapshot(List.of(), HistoryIndex.empty());

        private final List<MetricaWalletDto> values;
        private final HistoryIndex index;

        private HistorySnapshot(List<MetricaWalletDto> values, HistoryIndex index) {
            this.values = values == null ? List.of() : values;
            this.index = index == null ? HistoryIndex.empty() : index;
        }

        static HistorySnapshot empty() {
            return EMPTY;
        }

        static HistorySnapshot from(List<MetricaWalletDto> values, CopyStrategyRuntimeRouter router) {
            List<MetricaWalletDto> snapshot = values == null ? List.of() : List.copyOf(values);
            return snapshot.isEmpty()
                    ? empty()
                    : new HistorySnapshot(snapshot, HistoryIndex.from(snapshot, router));
        }

        List<MetricaWalletDto> values() {
            return values;
        }

        HistoryIndex index() {
            return index;
        }

        int size() {
            return values.size();
        }

        boolean isEmpty() {
            return values.isEmpty();
        }
    }

    private static final class HistoryIndex {
        private static final HistoryIndex EMPTY = new HistoryIndex(Map.of(), Map.of());

        private final Map<String, MetricaWalletDto> byAllocationKey;
        private final Map<String, MetricaWalletDto> byWalletId;

        private HistoryIndex(Map<String, MetricaWalletDto> byAllocationKey,
                             Map<String, MetricaWalletDto> byWalletId) {
            this.byAllocationKey = byAllocationKey == null ? Map.of() : Map.copyOf(byAllocationKey);
            this.byWalletId = byWalletId == null ? Map.of() : Map.copyOf(byWalletId);
        }

        static HistoryIndex empty() {
            return EMPTY;
        }

        static HistoryIndex from(List<MetricaWalletDto> values, CopyStrategyRuntimeRouter router) {
            if (values == null || values.isEmpty()) {
                return empty();
            }
            Map<String, MetricaWalletDto> byAllocationKey = new java.util.HashMap<>();
            Map<String, MetricaWalletDto> byWalletId = new java.util.HashMap<>();
            for (MetricaWalletDto metric : values) {
                if (metric == null || metric.getWallet() == null) {
                    continue;
                }
                String walletId = normalizeWalletId(metric.getWallet().getIdWallet());
                if (walletId == null) {
                    continue;
                }
                byWalletId.putIfAbsent(walletId, metric);
                String strategyCode = router == null ? strategyCodeFromMetric(metric) : router.strategyCodeOf(metric);
                String allocationKey = router == null
                        ? walletId + "|" + strategyCodeFromMetric(metric) + "|" + scopeTypeFromMetric(metric) + "|" + scopeValueFromMetric(metric, strategyCode)
                        : router.allocationKey(walletId, strategyCode, scopeTypeFromMetric(metric), scopeValueFromMetric(metric, strategyCode));
                if (allocationKey != null) {
                    byAllocationKey.put(allocationKey, metric);
                }
            }
            return byAllocationKey.isEmpty() && byWalletId.isEmpty()
                    ? empty()
                    : new HistoryIndex(byAllocationKey, byWalletId);
        }

        MetricaWalletDto find(String walletId, String strategyCode) {
            String normalizedWallet = normalizeWalletId(walletId);
            if (normalizedWallet == null) {
                return null;
            }
            String normalizedStrategy = strategyCode == null ? CopyStrategyRuntimeRouter.DEFAULT_STRATEGY_CODE : strategyCode;
            MetricaWalletDto direct = byAllocationKey.get(normalizedWallet + "|" + normalizedStrategy + "|strategy|" + normalizedStrategy);
            return direct == null ? byAllocationKey.get(normalizedWallet + "|" + normalizedStrategy + "|all|ALL") : direct;
        }

        MetricaWalletDto findByAllocationKey(String allocationKey) {
            return allocationKey == null ? null : byAllocationKey.get(allocationKey);
        }

        MetricaWalletDto findByWallet(String walletId) {
            String normalizedWallet = normalizeWalletId(walletId);
            return normalizedWallet == null ? null : byWalletId.get(normalizedWallet);
        }

        int allocationSize() {
            return byAllocationKey.size();
        }

        int walletSize() {
            return byWalletId.size();
        }
    }

    private static final class HistoryResult {
        private final List<MetricaWalletDto> values;
        private final String source;
        private final HistoryIndex index;

        private HistoryResult(HistorySnapshot snapshot, String source) {
            this.values = snapshot == null ? List.of() : snapshot.values();
            this.source = source;
            this.index = snapshot == null ? HistoryIndex.empty() : snapshot.index();
        }

        public List<MetricaWalletDto> values() {
            return values;
        }

        public String source() {
            return source;
        }

        public HistoryIndex index() {
            return index;
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
