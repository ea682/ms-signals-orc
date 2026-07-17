package com.apunto.engine.service.metric;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Component
@Slf4j
public class MetricV2SnapshotStore {

    private static final Set<String> ALL_WINDOWS = Set.of(
            "1d", "3d", "1w", "2w", "3w", "1mo", "2mo", "3mo",
            "6mo", "9mo", "1y", "2y", "all"
    );
    private static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(1);

    private final MetricWalletsInfoClient client;
    private final MetricStrategyShadowProjectionMapper mapper;
    private final MetricWalletReadModeResolver modeResolver;
    private final MeterRegistry meters;
    private final MetricV2SnapshotPersistence persistence;
    private final int summaryLimit;
    private final int fullLimit;
    private final int historyDays;
    private final Duration fullRefreshAfter;
    private final Duration guardRefreshAfter;
    private final Duration maxStaleness;
    private final boolean allowCloseOnFailure;
    private final String windows;
    private final AtomicReference<Snapshot> current = new AtomicReference<>(Snapshot.empty());
    private final AtomicBoolean refreshInFlight = new AtomicBoolean(false);

    @Autowired
    public MetricV2SnapshotStore(
            MetricWalletsInfoClient client,
            MetricStrategyShadowProjectionMapper mapper,
            MetricWalletReadModeResolver modeResolver,
            MeterRegistry meters,
            MetricV2SnapshotPersistence persistence,
            @Value("${metric-wallet.v2.summary-limit:${metric-wallet.history.limit:300}}") int summaryLimit,
            @Value("${metric-wallet.v2.full-limit:${metric-wallet.shadow.max-strategies:80}}") int fullLimit,
            @Value("${metric-wallet.joyas.dayz:30}") int historyDays,
            @Value("${metric-wallet.v2.full-refresh-after:PT10M}") Duration fullRefreshAfter,
            @Value("${metric-wallet.v2.copy-guard-refresh-after:PT2M}") Duration guardRefreshAfter,
            @Value("${metric-wallet.v2.max-staleness:PT10M}") Duration maxStaleness,
            @Value("${metric-wallet.v2.fail-open-new-exposure:false}") boolean failOpenNewExposure,
            @Value("${metric-wallet.v2.allow-close-on-failure:true}") boolean allowCloseOnFailure,
            @Value("${metric-wallet.copy-guard.available-windows:1d,3d,1w,2w,3w,1mo,2mo,3mo,6mo,9mo,1y,2y,all}") String windows
    ) {
        this.client = Objects.requireNonNull(client, "client");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.modeResolver = Objects.requireNonNull(modeResolver, "modeResolver");
        this.meters = Objects.requireNonNull(meters, "meters");
        this.persistence = Objects.requireNonNull(persistence, "persistence");
        this.summaryLimit = Math.max(1, summaryLimit);
        this.fullLimit = Math.max(1, fullLimit);
        this.historyDays = Math.max(1, historyDays);
        this.fullRefreshAfter = positive(fullRefreshAfter, Duration.ofMinutes(10));
        this.guardRefreshAfter = positive(guardRefreshAfter, Duration.ofMinutes(2));
        this.maxStaleness = positive(maxStaleness, Duration.ofMinutes(10));
        if (failOpenNewExposure) {
            throw new IllegalStateException("METRIC_WALLET_V2_FAIL_OPEN_NEW_EXPOSURE_MUST_BE_FALSE");
        }
        if (!allowCloseOnFailure) {
            throw new IllegalStateException("METRIC_WALLET_V2_ALLOW_CLOSE_ON_FAILURE_MUST_BE_TRUE");
        }
        this.allowCloseOnFailure = true;
        this.windows = normalizeWindows(windows);
    }

    public MetricV2SnapshotStore(
            MetricWalletsInfoClient client,
            MetricStrategyShadowProjectionMapper mapper,
            MetricWalletReadModeResolver modeResolver,
            MeterRegistry meters,
            int summaryLimit,
            int fullLimit,
            int historyDays,
            Duration fullRefreshAfter,
            Duration guardRefreshAfter,
            Duration maxStaleness,
            boolean failOpenNewExposure,
            boolean allowCloseOnFailure,
            String windows
    ) {
        this(
                client,
                mapper,
                modeResolver,
                meters,
                MetricV2SnapshotPersistence.noOp(),
                summaryLimit,
                fullLimit,
                historyDays,
                fullRefreshAfter,
                guardRefreshAfter,
                maxStaleness,
                failOpenNewExposure,
                allowCloseOnFailure,
                windows
        );
    }

    @PostConstruct
    void restorePersistedSnapshot() {
        if (modeResolver.effectiveMode() == MetricWalletReadMode.V1) return;
        try {
            persistence.load().ifPresent(snapshot -> {
                validateRestored(snapshot);
                current.set(snapshot);
                log.info("event=metric_v2.snapshot.restored summary={} full={} copyGuard={} persisted=true",
                        snapshot.summaryByKey().size(), snapshot.fullByKey().size(), snapshot.guardByKey().size());
            });
        } catch (RuntimeException ex) {
            meters.counter("signals.metric_v2.contract.error.total").increment();
            log.warn("event=metric_v2.snapshot.restore_failed reasonCode=METRIC_V2_PERSISTED_SNAPSHOT_INVALID errClass={} errMsg={} action=START_EMPTY_FAIL_CLOSED",
                    ex.getClass().getSimpleName(), safe(ex.getMessage()));
            current.set(Snapshot.empty());
        }
    }

    public void refreshNow() {
        if (modeResolver.effectiveMode() == MetricWalletReadMode.V1) return;
        if (!refreshInFlight.compareAndSet(false, true)) {
            log.debug("event=metric_v2.refresh.singleflight_join action=skip_duplicate_refresh");
            return;
        }
        long started = System.nanoTime();
        try {
            Snapshot before = current.get();
            List<MetricStrategySnapshotDto> summary = fetch(
                    "summary",
                    () -> client.metricStrategySnapshots(summaryLimit, historyDays, "summary")
            );
            Map<String, MetricStrategySnapshotDto> summaryByKey = validateAndIndex(
                    summary,
                    MetricStrategySnapshotDto.EvaluationMode.SUMMARY,
                    false
            );
            Map<String, String> nextWalletGenerations = generationsByWallet(summaryByKey.values());
            boolean generationChanged = !before.walletGenerations().isEmpty()
                    && !before.walletGenerations().equals(nextWalletGenerations);
            boolean candidateSetChanged = !before.summaryByKey().keySet().equals(summaryByKey.keySet());
            Instant now = Instant.now();
            boolean refreshFull = before.fullByKey().isEmpty()
                    || generationChanged
                    || candidateSetChanged
                    || olderThan(before.fullFetchedAt(), fullRefreshAfter, now);
            boolean refreshGuard = before.guardByKey().isEmpty()
                    || generationChanged
                    || candidateSetChanged
                    || olderThan(before.guardFetchedAt(), guardRefreshAfter, now);

            Map<String, MetricStrategySnapshotDto> fullByKey = refreshFull
                    ? validateAndIndex(fetch(
                            "full",
                            () -> client.metricStrategySnapshots(fullLimit, historyDays, "full")
                    ), MetricStrategySnapshotDto.EvaluationMode.FULL, false)
                    : before.fullByKey();
            Map<String, MetricStrategySnapshotDto> guardByKey = refreshGuard
                    ? validateAndIndex(fetch(
                            "copy_guard",
                            () -> client.metricStrategyCopyGuardWindows(
                                    fullLimit,
                                    historyDays,
                                    "snapshot",
                                    windows
                            )
                    ), MetricStrategySnapshotDto.EvaluationMode.FULL, true)
                    : before.guardByKey();

            assertGenerationCoherence(summaryByKey, fullByKey, guardByKey);
            Snapshot next = new Snapshot(
                    Map.copyOf(summaryByKey),
                    Map.copyOf(fullByKey),
                    Map.copyOf(guardByKey),
                    Map.copyOf(nextWalletGenerations),
                    now,
                    refreshFull ? now : before.fullFetchedAt(),
                    refreshGuard ? now : before.guardFetchedAt()
            );
            persistence.replace(next, maxStaleness);
            current.set(next);
            if (generationChanged) {
                meters.counter("signals.metric_v2.generation.change.total").increment();
                meters.counter("signals.metric_generation.change.total").increment();
            }
            log.info("event=metric_v2.refresh.finished summary={} full={} copyGuard={} generationChanged={} candidateSetChanged={} elapsedMs={} atomicSwap=true",
                    summaryByKey.size(), fullByKey.size(), guardByKey.size(), generationChanged, candidateSetChanged, elapsedMs(started));
        } catch (RuntimeException ex) {
            log.warn("event=metric_v2.refresh.failed reasonCode=METRIC_V2_REFRESH_FAILED errClass={} errMsg={} elapsedMs={} snapshotRetained=true",
                    ex.getClass().getSimpleName(), safe(ex.getMessage()), elapsedMs(started));
            throw ex;
        } finally {
            refreshInFlight.set(false);
        }
    }

    public List<MetricaWalletDto> fullShadowCandidatesCachedOnly() {
        Snapshot snapshot = current.get();
        if (snapshot.fullByKey().isEmpty()) {
            meters.counter("signals.metric_v2.cache.miss.total", "cache", "full").increment();
            return List.of();
        }
        List<MetricaWalletDto> result = new ArrayList<>();
        for (MetricStrategySnapshotDto full : snapshot.fullByKey().values()) {
            CopyStrategyGuardDecision decision = evaluate(
                    full.getWalletId(),
                    full.getStrategyCode(),
                    full.getScopeType(),
                    full.getScopeValue()
            );
            if (decision.allowed()) result.add(mapper.toShadowProjection(full));
        }
        result.sort((left, right) -> Integer.compare(
                rank(left.getStrategy() == null ? null : left.getStrategy().getGlobalRank()),
                rank(right.getStrategy() == null ? null : right.getStrategy().getGlobalRank())
        ));
        return List.copyOf(result);
    }

    public CopyStrategyGuardDecision evaluate(
            String walletId,
            String strategyCode,
            String scopeType,
            String scopeValue
    ) {
        String key = MetricStrategySnapshotDto.canonicalStrategyKey(
                walletId,
                strategyCode,
                scopeType,
                scopeValue
        );
        Snapshot snapshot = current.get();
        MetricStrategySnapshotDto full = snapshot.fullByKey().get(key);
        MetricStrategySnapshotDto guard = snapshot.guardByKey().get(key);
        if (full == null || guard == null) {
            meters.counter("signals.metric_v2.cache.miss.total", "cache", full == null ? "full" : "copy_guard").increment();
            return block(key, "METRIC_V2_CACHE_MISSING", "fullOrGuardSnapshotMissing");
        }
        meters.counter("signals.metric_v2.cache.hit.total", "cache", "decision").increment();
        Instant now = Instant.now();
        if (olderThan(snapshot.fullFetchedAt(), maxStaleness, now)
                || olderThan(snapshot.guardFetchedAt(), maxStaleness, now)
                || staleData(full, now)
                || staleData(guard, now)) {
            return block(key, "METRIC_V2_CACHE_STALE", "snapshotOrDataAsOfExceededMaxStaleness");
        }
        if (!Objects.equals(full.getGenerationId(), guard.getGenerationId())) {
            return block(key, "METRIC_V2_GENERATION_MISMATCH", "fullAndCopyGuardGenerationDiffer");
        }
        if (!full.contractErrors().isEmpty()) {
            return block(key, "METRIC_V2_FULL_CONTRACT_INVALID", String.join(",", full.contractErrors()));
        }
        if (!guard.contractErrors().isEmpty()) {
            return block(key, "METRIC_V2_COPY_GUARD_CONTRACT_INVALID", String.join(",", guard.contractErrors()));
        }
        List<String> simulationMatrixErrors = full.simulationMatrixContractErrors();
        if (!simulationMatrixErrors.isEmpty()) {
            return block(key, simulationMatrixErrors.getFirst(), String.join(",", simulationMatrixErrors));
        }
        if (full.getCoverage() == null || !full.getCoverage().isComplete()
                || guard.getCoverage() == null || !guard.getCoverage().isComplete()) {
            meters.counter("signals.metric.v2.rejected.coverage.total").increment();
            return block(key, "METRIC_V2_COVERAGE_INSUFFICIENT", "fullOrCopyGuardCoverageIncomplete");
        }
        if (!full.isEligibleForShadow()) {
            return block(key, first(full.getReasonCodes(), "METRIC_V2_FULL_BLOCKED"), "fullDecisionNotEligible");
        }
        if (!guard.isCopyGuardAllowed()) {
            return block(key, first(guard.getReasonCodes(), "METRIC_V2_COPY_GUARD_BLOCKED"), "copyGuardDisallowsNewEntries");
        }
        OffsetDateTime expiresAt = OffsetDateTime.ofInstant(
                min(snapshot.fullFetchedAt(), snapshot.guardFetchedAt()).plus(maxStaleness),
                ZoneOffset.UTC
        );
        return CopyStrategyGuardDecision.allow().withMetadata(
                "METRIC_V2",
                full.getGenerationId(),
                full.getComputedAt(),
                expiresAt,
                true,
                "FULL",
                fingerprint(full, guard)
        );
    }

    public CopyStrategyGuardDecision evaluateByWalletStrategy(String walletId, String strategyCode) {
        String wallet = normalize(walletId, false);
        String strategy = normalize(strategyCode, true);
        List<MetricStrategySnapshotDto> matches = current.get().fullByKey().values().stream()
                .filter(item -> wallet.equals(item.getWalletId()) && strategy.equals(item.getStrategyCode()))
                .toList();
        if (matches.size() != 1) {
            return block(wallet + "|" + strategy, matches.isEmpty()
                    ? "METRIC_V2_CACHE_MISSING"
                    : "METRIC_V2_SCOPE_REQUIRED", "matchingStrategies=" + matches.size());
        }
        MetricStrategySnapshotDto match = matches.getFirst();
        return evaluate(wallet, strategy, match.getScopeType(), match.getScopeValue());
    }

    public Snapshot snapshot() {
        return current.get();
    }

    public boolean allowCloseOnFailure() {
        return allowCloseOnFailure;
    }

    private <T> T fetch(String type, Supplier<T> operation) {
        long started = System.nanoTime();
        try {
            T value = operation.get();
            meters.counter("signals.metric_v2." + type + ".refresh.total", "result", "success").increment();
            log.info("event=metric_v2.{}.refresh.finished elapsedMs={}", type, elapsedMs(started));
            return value;
        } catch (RuntimeException ex) {
            meters.counter("signals.metric_v2." + type + ".refresh.total", "result", "failure").increment();
            throw ex;
        }
    }

    private Map<String, MetricStrategySnapshotDto> validateAndIndex(
            List<MetricStrategySnapshotDto> values,
            MetricStrategySnapshotDto.EvaluationMode expected,
            boolean requireWindows
    ) {
        if (values == null) throw new IllegalStateException("METRIC_V2_NULL_RESPONSE");
        Map<String, MetricStrategySnapshotDto> result = new LinkedHashMap<>();
        for (MetricStrategySnapshotDto value : values) {
            if (value == null) throw contractError("METRIC_V2_NULL_ITEM", List.of());
            List<String> errors = new ArrayList<>(value.contractErrors());
            if (value.getEvaluationMode() != expected) errors.add("EVALUATION_MODE_MISMATCH");
            if (requireWindows) {
                if (value.getWindows() == null || !value.getWindows().keySet().containsAll(ALL_WINDOWS)) {
                    errors.add("COPY_GUARD_WINDOWS_INCOMPLETE");
                }
            } else if (expected == MetricStrategySnapshotDto.EvaluationMode.FULL
                    && (value.getWindows() != null && !value.getWindows().isEmpty())) {
                errors.add("FULL_SIMULATION_EXPECTED_NOT_COPY_GUARD");
            }
            if (!errors.isEmpty()) throw contractError(value.getStrategyKey(), errors);
            MetricStrategySnapshotDto previous = result.putIfAbsent(value.getStrategyKey(), value);
            if (previous != null) {
                throw contractError(value.getStrategyKey(), List.of("DUPLICATE_STRATEGY_KEY"));
            }
        }
        generationsByWallet(result.values());
        return result;
    }

    private void assertGenerationCoherence(
            Map<String, MetricStrategySnapshotDto> summary,
            Map<String, MetricStrategySnapshotDto> full,
            Map<String, MetricStrategySnapshotDto> guard
    ) {
        Map<String, String> summaryGeneration = generationsByWallet(summary.values());
        if (!guard.keySet().containsAll(full.keySet())) {
            throw contractError("aggregate", List.of("COPY_GUARD_KEYS_MISSING_FOR_FULL"));
        }
        if (!full.keySet().containsAll(guard.keySet())) {
            throw contractError("aggregate", List.of("COPY_GUARD_KEYS_EXTRA_OUTSIDE_FULL"));
        }
        for (Map.Entry<String, MetricStrategySnapshotDto> entry : full.entrySet()) {
            MetricStrategySnapshotDto fullItem = entry.getValue();
            MetricStrategySnapshotDto summaryItem = summary.get(entry.getKey());
            if (summaryItem == null) {
                throw contractError(entry.getKey(), List.of("FULL_STRATEGY_MISSING_FROM_SUMMARY"));
            }
            String expected = summaryGeneration.get(fullItem.getWalletId());
            if (!Objects.equals(expected, fullItem.getGenerationId())
                    || !Objects.equals(summaryItem.getGenerationId(), fullItem.getGenerationId())) {
                throw contractError(entry.getKey(), List.of("SUMMARY_FULL_GENERATION_MISMATCH"));
            }
            MetricStrategySnapshotDto guardItem = guard.get(entry.getKey());
            if (guardItem == null) {
                throw contractError(entry.getKey(), List.of("COPY_GUARD_MISSING_FOR_FULL"));
            }
            if (!fullItem.getGenerationId().equals(guardItem.getGenerationId())) {
                throw contractError(entry.getKey(), List.of("FULL_GUARD_GENERATION_MISMATCH"));
            }
        }
    }

    private void validateRestored(Snapshot snapshot) {
        if (snapshot == null) throw new IllegalStateException("METRIC_V2_PERSISTED_SNAPSHOT_NULL");
        Map<String, MetricStrategySnapshotDto> summary = validateAndIndex(
                new ArrayList<>(snapshot.summaryByKey().values()),
                MetricStrategySnapshotDto.EvaluationMode.SUMMARY,
                false
        );
        Map<String, MetricStrategySnapshotDto> full = validateAndIndex(
                new ArrayList<>(snapshot.fullByKey().values()),
                MetricStrategySnapshotDto.EvaluationMode.FULL,
                false
        );
        Map<String, MetricStrategySnapshotDto> guard = validateAndIndex(
                new ArrayList<>(snapshot.guardByKey().values()),
                MetricStrategySnapshotDto.EvaluationMode.FULL,
                true
        );
        assertGenerationCoherence(summary, full, guard);
        if (!summary.keySet().equals(snapshot.summaryByKey().keySet())
                || !full.keySet().equals(snapshot.fullByKey().keySet())
                || !guard.keySet().equals(snapshot.guardByKey().keySet())) {
            throw new IllegalStateException("METRIC_V2_PERSISTED_SNAPSHOT_KEY_MISMATCH");
        }
    }

    private Map<String, String> generationsByWallet(Collection<MetricStrategySnapshotDto> values) {
        Map<String, String> result = new HashMap<>();
        for (MetricStrategySnapshotDto item : values) {
            String previous = result.putIfAbsent(item.getWalletId(), item.getGenerationId());
            if (previous != null && !previous.equals(item.getGenerationId())) {
                throw contractError(item.getStrategyKey(), List.of("MULTIPLE_ACTIVE_GENERATIONS_FOR_WALLET"));
            }
        }
        return result;
    }

    private IllegalStateException contractError(String key, List<String> errors) {
        meters.counter("signals.metric_v2.contract.error.total").increment();
        log.warn("event=metric_v2.contract.error strategyKey={} errors={}", safe(key), errors);
        return new IllegalStateException("METRIC_V2_CONTRACT_ERROR:" + key + ":" + String.join(",", errors));
    }

    private CopyStrategyGuardDecision block(String key, String reason, String detail) {
        meters.counter("signals.metric_v2.open.blocked.total", "reason", metricTag(reason)).increment();
        log.warn("event=metric_v2.open.blocked strategyKey={} decision=BLOCK reasonCode={} detail={} hotPathRemoteCall=false",
                safe(key), safe(reason), safe(detail));
        return CopyStrategyGuardDecision.blocked(reason, detail).withMetadata(
                "METRIC_V2",
                null,
                OffsetDateTime.now(ZoneOffset.UTC),
                null,
                false,
                "NOT_MATERIALIZED",
                null
        );
    }

    private boolean staleData(MetricStrategySnapshotDto item, Instant now) {
        return outsideTimeWindow(
                item.getDataAsOf() == null ? null : item.getDataAsOf().toInstant(),
                maxStaleness,
                now
        );
    }

    private static boolean olderThan(Instant value, Duration maxAge, Instant now) {
        return outsideTimeWindow(value, maxAge, now);
    }

    private static boolean outsideTimeWindow(Instant value, Duration maxAge, Instant now) {
        if (value == null) return true;
        Duration age = Duration.between(value, now);
        return age.compareTo(maxAge) > 0 || age.compareTo(MAX_FUTURE_SKEW.negated()) < 0;
    }

    private static Duration positive(Duration value, Duration fallback) {
        return value == null || value.isZero() || value.isNegative() ? fallback : value;
    }

    private static Instant min(Instant left, Instant right) {
        if (left == null) return right;
        if (right == null) return left;
        return left.isBefore(right) ? left : right;
    }

    private static int rank(Integer value) {
        return value == null ? Integer.MAX_VALUE : value;
    }

    private static String first(List<String> values, String fallback) {
        return values == null || values.isEmpty() || values.getFirst() == null || values.getFirst().isBlank()
                ? fallback
                : values.getFirst();
    }

    private static String normalize(String value, boolean upper) {
        String normalized = value == null ? "" : value.trim();
        return upper ? normalized.toUpperCase(Locale.ROOT) : normalized.toLowerCase(Locale.ROOT);
    }

    private static String normalizeWindows(String raw) {
        if (raw == null || raw.isBlank()) return String.join(",", ALL_WINDOWS);
        String normalized = java.util.Arrays.stream(raw.split(","))
                .map(String::trim)
                .map(value -> value.toLowerCase(Locale.ROOT))
                .filter(ALL_WINDOWS::contains)
                .distinct()
                .collect(java.util.stream.Collectors.joining(","));
        return normalized.isBlank() ? String.join(",", ALL_WINDOWS) : normalized;
    }

    private static String fingerprint(MetricStrategySnapshotDto full, MetricStrategySnapshotDto guard) {
        String raw = full.getStrategyKey() + "|" + full.getGenerationId() + "|"
                + full.getComputedAt() + "|" + guard.getComputedAt();
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(raw.getBytes(StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static String metricTag(String value) {
        String normalized = value == null ? "unknown" : value.trim().toLowerCase(Locale.ROOT);
        String clean = normalized.replaceAll("[^a-z0-9_]+", "_");
        return clean.substring(0, Math.min(40, clean.length()));
    }

    private static String safe(String value) {
        if (value == null || value.isBlank()) return "NA";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
        return clean.substring(0, Math.min(300, clean.length()));
    }

    private static long elapsedMs(long started) {
        return (System.nanoTime() - started) / 1_000_000L;
    }

    public record Snapshot(
            Map<String, MetricStrategySnapshotDto> summaryByKey,
            Map<String, MetricStrategySnapshotDto> fullByKey,
            Map<String, MetricStrategySnapshotDto> guardByKey,
            Map<String, String> walletGenerations,
            Instant summaryFetchedAt,
            Instant fullFetchedAt,
            Instant guardFetchedAt
    ) {
        public static Snapshot empty() {
            return new Snapshot(Map.of(), Map.of(), Map.of(), Map.of(), null, null, null);
        }
    }
}
