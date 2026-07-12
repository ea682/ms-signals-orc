package com.apunto.engine.service.copy.allocation;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
public class PostgresLiveAllocationDistributionService
        implements LiveAllocationDistributionPublisher, LiveAllocationPercentageResolver {

    public static final String SOURCE = "SIGNALS_CURRENT_LIVE_DISTRIBUTION";
    private static final BigDecimal ONE = BigDecimal.ONE.setScale(6, RoundingMode.HALF_UP);
    private static final BigDecimal EPSILON = new BigDecimal("0.000001");

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;
    private final Duration validity;
    private final AtomicReference<Double> lastPublishedTotal = new AtomicReference<>(0.0);

    public PostgresLiveAllocationDistributionService(
            JdbcTemplate jdbcTemplate,
            MeterRegistry meterRegistry,
            @Value("${metric-wallet.allocation.live-distribution-validity:5m}") Duration validity
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
        this.validity = validity == null || validity.isNegative() || validity.isZero()
                ? Duration.ofMinutes(5)
                : validity;
        Gauge.builder("copy_live_distribution_percentage", lastPublishedTotal, AtomicReference::get)
                .description("Last complete LIVE distribution percentage published by ms-signals-orc")
                .register(meterRegistry);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public UUID publish(UUID userId, List<LiveAllocationDistributionEntry> entries, OffsetDateTime calculatedAt) {
        PreparedDistribution prepared = insertDistribution(userId, entries, calculatedAt, "COMPLETED");
        recordPublished(prepared);
        return prepared.publication().distributionId();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public LiveAllocationDistributionPublication stage(
            UUID userId,
            List<LiveAllocationDistributionEntry> entries,
            OffsetDateTime calculatedAt
    ) {
        PreparedDistribution prepared = insertDistribution(userId, entries, calculatedAt, "STAGED");
        log.info("event=copy.live_distribution.staged distributionId={} userId={} source={} profiles={} wallets={} userTotalAllocationPct={} calculatedAt={} validUntil={} decision=STAGE reasonCode=LIVE_DISTRIBUTION_SNAPSHOT_STAGED",
                prepared.publication().distributionId(), userId, SOURCE, prepared.profileCount(),
                prepared.walletCount(), prepared.userTotal(), prepared.publication().calculatedAt(),
                prepared.publication().validUntil());
        return prepared.publication();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void complete(UUID distributionId) {
        List<CompletedDistribution> rows = jdbcTemplate.query("""
                        update futuros_operaciones.live_allocation_distribution_run
                        set status = 'COMPLETED', reason_code = null
                        where distribution_id = ? and status = 'STAGED'
                        returning id_user, user_total_allocation_pct, calculated_at, valid_until
                        """,
                (rs, rowNum) -> new CompletedDistribution(
                        rs.getObject("id_user", UUID.class),
                        rs.getBigDecimal("user_total_allocation_pct"),
                        rs.getObject("calculated_at", OffsetDateTime.class),
                        rs.getObject("valid_until", OffsetDateTime.class)),
                distributionId);
        if (rows.size() != 1) {
            throw new IllegalStateException("LIVE_DISTRIBUTION_STAGE_NOT_COMPLETABLE");
        }
        CompletedDistribution row = rows.getFirst();
        Integer profileCount = jdbcTemplate.queryForObject("""
                select count(*)
                from futuros_operaciones.live_allocation_distribution_detail
                where distribution_id = ?
                """, Integer.class, distributionId);
        Integer walletCount = jdbcTemplate.queryForObject("""
                select count(distinct wallet_lc)
                from futuros_operaciones.live_allocation_distribution_detail
                where distribution_id = ?
                """, Integer.class, distributionId);
        PreparedDistribution prepared = new PreparedDistribution(
                new LiveAllocationDistributionPublication(distributionId, SOURCE, row.calculatedAt(), row.validUntil()),
                row.userTotal(), profileCount == null ? 0 : profileCount, walletCount == null ? 0 : walletCount);
        recordPublished(prepared);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void fail(UUID distributionId, String reasonCode) {
        int updated = jdbcTemplate.update("""
                update futuros_operaciones.live_allocation_distribution_run
                set status = 'FAILED', reason_code = ?
                where distribution_id = ? and status = 'STAGED'
                """, safeReason(reasonCode), distributionId);
        if (updated > 0) {
            recordResolution("invalidated", safeTag(reasonCode));
            log.warn("event=copy.live_distribution.failed distributionId={} source={} decision=REJECT_PROMOTION reasonCode={}",
                    distributionId, SOURCE, safeReason(reasonCode));
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public UUID invalidate(UUID userId, OffsetDateTime calculatedAt, String reasonCode) {
        Objects.requireNonNull(userId, "userId");
        OffsetDateTime effectiveCalculatedAt = calculatedAt == null ? OffsetDateTime.now() : calculatedAt;
        lockUserDistribution(userId);
        UUID distributionId = UUID.randomUUID();
        jdbcTemplate.update("""
                insert into futuros_operaciones.live_allocation_distribution_run(
                    distribution_id, id_user, source, status, reason_code,
                    user_total_allocation_pct, calculated_at, valid_until, created_at
                ) values (?, ?, ?, 'FAILED', ?, 0, ?, ?, now())
                """, distributionId, userId, SOURCE, safeReason(reasonCode),
                effectiveCalculatedAt, effectiveCalculatedAt.plus(validity));
        recordResolution("invalidated", safeTag(reasonCode));
        log.warn("event=copy.live_distribution.invalidated distributionId={} userId={} source={} decision=REJECT_PROMOTION reasonCode={}",
                distributionId, userId, SOURCE, safeReason(reasonCode));
        return distributionId;
    }

    @Override
    @Transactional(readOnly = true)
    public LiveAllocationPercentageResolution resolve(LiveAllocationPercentageRequest request) {
        Objects.requireNonNull(request, "request");
        List<RunRow> runs = jdbcTemplate.query("""
                        select distribution_id, source, status, reason_code,
                               user_total_allocation_pct, calculated_at, valid_until
                        from futuros_operaciones.live_allocation_distribution_run
                        where id_user = ?
                          and calculated_at <= ?
                        order by calculated_at desc, created_at desc, distribution_id desc
                        limit 1
                        """,
                (rs, rowNum) -> new RunRow(
                        rs.getObject("distribution_id", UUID.class),
                        rs.getString("source"),
                        rs.getString("status"),
                        rs.getString("reason_code"),
                        rs.getBigDecimal("user_total_allocation_pct"),
                        rs.getObject("calculated_at", OffsetDateTime.class),
                        rs.getObject("valid_until", OffsetDateTime.class)),
                request.userId(), request.promotionTime());

        if (runs.isEmpty()) {
            return rejected(request, "LIVE_DISTRIBUTION_NOT_AVAILABLE", null);
        }
        RunRow run = runs.getFirst();
        if (!"COMPLETED".equals(run.status())) {
            return rejected(request, "LIVE_DISTRIBUTION_NOT_AVAILABLE", run);
        }
        if (run.validUntil() == null || !run.validUntil().isAfter(request.promotionTime())) {
            return rejected(request, "LIVE_ALLOCATION_PCT_STALE", run);
        }
        if (run.userTotalPercentage() != null && run.userTotalPercentage().signum() == 0) {
            return rejected(request, "LIVE_ALLOCATION_PCT_MISSING", run);
        }
        if (!validPositive(run.userTotalPercentage())) {
            return rejected(request, "LIVE_ALLOCATION_PCT_INVALID", run);
        }

        List<DetailRow> details = jdbcTemplate.query("""
                        select strategy_allocation_pct, wallet_total_allocation_pct
                        from futuros_operaciones.live_allocation_distribution_detail
                        where distribution_id = ?
                          and id_user = ?
                          and wallet_lc = ?
                          and strategy_code = ?
                          and scope_type = ?
                          and scope_value = ?
                        """,
                (rs, rowNum) -> new DetailRow(
                        rs.getBigDecimal("strategy_allocation_pct"),
                        rs.getBigDecimal("wallet_total_allocation_pct")),
                run.distributionId(), request.userId(), request.walletId(), request.strategyCode(),
                request.scopeType(), request.scopeValue());

        if (details.size() > 1) {
            return rejected(request, "LIVE_DISTRIBUTION_AMBIGUOUS", run);
        }
        if (details.isEmpty()) {
            Integer walletRows = jdbcTemplate.queryForObject("""
                    select count(*)
                    from futuros_operaciones.live_allocation_distribution_detail
                    where distribution_id = ? and id_user = ? and wallet_lc = ?
                    """, Integer.class, run.distributionId(), request.userId(), request.walletId());
            return rejected(request,
                    walletRows != null && walletRows > 0
                            ? "LIVE_ALLOCATION_PCT_SCOPE_MISMATCH"
                            : "LIVE_ALLOCATION_PCT_MISSING",
                    run);
        }

        DetailRow detail = details.getFirst();
        if (!validPositive(detail.strategyPercentage())
                || !validPositive(detail.walletTotalPercentage())
                || detail.strategyPercentage().compareTo(detail.walletTotalPercentage()) > 0) {
            return rejected(request, "LIVE_ALLOCATION_PCT_INVALID", run);
        }
        BigDecimal walletSum = jdbcTemplate.queryForObject("""
                select coalesce(sum(strategy_allocation_pct), 0)
                from futuros_operaciones.live_allocation_distribution_detail
                where distribution_id = ? and id_user = ? and wallet_lc = ?
                """, BigDecimal.class, run.distributionId(), request.userId(), request.walletId());
        if (walletSum == null
                || walletSum.setScale(6, RoundingMode.HALF_UP)
                .subtract(detail.walletTotalPercentage()).abs().compareTo(EPSILON) > 0) {
            return rejected(request, "LIVE_ALLOCATION_PCT_TOTAL_EXCEEDED", run);
        }
        BigDecimal userSum = jdbcTemplate.queryForObject("""
                select coalesce(sum(strategy_allocation_pct), 0)
                from futuros_operaciones.live_allocation_distribution_detail
                where distribution_id = ? and id_user = ?
                """, BigDecimal.class, run.distributionId(), request.userId());
        if (userSum == null
                || userSum.setScale(6, RoundingMode.HALF_UP)
                .subtract(run.userTotalPercentage()).abs().compareTo(EPSILON) > 0) {
            return rejected(request, "LIVE_ALLOCATION_PCT_TOTAL_EXCEEDED", run);
        }

        LiveAllocationPercentageResolution resolution = LiveAllocationPercentageResolution.resolved(
                detail.strategyPercentage(), detail.walletTotalPercentage(), run.source(),
                run.distributionId(), run.calculatedAt(), run.validUntil());
        recordResolution("resolved", "live_allocation_pct_resolved");
        log.info("event=copy.promotion.live.percentage.resolved executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} resolvedLiveAllocationPct={} walletTotalAllocationPct={} allocationPctSource={} distributionDecisionId={} distributionCalculatedAt={} validUntil={} usesAllocationPctForSizing=true decision=PROMOTE reasonCode=LIVE_ALLOCATION_PCT_RESOLVED",
                request.userId(), request.walletId(), request.strategyCode(), request.scopeType(), request.scopeValue(),
                resolution.percentage(), resolution.walletTotalPercentage(), resolution.source(), resolution.sourceId(),
                resolution.calculatedAt(), resolution.validUntil());
        return resolution;
    }

    private LiveAllocationPercentageResolution rejected(
            LiveAllocationPercentageRequest request,
            String reasonCode,
            RunRow run
    ) {
        recordResolution("rejected", reasonCode.toLowerCase(Locale.ROOT));
        log.info("event=copy.promotion.live.rejected executionMode=LIVE userId={} walletId={} strategyCode={} scopeType={} scopeValue={} resolvedLiveAllocationPct=null allocationPctSource={} distributionDecisionId={} distributionCalculatedAt={} decision=KEEP_MICRO_LIVE reasonCode={} retryable=true shouldAlert=false recommendedAction=WAIT_FOR_NEXT_VALID_DISTRIBUTION",
                request.userId(), request.walletId(), request.strategyCode(), request.scopeType(), request.scopeValue(),
                run == null ? null : run.source(), run == null ? null : run.distributionId(),
                run == null ? null : run.calculatedAt(), reasonCode);
        return LiveAllocationPercentageResolution.rejected(reasonCode);
    }

    private void recordResolution(String result, String reason) {
        meterRegistry.counter("copy_allocation_percentage_resolution_total",
                "execution_mode", "LIVE", "result", result, "source", "signals_allocator",
                "reason", safeTag(reason)).increment();
    }

    private PreparedDistribution insertDistribution(
            UUID userId,
            List<LiveAllocationDistributionEntry> entries,
            OffsetDateTime calculatedAt,
            String status
    ) {
        Objects.requireNonNull(userId, "userId");
        OffsetDateTime effectiveCalculatedAt = calculatedAt == null ? OffsetDateTime.now() : calculatedAt;
        OffsetDateTime validUntil = effectiveCalculatedAt.plus(validity);
        List<LiveAllocationDistributionEntry> normalized = deduplicate(entries);
        Map<String, BigDecimal> walletTotals = walletTotals(normalized);
        BigDecimal userTotal = normalized.stream()
                .map(LiveAllocationDistributionEntry::strategyPercentage)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .setScale(6, RoundingMode.HALF_UP);
        validateTotals(userTotal, walletTotals);
        lockUserDistribution(userId);

        UUID distributionId = UUID.randomUUID();
        jdbcTemplate.update("""
                insert into futuros_operaciones.live_allocation_distribution_run(
                    distribution_id, id_user, source, status, user_total_allocation_pct,
                    calculated_at, valid_until, created_at
                ) values (?, ?, ?, ?, ?, ?, ?, now())
                """, distributionId, userId, SOURCE, status, userTotal, effectiveCalculatedAt, validUntil);

        if (!normalized.isEmpty()) {
            jdbcTemplate.batchUpdate("""
                    insert into futuros_operaciones.live_allocation_distribution_detail(
                        distribution_id, id_user, wallet_lc, strategy_code, scope_type, scope_value,
                        strategy_allocation_pct, wallet_total_allocation_pct, created_at
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, now())
                    """, normalized, normalized.size(), (PreparedStatement ps, LiveAllocationDistributionEntry entry) -> {
                ps.setObject(1, distributionId);
                ps.setObject(2, userId);
                ps.setString(3, entry.walletId());
                ps.setString(4, entry.strategyCode());
                ps.setString(5, entry.scopeType());
                ps.setString(6, entry.scopeValue());
                ps.setBigDecimal(7, entry.strategyPercentage());
                ps.setBigDecimal(8, walletTotals.get(entry.walletId()));
            });
        }
        return new PreparedDistribution(
                new LiveAllocationDistributionPublication(
                        distributionId, SOURCE, effectiveCalculatedAt, validUntil),
                userTotal, normalized.size(), walletTotals.size());
    }

    private void recordPublished(PreparedDistribution prepared) {
        lastPublishedTotal.set(prepared.userTotal().doubleValue());
        meterRegistry.counter("copy_allocation_percentage_resolution_total",
                "execution_mode", "LIVE", "result", "published", "source", "signals_allocator",
                "reason", "live_distribution_snapshot_published").increment();
        log.info("event=copy.live_distribution.published distributionId={} source={} profiles={} wallets={} userTotalAllocationPct={} calculatedAt={} validUntil={} decision=PUBLISH reasonCode=LIVE_DISTRIBUTION_SNAPSHOT_PUBLISHED",
                prepared.publication().distributionId(), SOURCE, prepared.profileCount(), prepared.walletCount(),
                prepared.userTotal(), prepared.publication().calculatedAt(), prepared.publication().validUntil());
    }

    private static List<LiveAllocationDistributionEntry> deduplicate(List<LiveAllocationDistributionEntry> entries) {
        Map<String, LiveAllocationDistributionEntry> unique = new LinkedHashMap<>();
        for (LiveAllocationDistributionEntry entry : entries == null ? List.<LiveAllocationDistributionEntry>of() : entries) {
            if (entry == null) continue;
            LiveAllocationDistributionEntry previous = unique.putIfAbsent(entry.profileKey(), entry);
            if (previous != null && previous.strategyPercentage().compareTo(entry.strategyPercentage()) != 0) {
                throw new IllegalArgumentException("ambiguous percentage for profile " + entry.profileKey());
            }
        }
        return new ArrayList<>(unique.values());
    }

    private static Map<String, BigDecimal> walletTotals(List<LiveAllocationDistributionEntry> entries) {
        Map<String, BigDecimal> totals = new LinkedHashMap<>();
        for (LiveAllocationDistributionEntry entry : entries) {
            totals.merge(entry.walletId(), entry.strategyPercentage(), BigDecimal::add);
        }
        totals.replaceAll((wallet, total) -> total.setScale(6, RoundingMode.HALF_UP));
        return totals;
    }

    private static void validateTotals(BigDecimal userTotal, Map<String, BigDecimal> walletTotals) {
        if (userTotal.signum() < 0 || userTotal.compareTo(ONE) > 0) {
            throw new IllegalArgumentException("user live distribution exceeds 1.0");
        }
        walletTotals.forEach((wallet, total) -> {
            if (!validPositive(total)) {
                throw new IllegalArgumentException("wallet live distribution is invalid for " + wallet);
            }
        });
    }

    private static boolean validPositive(BigDecimal value) {
        return value != null && value.signum() > 0 && value.compareTo(ONE) <= 0;
    }

    private static String safeTag(String value) {
        if (value == null || value.isBlank()) return "unknown";
        String safe = value.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
        return safe.length() > 80 ? safe.substring(0, 80) : safe;
    }

    private void lockUserDistribution(UUID userId) {
        jdbcTemplate.query(
                "select pg_advisory_xact_lock(hashtextextended(cast(? as text), 0))",
                (java.sql.ResultSet ignored) -> null,
                "live-allocation-distribution:" + userId);
    }

    private static String safeReason(String value) {
        if (value == null || value.isBlank()) return "LIVE_DISTRIBUTION_INVALIDATED";
        String normalized = value.trim().toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9_]+", "_");
        return normalized.length() > 80 ? normalized.substring(0, 80) : normalized;
    }

    private record RunRow(
            UUID distributionId,
            String source,
            String status,
            String reasonCode,
            BigDecimal userTotalPercentage,
            OffsetDateTime calculatedAt,
            OffsetDateTime validUntil
    ) {
    }

    private record DetailRow(BigDecimal strategyPercentage, BigDecimal walletTotalPercentage) {
    }

    private record PreparedDistribution(
            LiveAllocationDistributionPublication publication,
            BigDecimal userTotal,
            int profileCount,
            int walletCount
    ) {
    }

    private record CompletedDistribution(
            UUID userId,
            BigDecimal userTotal,
            OffsetDateTime calculatedAt,
            OffsetDateTime validUntil
    ) {
    }
}
