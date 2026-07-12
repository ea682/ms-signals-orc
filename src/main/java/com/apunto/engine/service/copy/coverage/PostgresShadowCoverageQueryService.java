package com.apunto.engine.service.copy.coverage;

import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowCoverageCountsProjection;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class PostgresShadowCoverageQueryService implements ShadowCoverageQueryService {

    private final ShadowCopyOperationEventRepository repository;
    private final ShadowCoverageWindowProperties properties;
    private MeterRegistry meterRegistry = Metrics.globalRegistry;

    @Autowired(required = false)
    void setMeterRegistry(MeterRegistry meterRegistry) {
        if (meterRegistry != null) {
            this.meterRegistry = meterRegistry;
        }
    }

    @Override
    @Transactional(readOnly = true)
    public ShadowCoverageBatch load(List<Long> shadowAllocationIds, OffsetDateTime windowEnd) {
        properties.validate();
        OffsetDateTime endUtc = utc(windowEnd);
        OffsetDateTime startUtc = endUtc.minusDays(properties.getWindowDays());
        List<Long> allocationIds = distinctIds(shadowAllocationIds);
        if (allocationIds.isEmpty()) {
            return ShadowCoverageBatch.success(Map.of(), startUtc, endUtc);
        }
        if (properties.effectiveMode() == ShadowCoverageMode.LEGACY) {
            meterRegistry.counter("shadow_coverage_fallback_total", "reason", "rolling_disabled").increment();
            log.info("event=shadow.coverage.fallback.used reasonCode=SHADOW_COVERAGE_FALLBACK_USED primaryFailureReason=ROLLING_DISABLED fallbackSource=HISTORICAL rowsReturned=0 decisionImpact=LEGACY_POLICY promotionBlocked=false elapsedMs=0 traceId={}", traceId());
            return ShadowCoverageBatch.success(Map.of(), startUtc, endUtc);
        }

        long startedNs = System.nanoTime();
        log.info("event=shadow.coverage.query.started reasonCode=SHADOW_COVERAGE_QUERY_STARTED allocationCount={} windowStart={} windowEnd={} maxEvents={} projectionName=ShadowCoverageCountsProjection expectedTemporalType=Instant fallbackAllowed=false traceId={}",
                allocationIds.size(), startUtc, endUtc, properties.getMaxEvents(), traceId());
        try {
            List<ShadowCoverageCountsProjection> rows = repository.findRollingCoverageBatch(
                    allocationIds,
                    startUtc,
                    endUtc,
                    properties.getMaxEvents()
            );
            Map<Long, ShadowCoverageCounts> counts = new LinkedHashMap<>();
            if (rows != null) {
                for (ShadowCoverageCountsProjection row : rows) {
                    if (row == null || row.getShadowAllocationId() == null) continue;
                    Long allocationId = row.getShadowAllocationId();
                    counts.put(allocationId, new ShadowCoverageCounts(
                            allocationId,
                            value(row.getSimulatedEvents()),
                            value(row.getRecordedEvents()),
                            value(row.getSkippedEvents()),
                            value(row.getErrorEvents()),
                            utcNullable(row.getOldestEventTime()),
                            utcNullable(row.getNewestEventTime()),
                            false
                    ));
                }
            }
            long elapsedMs = elapsedMs(startedNs);
            meterRegistry.counter("shadow_coverage_query_total", "result", "success", "source", "rolling").increment();
            Timer.builder("shadow_coverage_query_duration")
                    .tag("result", "success")
                    .register(meterRegistry)
                    .record(Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNs)));
            log.info("event=shadow.coverage.query.succeeded reasonCode=SHADOW_COVERAGE_QUERY_OK allocationCount={} rowsReturned={} allocationsWithCoverage={} allocationsWithoutCoverage={} windowStart={} windowEnd={} source=ROLLING_QUERY fallbackUsed=false elapsedMs={} decisionImpact=ROLLING_RESULT_AVAILABLE traceId={}",
                    allocationIds.size(), rows == null ? 0 : rows.size(), counts.size(), Math.max(0, allocationIds.size() - counts.size()),
                    startUtc, endUtc, elapsedMs, traceId());
            return ShadowCoverageBatch.success(counts, startUtc, endUtc);
        } catch (RuntimeException ex) {
            long elapsedMs = elapsedMs(startedNs);
            boolean retryable = ex instanceof TransientDataAccessException;
            meterRegistry.counter("shadow_coverage_query_total", "result", "failure", "source", "rolling").increment();
            Timer.builder("shadow_coverage_query_duration")
                    .tag("result", "failure")
                    .register(meterRegistry)
                    .record(Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNs)));
            log.warn(
                    "event=shadow.coverage.query.failed reasonCode=SHADOW_COVERAGE_QUERY_FAILED projectionName=ShadowCoverageCountsProjection columnAlias=oldestEventTime,newestEventTime sourceTemporalType=Instant targetTemporalType=OffsetDateTime allocationCount={} windowStart={} windowEnd={} timezone=UTC fallbackUsed=false fallbackResult=FAIL_CLOSED decisionImpact=PROMOTION_BLOCKED retryable={} errorClass={} errorMessage=\"{}\" shouldAlert=true recommendedAction=CHECK_POSTGRES_QUERY_AND_PROJECTION elapsedMs={} traceId={}",
                    allocationIds.size(),
                    startUtc,
                    endUtc,
                    retryable,
                    ex.getClass().getSimpleName(),
                    safe(ex.getMessage()),
                    elapsedMs,
                    traceId()
            );
            return ShadowCoverageBatch.failure(allocationIds, startUtc, endUtc, ex);
        }
    }

    private static List<Long> distinctIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        LinkedHashSet<Long> values = new LinkedHashSet<>();
        for (Long id : ids) {
            if (id != null && id > 0) values.add(id);
        }
        return List.copyOf(values);
    }

    private static OffsetDateTime utc(OffsetDateTime value) {
        return (value == null ? OffsetDateTime.now(ZoneOffset.UTC) : value)
                .withOffsetSameInstant(ZoneOffset.UTC);
    }

    private static OffsetDateTime utcNullable(Instant value) {
        return value == null ? null : OffsetDateTime.ofInstant(value, ZoneOffset.UTC);
    }

    private static long value(Long value) {
        return value == null ? 0L : Math.max(0L, value);
    }

    private static String safe(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('"', '\'');
        return clean.length() <= 300 ? clean : clean.substring(0, 300);
    }

    private static long elapsedMs(long startedNs) {
        return Math.max(0L, (System.nanoTime() - startedNs) / 1_000_000L);
    }

    private static String traceId() {
        String traceId = MDC.get("traceId");
        return traceId == null || traceId.isBlank() ? "NA" : safe(traceId);
    }
}
