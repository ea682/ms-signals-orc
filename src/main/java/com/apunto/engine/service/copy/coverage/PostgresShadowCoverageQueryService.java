package com.apunto.engine.service.copy.coverage;

import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowCoverageCountsProjection;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

    @Override
    @Transactional(readOnly = true)
    public ShadowCoverageBatch load(List<Long> shadowAllocationIds, OffsetDateTime windowEnd) {
        properties.validate();
        OffsetDateTime endUtc = utc(windowEnd);
        OffsetDateTime startUtc = endUtc.minusDays(properties.getWindowDays());
        List<Long> allocationIds = distinctIds(shadowAllocationIds);
        if (allocationIds.isEmpty() || properties.effectiveMode() == ShadowCoverageMode.LEGACY) {
            return ShadowCoverageBatch.success(Map.of(), startUtc, endUtc);
        }

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
            return ShadowCoverageBatch.success(counts, startUtc, endUtc);
        } catch (RuntimeException ex) {
            log.warn(
                    "event=shadow.coverage.query_failed allocationCount={} windowStart={} windowEnd={} maxEvents={} reasonCode=SHADOW_COVERAGE_ROLLING_QUERY_FAILED errorClass={} errorMessage=\"{}\"",
                    allocationIds.size(),
                    startUtc,
                    endUtc,
                    properties.getMaxEvents(),
                    ex.getClass().getSimpleName(),
                    safe(ex.getMessage())
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

    private static OffsetDateTime utcNullable(OffsetDateTime value) {
        return value == null ? null : value.withOffsetSameInstant(ZoneOffset.UTC);
    }

    private static long value(Long value) {
        return value == null ? 0L : Math.max(0L, value);
    }

    private static String safe(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('"', '\'');
        return clean.length() <= 300 ? clean : clean.substring(0, 300);
    }
}
