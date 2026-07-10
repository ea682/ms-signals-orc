package com.apunto.engine.service.copy.coverage;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record ShadowCoverageBatch(
        Map<Long, ShadowCoverageCounts> countsByAllocation,
        OffsetDateTime windowStart,
        OffsetDateTime windowEnd,
        boolean queryFailed,
        String failureClass
) {
    public ShadowCoverageBatch {
        countsByAllocation = countsByAllocation == null ? Map.of() : Map.copyOf(countsByAllocation);
    }

    public ShadowCoverageCounts countsFor(Long shadowAllocationId) {
        ShadowCoverageCounts counts = countsByAllocation.get(shadowAllocationId);
        if (counts != null) return counts;
        return queryFailed
                ? ShadowCoverageCounts.failed(shadowAllocationId)
                : ShadowCoverageCounts.empty(shadowAllocationId);
    }

    public static ShadowCoverageBatch success(
            Map<Long, ShadowCoverageCounts> counts,
            OffsetDateTime windowStart,
            OffsetDateTime windowEnd
    ) {
        return new ShadowCoverageBatch(counts, windowStart, windowEnd, false, null);
    }

    public static ShadowCoverageBatch failure(
            List<Long> allocationIds,
            OffsetDateTime windowStart,
            OffsetDateTime windowEnd,
            RuntimeException failure
    ) {
        Map<Long, ShadowCoverageCounts> failed = new LinkedHashMap<>();
        if (allocationIds != null) {
            allocationIds.forEach(id -> failed.put(id, ShadowCoverageCounts.failed(id)));
        }
        return new ShadowCoverageBatch(failed, windowStart, windowEnd, true,
                failure == null ? null : failure.getClass().getSimpleName());
    }
}
