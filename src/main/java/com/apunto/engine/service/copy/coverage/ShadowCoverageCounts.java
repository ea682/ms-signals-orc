package com.apunto.engine.service.copy.coverage;

import java.time.OffsetDateTime;

public record ShadowCoverageCounts(
        Long shadowAllocationId,
        long simulatedEvents,
        long recordedEvents,
        long skippedEvents,
        long errorEvents,
        OffsetDateTime oldestEventTime,
        OffsetDateTime newestEventTime,
        boolean queryFailed
) {
    public static ShadowCoverageCounts empty(Long shadowAllocationId) {
        return new ShadowCoverageCounts(shadowAllocationId, 0, 0, 0, 0, null, null, false);
    }

    public static ShadowCoverageCounts failed(Long shadowAllocationId) {
        return new ShadowCoverageCounts(shadowAllocationId, 0, 0, 0, 0, null, null, true);
    }
}
