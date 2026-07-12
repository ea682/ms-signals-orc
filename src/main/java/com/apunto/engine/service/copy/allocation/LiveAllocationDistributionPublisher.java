package com.apunto.engine.service.copy.allocation;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public interface LiveAllocationDistributionPublisher {

    UUID publish(UUID userId, List<LiveAllocationDistributionEntry> entries, OffsetDateTime calculatedAt);

    default LiveAllocationDistributionPublication stage(
            UUID userId,
            List<LiveAllocationDistributionEntry> entries,
            OffsetDateTime calculatedAt
    ) {
        UUID distributionId = publish(userId, entries, calculatedAt);
        return new LiveAllocationDistributionPublication(distributionId, null, calculatedAt, null);
    }

    default void complete(UUID distributionId) {
    }

    default void fail(UUID distributionId, String reasonCode) {
    }

    default UUID invalidate(UUID userId, OffsetDateTime calculatedAt, String reasonCode) {
        return null;
    }
}
