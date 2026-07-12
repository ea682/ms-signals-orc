package com.apunto.engine.service.copy.allocation;

import java.time.OffsetDateTime;
import java.util.UUID;

public record LiveAllocationDistributionPublication(
        UUID distributionId,
        String source,
        OffsetDateTime calculatedAt,
        OffsetDateTime validUntil
) {
}
