package com.apunto.engine.outbox.dto;

import java.time.OffsetDateTime;

public record MetricOutboxRecord(
        long id,
        String eventType,
        String kafkaKey,
        String payload,
        OffsetDateTime createdAt,
        int attempts
) {
}
