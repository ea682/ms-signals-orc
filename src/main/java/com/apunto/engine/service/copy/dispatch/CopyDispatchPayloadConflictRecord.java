package com.apunto.engine.service.copy.dispatch;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public record CopyDispatchPayloadConflictRecord(
        UUID id,
        UUID dispatchIntentId,
        String idempotencyKey,
        String existingHash,
        String incomingHash,
        String existingStatus,
        Map<String, Object> existingPayload,
        Map<String, Object> incomingPayload,
        Map<String, Map<String, Object>> fieldDiff,
        boolean manualReviewRequired,
        OffsetDateTime occurredAt
) {
}
