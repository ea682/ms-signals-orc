package com.apunto.engine.service.copy.dispatch;

import java.util.Map;

public record CopyDispatchPayloadComparison(
        Map<String, Object> existingPayload,
        Map<String, Object> incomingPayload,
        Map<String, Map<String, Object>> fieldDiff
) {
}
