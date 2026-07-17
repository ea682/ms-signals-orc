package com.apunto.engine.service.copy.certification;

public record LiveAllocationActivationResult(
        boolean activated,
        boolean idempotent,
        String reasonCode,
        Long allocationId
) {
    static LiveAllocationActivationResult activated(Long id, boolean idempotent) {
        return new LiveAllocationActivationResult(true, idempotent,
                idempotent ? "LIVE_ALLOCATION_ALREADY_ACTIVATED" : "LIVE_ALLOCATION_ACTIVATED",
                id);
    }

    static LiveAllocationActivationResult blocked(String reason, Long id) {
        return new LiveAllocationActivationResult(false, false, reason, id);
    }
}
