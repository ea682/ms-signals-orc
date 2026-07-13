package com.apunto.engine.service.copy.certification;

import java.util.UUID;

public record LiveAllocationActivationCommand(
        Long allocationId,
        UUID certificationId,
        String actor,
        String reason,
        String activationKey
) {
}
