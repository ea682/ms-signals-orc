package com.apunto.engine.service.copy.certification;

import java.util.UUID;

public record MicroLiveRecertificationRequest(
        UUID certificationId,
        String walletId,
        String strategyCode,
        String strategyVersion,
        UUID userId,
        UUID executionAccountId,
        int priority,
        String reasonCode
) {
}
