package com.apunto.engine.service.copy.position;

import java.math.BigDecimal;

public record TargetPositionExitDecision(
        boolean authoritative,
        boolean alreadyFlat,
        BigDecimal closeQuantity,
        String reasonCode
) { }
