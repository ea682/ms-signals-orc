package com.apunto.engine.service.copy.ownership;

import com.apunto.engine.entity.CopyPositionOwnershipEntity;

import java.math.BigDecimal;

public record CopyPositionOwnershipDecision(
        boolean allowed,
        String reasonCode,
        CopyPositionOwnershipEntity ownership,
        BigDecimal authorizedQuantity
) {
    public CopyPositionOwnershipDecision(boolean allowed,
                                         String reasonCode,
                                         CopyPositionOwnershipEntity ownership) {
        this(allowed, reasonCode, ownership, null);
    }
}
