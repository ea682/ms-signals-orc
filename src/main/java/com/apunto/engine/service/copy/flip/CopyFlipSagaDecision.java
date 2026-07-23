package com.apunto.engine.service.copy.flip;

import com.apunto.engine.entity.CopyFlipSagaEntity;

public record CopyFlipSagaDecision(
        boolean mayOpenNewLeg,
        String reasonCode,
        CopyFlipSagaEntity saga
) {
}
