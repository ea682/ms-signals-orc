package com.apunto.engine.repository;

import java.math.BigDecimal;

public interface CopyBudgetSnapshotProjection {
    BigDecimal getUsedMarginUsd();

    BigDecimal getReservedPendingMarginUsd();

    Long getOpenPositions();

    Long getReservedPositions();
}
