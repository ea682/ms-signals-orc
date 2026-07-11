package com.apunto.engine.service.copy.readiness;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Builder(toBuilder = true)
public record MicroLiveExecutionEvidence(
        Long allocationId,
        long observedDays,
        long submittedOrders,
        long acknowledgedOrders,
        long filledOrders,
        long closedOperations,
        long dispatchErrors,
        long reconciliationPending,
        long duplicateCount,
        long unresolvedAmbiguousTimeouts,
        long slippageSamples,
        BigDecimal realizedPnlUsd,
        BigDecimal maxDrawdownUsd,
        BigDecimal adverseSlippageP95Bps,
        OffsetDateTime firstSubmittedAt
) {
}
