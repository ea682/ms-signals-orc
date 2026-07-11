package com.apunto.engine.repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public interface MicroLiveExecutionEvidenceProjection {
    Long getAllocationId();
    Long getSubmittedOrders();
    Long getAcknowledgedOrders();
    Long getFilledOrders();
    Long getClosedOperations();
    Long getDispatchErrors();
    Long getReconciliationPending();
    Long getDuplicateCount();
    Long getUnresolvedAmbiguousTimeouts();
    Long getSlippageSamples();
    BigDecimal getRealizedPnlUsd();
    BigDecimal getMaxDrawdownUsd();
    BigDecimal getAdverseSlippageP95Bps();
    OffsetDateTime getFirstSubmittedAt();
}
