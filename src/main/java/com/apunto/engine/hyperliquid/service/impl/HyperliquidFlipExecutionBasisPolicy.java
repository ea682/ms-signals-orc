package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;

import java.math.BigDecimal;

/**
 * Keeps FLIP execution fail-closed until an authoritative fill contract is available.
 * POSITION_DELTA is useful for state/audit, but it cannot prove the two economic legs.
 */
final class HyperliquidFlipExecutionBasisPolicy {

    static final String MISSING_REASON_CODE = "FLIP_EXECUTION_BASIS_MISSING";

    Decision evaluate(HyperliquidMappedDelta mapped) {
        if (HyperliquidDeltaType.from(mapped == null ? null : mapped.deltaType())
                != HyperliquidDeltaType.FLIP) {
            return Decision.notFlip();
        }
        HyperliquidDeltaRequest request = mapped.request();
        if (request == null) {
            return Decision.block("request_missing");
        }
        if (!"USER_FILL".equalsIgnoreCase(clean(request.economicEventKind()))) {
            return Decision.block("authoritative_user_fill_missing");
        }
        if (!Boolean.FALSE.equals(request.sourceEstimated())) {
            return Decision.block("source_estimated");
        }
        if (blank(request.sourceEventId())) {
            return Decision.block("source_tid_missing");
        }
        if (request.sourceSequence() == null || request.sourceSequence() <= 0L) {
            return Decision.block("source_sequence_missing");
        }
        if (!positive(request.effectiveCloseQty())) {
            return Decision.block("close_quantity_missing");
        }
        if (!positive(request.effectiveExitPrice())) {
            return Decision.block("close_price_missing");
        }
        if (!positive(request.sizeQty())) {
            return Decision.block("new_leg_quantity_missing");
        }
        if (request.effectiveRealizedPnlUsd() == null) {
            return Decision.block("closed_pnl_missing");
        }
        return Decision.allow();
    }

    private boolean positive(BigDecimal value) {
        return value != null && value.compareTo(BigDecimal.ZERO) > 0;
    }

    private boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private String clean(String value) {
        return value == null ? "" : value.trim();
    }

    record Decision(boolean flip, boolean allowed, String reason) {
        private static Decision notFlip() {
            return new Decision(false, true, "not_flip");
        }

        private static Decision block(String reason) {
            return new Decision(true, false, reason);
        }

        private static Decision allow() {
            return new Decision(true, true, "authoritative_user_fill");
        }
    }
}
