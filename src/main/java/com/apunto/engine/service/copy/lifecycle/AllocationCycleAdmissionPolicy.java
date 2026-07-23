package com.apunto.engine.service.copy.lifecycle;

import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;

@Component
public class AllocationCycleAdmissionPolicy {

    public Decision evaluate(OffsetDateTime activationAt,
                             Instant sourceEventTime,
                             HyperliquidDeltaType deltaType,
                             boolean trackedCycle) {
        if (activationAt == null || sourceEventTime == null
                || !sourceEventTime.isAfter(activationAt.toInstant())) {
            return Decision.block("OPEN_NOT_TRACKED_FOR_ALLOCATION_MODE");
        }
        HyperliquidDeltaType type = deltaType == null ? HyperliquidDeltaType.UNKNOWN : deltaType;
        if (type == HyperliquidDeltaType.FLIP) {
            return Decision.allow("FLIP_NEW_CYCLE_OPEN_ALLOWED");
        }
        if (type == HyperliquidDeltaType.OPEN) {
            return Decision.allow(trackedCycle ? "DUPLICATE_OPEN_CHECK_REQUIRED" : "NEW_CYCLE_OPEN_ALLOWED");
        }
        if (type == HyperliquidDeltaType.RESIZE || type == HyperliquidDeltaType.UPDATE
                || type == HyperliquidDeltaType.CLOSE) {
            return trackedCycle
                    ? Decision.allow("TRACKED_CYCLE_EVENT_ALLOWED")
                    : Decision.block("OPEN_NOT_TRACKED_FOR_ALLOCATION_MODE");
        }
        return Decision.block("OPEN_NOT_TRACKED_FOR_ALLOCATION_MODE");
    }

    public record Decision(boolean allowed, String reasonCode) {
        static Decision allow(String reason) { return new Decision(true, reason); }
        static Decision block(String reason) { return new Decision(false, reason); }
    }
}

