package com.apunto.engine.service.copy.lifecycle;

import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AllocationCycleAdmissionPolicyTest {

    private static final OffsetDateTime ACTIVATED = OffsetDateTime.parse("2026-07-17T12:00:00Z");
    private final AllocationCycleAdmissionPolicy policy = new AllocationCycleAdmissionPolicy();

    @Test
    void positionOpenedBeforeModeActivationIsNeverInherited() {
        assertFalse(policy.evaluate(ACTIVATED, Instant.parse("2026-07-17T11:59:59Z"),
                HyperliquidDeltaType.OPEN, false).allowed());
    }

    @Test
    void resizeCloseAndUpdateWithoutAdmittedOpenAreIgnored() {
        Instant after = Instant.parse("2026-07-17T12:00:01Z");
        assertFalse(policy.evaluate(ACTIVATED, after, HyperliquidDeltaType.RESIZE, false).allowed());
        assertFalse(policy.evaluate(ACTIVATED, after, HyperliquidDeltaType.UPDATE, false).allowed());
        assertFalse(policy.evaluate(ACTIVATED, after, HyperliquidDeltaType.CLOSE, false).allowed());
    }

    @Test
    void trackedCycleMayResizeAndClose() {
        Instant after = Instant.parse("2026-07-17T12:00:01Z");
        assertTrue(policy.evaluate(ACTIVATED, after, HyperliquidDeltaType.RESIZE, true).allowed());
        assertTrue(policy.evaluate(ACTIVATED, after, HyperliquidDeltaType.CLOSE, true).allowed());
    }

    @Test
    void flipWithoutTrackedOldSideStillAdmitsNewOppositeOpen() {
        assertTrue(policy.evaluate(ACTIVATED, Instant.parse("2026-07-17T12:00:01Z"),
                HyperliquidDeltaType.FLIP, false).allowed());
    }
}

