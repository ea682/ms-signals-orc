package com.apunto.engine.service.copy.promotion;

import com.apunto.engine.service.copy.promotion.UserCopyAllocationCopyModeResolver.CopyModeResolution;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserCopyAllocationCopyModeResolverTest {

    @Test
    void mapsCoreStrategiesToDbAllowedCopyModes() {
        assertResolved("MOVEMENT_ALL", null, "copy_all_metric_movements", "COPY_MODE_RESOLVED");
        assertResolved("SHORT_ONLY", null, "copy_only_short_events", "COPY_MODE_RESOLVED");
        assertResolved("LONG_ONLY", null, "copy_only_long_events", "COPY_MODE_RESOLVED");
    }

    @Test
    void mapsAdvancedStrategiesToDbAllowedCopyModes() {
        assertResolved("OPEN_CLOSE_ONLY", null, "copy_open_and_full_close_only", "COPY_MODE_RESOLVED");
        assertResolved("OPEN_AND_FULL_CLOSE_ONLY", null, "copy_open_and_full_close_only", "COPY_MODE_RESOLVED");
        assertResolved("PURE_OPEN_CLOSE", null, "copy_open_and_full_close_only", "COPY_MODE_RESOLVED");
        assertResolved("FIRST_OPEN_FINAL_CLOSE", null, "copy_first_open_final_close", "COPY_MODE_RESOLVED");
        assertResolved("FLIP_ONLY", null, "copy_only_flip_events", "COPY_MODE_RESOLVED");
    }

    @Test
    void mapsSupportedFilteredStrategiesToSafeFallbackMode() {
        assertResolved("SYMBOL_SPECIALIST", null, "copy_strategy_filtered_events", "COPY_MODE_MAPPING_FALLBACK");
        assertResolved("LOW_LEVERAGE_ONLY", null, "copy_strategy_filtered_events", "COPY_MODE_MAPPING_FALLBACK");
        assertResolved("TOP_SYMBOLS_ONLY", null, "copy_strategy_filtered_events", "COPY_MODE_MAPPING_FALLBACK");
    }

    @Test
    void mapsLegacySourceCopyModesWhenStrategyIsUnavailable() {
        assertResolved(null, "copy_movement_all_events", "copy_all_metric_movements", "COPY_MODE_MAPPING_FALLBACK");
        assertResolved(null, "copy_short_events", "copy_only_short_events", "COPY_MODE_MAPPING_FALLBACK");
        assertResolved(null, "copy_long_events", "copy_only_long_events", "COPY_MODE_MAPPING_FALLBACK");
    }

    @Test
    void skipIsInvalidWhenNoStrategyCanResolveIt() {
        CopyModeResolution resolution = UserCopyAllocationCopyModeResolver.resolve(null, "SKIP");

        assertFalse(resolution.valid());
        assertEquals("INVALID_COPY_MODE_MAPPING", resolution.reasonCode());
    }

    @Test
    void allowedCopyModeSetMatchesDatabaseConstraintValues() {
        for (String value : new String[]{
                "copy_all_metric_movements",
                "copy_only_short_events",
                "copy_only_long_events",
                "copy_open_and_full_close_only",
                "copy_first_open_final_close",
                "copy_strategy_filtered_events",
                "copy_only_flip_events"
        }) {
            assertTrue(UserCopyAllocationCopyModeResolver.isAllowedCopyMode(value));
        }

        assertFalse(UserCopyAllocationCopyModeResolver.isAllowedCopyMode("SKIP"));
        assertFalse(UserCopyAllocationCopyModeResolver.isAllowedCopyMode("copy_movement_all_events"));
        assertFalse(UserCopyAllocationCopyModeResolver.isAllowedCopyMode("copy_short_events"));
        assertFalse(UserCopyAllocationCopyModeResolver.isAllowedCopyMode("copy_long_events"));
    }

    private static void assertResolved(String strategyCode, String sourceCopyMode, String expectedCopyMode, String expectedReasonCode) {
        CopyModeResolution resolution = UserCopyAllocationCopyModeResolver.resolve(strategyCode, sourceCopyMode);

        assertTrue(resolution.valid());
        assertEquals(expectedCopyMode, resolution.copyMode());
        assertEquals(expectedReasonCode, resolution.reasonCode());
        assertEquals("COPY_MODE_CONSTRAINT_SAFE", resolution.constraintReasonCode());
        assertTrue(UserCopyAllocationCopyModeResolver.isAllowedCopyMode(resolution.copyMode()));
    }
}
