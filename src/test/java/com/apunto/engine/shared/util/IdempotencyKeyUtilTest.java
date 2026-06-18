package com.apunto.engine.shared.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdempotencyKeyUtilTest {

    @Test
    void openClientOrderIdIncludesAllocationScope() {
        String shortOnly = IdempotencyKeyUtil.openClientOrderId("origin-1", "user-1", "0xabc", 10L, "SHORT_ONLY");
        String movementAll = IdempotencyKeyUtil.openClientOrderId("origin-1", "user-1", "0xabc", 11L, "MOVEMENT_ALL");
        String shortOnlyRetry = IdempotencyKeyUtil.openClientOrderId("origin-1", "user-1", "0xabc", 10L, "SHORT_ONLY");

        assertNotEquals(shortOnly, movementAll);
        assertEquals(shortOnly, shortOnlyRetry);
        assertTrue(shortOnly.length() <= 36);
        assertTrue(movementAll.length() <= 36);
    }

    @Test
    void closeClientOrderIdIncludesStrategyFallbackWhenAllocationIsMissing() {
        String shortOnly = IdempotencyKeyUtil.closeClientOrderId("origin-1", "user-1", "0xabc", null, "SHORT_ONLY");
        String movementAll = IdempotencyKeyUtil.closeClientOrderId("origin-1", "user-1", "0xabc", null, "MOVEMENT_ALL");

        assertNotEquals(shortOnly, movementAll);
        assertTrue(shortOnly.length() <= 36);
        assertTrue(movementAll.length() <= 36);
    }
}
