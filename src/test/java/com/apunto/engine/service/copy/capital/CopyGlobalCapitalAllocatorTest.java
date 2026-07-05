package com.apunto.engine.service.copy.capital;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyGlobalCapitalAllocatorTest {

    @Test
    void blocksNewExposureWhenAvailableCapitalIsInsufficient() {
        CopyGlobalCapitalAllocation allocation = CopyGlobalCapitalAllocator.evaluate(
                new BigDecimal("100"),
                new BigDecimal("85"),
                new BigDecimal("0.10"),
                new BigDecimal("20"),
                "USDT"
        );

        assertTrue(allocation.capitalOverbooked());
        assertFalse(allocation.allowNewExposure());
        assertEquals("PRE_FLIGHT_BLOCKED_CAPITAL", allocation.reasonCode());
        assertEquals("USDT", allocation.capitalCurrency());
    }

    @Test
    void allowsNewExposureWhenCapitalIsAvailableInSameCurrency() {
        CopyGlobalCapitalAllocation allocation = CopyGlobalCapitalAllocator.evaluate(
                new BigDecimal("100"),
                new BigDecimal("20"),
                new BigDecimal("0.10"),
                new BigDecimal("20"),
                "USDC"
        );

        assertFalse(allocation.capitalOverbooked());
        assertTrue(allocation.allowNewExposure());
        assertEquals("OK", allocation.reasonCode());
        assertEquals("USDC", allocation.capitalCurrency());
        assertEquals(new BigDecimal("50.000000000000"), allocation.availableAfterRequiredAmount());
    }
}
