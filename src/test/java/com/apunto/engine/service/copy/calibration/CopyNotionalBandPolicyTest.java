package com.apunto.engine.service.copy.calibration;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CopyNotionalBandPolicyTest {

    @Test
    void boundariesAreDeterministicAndUnknownIsNotZero() {
        CopyNotionalBandPolicy policy = new CopyNotionalBandPolicy(
                new BigDecimal("100"), new BigDecimal("1000"), new BigDecimal("10000"));

        assertNull(policy.band(null));
        assertNull(policy.band(BigDecimal.ZERO));
        assertEquals("SMALL", policy.band(new BigDecimal("100")));
        assertEquals("MEDIUM", policy.band(new BigDecimal("100.01")));
        assertEquals("LARGE", policy.band(new BigDecimal("10000")));
        assertEquals("XLARGE", policy.band(new BigDecimal("10000.01")));
    }

    @Test
    void rejectsOverlappingOrDescendingConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new CopyNotionalBandPolicy(
                new BigDecimal("100"), new BigDecimal("100"), new BigDecimal("10000")));
    }
}
