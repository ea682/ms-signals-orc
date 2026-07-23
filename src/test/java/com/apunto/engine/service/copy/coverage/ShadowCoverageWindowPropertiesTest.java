package com.apunto.engine.service.copy.coverage;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShadowCoverageWindowPropertiesTest {

    @Test
    void defaultsAreProductionContract() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();

        assertEquals(14, properties.getWindowDays());
        assertEquals(500, properties.getMaxEvents());
        assertEquals(100, properties.getMinEvaluableEvents());
        assertEquals(new BigDecimal("95"), properties.getMinimumPercent());
        assertEquals(ShadowCoverageMode.ROLLING, properties.effectiveMode());
        assertDoesNotThrow(properties::validate);
    }

    @Test
    void disabledRollingForcesLegacyRollback() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();
        properties.setMode(ShadowCoverageMode.AUDIT);
        properties.setRollingEnabled(false);

        assertEquals(ShadowCoverageMode.LEGACY, properties.effectiveMode());
    }

    @Test
    void invalidWindowDaysFailsStartupValidation() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();
        properties.setWindowDays(0);

        assertThrows(IllegalArgumentException.class, properties::validate);
    }

    @Test
    void rollingWindowCannotExceedFourteenDays() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();
        properties.setWindowDays(15);

        assertThrows(IllegalArgumentException.class, properties::validate);
    }

    @Test
    void minimumSampleCannotExceedLimit() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();
        properties.setMaxEvents(99);
        properties.setMinEvaluableEvents(100);

        assertThrows(IllegalArgumentException.class, properties::validate);
    }

    @Test
    void percentageMustStayBetweenZeroAndOneHundred() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();
        properties.setMinimumPercent(new BigDecimal("100.01"));

        assertThrows(IllegalArgumentException.class, properties::validate);
    }

    @Test
    void nullModeFailsStartupValidation() {
        ShadowCoverageWindowProperties properties = new ShadowCoverageWindowProperties();
        properties.setMode(null);

        assertThrows(IllegalArgumentException.class, properties::validate);
    }
}
