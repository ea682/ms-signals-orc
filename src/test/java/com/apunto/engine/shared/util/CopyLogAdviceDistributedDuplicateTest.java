package com.apunto.engine.shared.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyLogAdviceDistributedDuplicateTest {

    @Test
    void healthyDistributedDuplicateIsExpectedInformationalNoop() {
        CopyLogAdvice.Advice advice = CopyLogAdvice.advice(
                "DISTRIBUTED_DUPLICATE_SUPPRESSED",
                CopyLogAdvice.Context.empty());

        assertEquals("INFO", advice.severity());
        assertFalse(advice.shouldAlert());
        assertTrue(advice.cause().contains("Otra instancia"));
        assertTrue(advice.impact().contains("segunda orden"));
        assertTrue(advice.fields().contains("diagnosticArea=idempotency"));
    }

    @Test
    void unavailableDistributedDedupeHasActionableCriticalDiagnosis() {
        CopyLogAdvice.Advice advice = CopyLogAdvice.advice(
                "dedupe_guard_unavailable",
                CopyLogAdvice.Context.empty());

        assertEquals("CRITICAL", advice.severity());
        assertTrue(advice.shouldAlert());
        assertTrue(advice.cause().contains("storage distribuido"));
        assertTrue(advice.impact().contains("duplicada"));
        assertFalse(advice.humanMessage().contains("no clasificado"));
        assertTrue(advice.fields().contains("diagnosticArea=idempotency"));
    }

    @Test
    void expectedMicroLiveBusinessBlocksAreCataloguedWithoutFalseAlerts() {
        for (String reasonCode : java.util.List.of(
                "STRATEGY_SCOPE_NOT_MATCHED",
                "MICRO_LIVE_SYMBOL_NOT_ALLOWED",
                "MICRO_LIVE_GUARD_BLOCKED",
                "MICRO_LIVE_MIN_NOTIONAL_NOT_REACHED",
                "SUMMARY_NOT_FINAL_LIVE_BLOCKED",
                "NEGATIVE_REQUIRED_WINDOW_2W",
                "NEGATIVE_REQUIRED_WINDOW_1MO",
                "NON_POSITIVE_REQUIRED_WINDOW_2W",
                "NON_POSITIVE_REQUIRED_WINDOW_1MO",
                "DATA_STALE_BLOCKS_LIVE",
                "DATA_STALE_BLOCKS_REAL_OPEN")) {
            CopyLogAdvice.Advice advice = CopyLogAdvice.advice(reasonCode, CopyLogAdvice.Context.empty());

            assertFalse(advice.shouldAlert(), "unexpected alert for " + reasonCode);
            assertFalse(advice.humanMessage().contains("no clasificado"), "uncatalogued " + reasonCode);
            assertFalse("REVIEW".equals(advice.severity()), "review noise for " + reasonCode);
        }
    }

    @Test
    void failedSimulationAuditIsActionableAndNeverFallsBackToGenericReview() {
        CopyLogAdvice.Advice advice = CopyLogAdvice.advice(
                "SIMULATION_AUDIT_FAILED",
                CopyLogAdvice.Context.empty());

        assertTrue(advice.shouldAlert());
        assertFalse(advice.humanMessage().contains("no clasificado"));
        assertFalse("REVIEW".equals(advice.severity()));
        assertTrue(advice.fields().contains("diagnosticArea=promotion_readiness"));
    }
}
