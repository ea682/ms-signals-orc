package com.apunto.engine.service.copy.readiness;

import com.apunto.engine.service.copy.promotion.LivePromotionProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroLiveExecutionEvidencePolicyTest {

    private MicroLiveExecutionEvidencePolicy policy;

    @BeforeEach
    void setUp() {
        LivePromotionProperties properties = new LivePromotionProperties();
        properties.setMinMicroDays(7);
        properties.setMinSubmittedOrders(10);
        properties.setMinAcknowledgedOrders(10);
        properties.setMinFilledOrders(10);
        properties.setMinClosedOperations(3);
        properties.setMaxDispatchErrors(0);
        properties.setMaxReconciliationPending(0);
        properties.setMaxDuplicateCount(0);
        properties.setMaxUnresolvedAmbiguousTimeouts(0);
        properties.setMinSlippageSamples(3);
        policy = new MicroLiveExecutionEvidencePolicy(properties, new SimpleMeterRegistry());
    }

    @Test
    void calendarCompletionCannotPromoteWithZeroRealOrders() {
        MicroLiveReadinessDecision decision = policy.evaluate(MicroLiveExecutionEvidence.builder()
                .allocationId(505L)
                .observedDays(8)
                .build());

        assertFalse(decision.allowed());
        assertTrue(decision.reasons().contains("MICRO_LIVE_NOT_READY_ZERO_SUBMITTED_ORDERS"));
        assertEquals(new BigDecimal("100.00"), decision.calendarProgressPct());
        assertEquals(new BigDecimal("0.00"), decision.executionEvidencePct());
        assertEquals(new BigDecimal("0.00"), decision.finalReadinessPct());
    }

    @Test
    void unresolvedAmbiguousOutcomeBlocksOtherwiseCompleteEvidence() {
        MicroLiveReadinessDecision decision = policy.evaluate(completeEvidence()
                .unresolvedAmbiguousTimeouts(1)
                .build());

        assertFalse(decision.allowed());
        assertTrue(decision.reasons().contains("MICRO_LIVE_NOT_READY_AMBIGUOUS_RECONCILIATION"));
    }

    @Test
    void completeExecutionAndReconciliationEvidenceCanPass() {
        MicroLiveReadinessDecision decision = policy.evaluate(completeEvidence().build());

        assertTrue(decision.allowed());
        assertTrue(decision.reasons().isEmpty());
        assertEquals(new BigDecimal("100.00"), decision.finalReadinessPct());
    }

    @Test
    void errorRateLimitStillAppliesWhenAbsoluteErrorLimitAllowsSomeErrors() {
        LivePromotionProperties properties = new LivePromotionProperties();
        properties.setMinMicroDays(0);
        properties.setMinSubmittedOrders(10);
        properties.setMinAcknowledgedOrders(0);
        properties.setMinFilledOrders(0);
        properties.setMinClosedOperations(0);
        properties.setMaxDispatchErrors(5);
        properties.setMaxErrorRatePct(new BigDecimal("5"));
        properties.setMinSlippageSamples(0);
        MicroLiveExecutionEvidencePolicy ratePolicy = new MicroLiveExecutionEvidencePolicy(
                properties, new SimpleMeterRegistry());

        MicroLiveReadinessDecision decision = ratePolicy.evaluate(MicroLiveExecutionEvidence.builder()
                .submittedOrders(10)
                .dispatchErrors(1)
                .build());

        assertFalse(decision.allowed());
        assertTrue(decision.reasons().contains("MICRO_LIVE_NOT_READY_ERROR_RATE"));
    }

    private static MicroLiveExecutionEvidence.MicroLiveExecutionEvidenceBuilder completeEvidence() {
        return MicroLiveExecutionEvidence.builder()
                .allocationId(505L)
                .observedDays(8)
                .submittedOrders(12)
                .acknowledgedOrders(12)
                .filledOrders(12)
                .closedOperations(4)
                .dispatchErrors(0)
                .reconciliationPending(0)
                .duplicateCount(0)
                .unresolvedAmbiguousTimeouts(0)
                .slippageSamples(12)
                .realizedPnlUsd(new BigDecimal("4.25"))
                .maxDrawdownUsd(new BigDecimal("1.10"))
                .adverseSlippageP95Bps(new BigDecimal("3.2"));
    }
}
