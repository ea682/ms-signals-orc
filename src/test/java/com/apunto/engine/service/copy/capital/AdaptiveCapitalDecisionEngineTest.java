package com.apunto.engine.service.copy.capital;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdaptiveCapitalDecisionEngineTest {

    private final AdaptiveCapitalDecisionEngine engine = new AdaptiveCapitalDecisionEngine();

    @Test
    void summaryNeverAuthorizesNewExposureOrMoney() {
        AdaptiveCapitalDecision decision = engine.evaluate(input(CopyExposureAction.OPEN)
                .decisionFinal(false)
                .eligibleForShadow(false)
                .build());

        assertFalse(decision.allowAction());
        assertFalse(decision.allowsMoney());
        assertTrue(decision.reasonCodes().contains("SUMMARY_REQUIRES_FULL_SIMULATION"));
    }

    @Test
    void closeAndReduceRemainAllowedWhenMetricsAreStaleAndPaused() {
        for (CopyExposureAction action : Set.of(CopyExposureAction.CLOSE, CopyExposureAction.REDUCE)) {
            AdaptiveCapitalDecision decision = engine.evaluate(input(action)
                    .metricsFresh(false)
                    .operationalState(StrategyOperationalState.PAUSED)
                    .copyGuardAction("PAUSE_OPEN")
                    .sourceEquityAvailable(false)
                    .build());

            assertTrue(decision.allowAction());
            assertFalse(decision.allowsMoney());
            assertEquals(BigDecimal.ZERO, decision.capitalMultiplier());
        }
    }

    @Test
    void flipAlwaysClosesFirstAndIndependentlyBlocksTheNewSide() {
        AdaptiveCapitalDecision decision = engine.evaluate(input(CopyExposureAction.FLIP)
                .sourceEquityAvailable(false)
                .build());

        assertTrue(decision.closeCurrentSide());
        assertFalse(decision.openNewSide());
        assertFalse(decision.allowAction());
        assertTrue(decision.reasonCodes().contains("SOURCE_EQUITY_REQUIRED_FOR_NEW_EXPOSURE"));
    }

    @Test
    void capacityUnknownFailsClosedForOpen() {
        AdaptiveCapitalDecision decision = engine.evaluate(input(CopyExposureAction.OPEN)
                .executionMode("LIVE")
                .liveEnabled(true)
                .capacityUsd(null)
                .build());

        assertFalse(decision.allowAction());
        assertTrue(decision.reasonCodes().contains("CAPACITY_UNKNOWN"));
    }

    @Test
    void recoveryNeedsTwoHealthyEvaluationsAndAdvancesOnlyOneStep() {
        AdaptiveCapitalDecision first = engine.evaluate(input(CopyExposureAction.OPEN)
                .operationalState(StrategyOperationalState.WATCH)
                .currentCapitalMultiplier(new BigDecimal("0.25"))
                .healthyConsecutiveEvaluations(1)
                .build());
        assertEquals(new BigDecimal("0.25"), first.capitalMultiplier());

        AdaptiveCapitalDecision second = engine.evaluate(input(CopyExposureAction.OPEN)
                .operationalState(StrategyOperationalState.WATCH)
                .currentCapitalMultiplier(new BigDecimal("0.25"))
                .healthyConsecutiveEvaluations(2)
                .build());
        assertEquals(new BigDecimal("0.50"), second.capitalMultiplier());
    }

    @Test
    void microLiveAndLiveStayDisabledByDefault() {
        AdaptiveCapitalDecision micro = engine.evaluate(input(CopyExposureAction.OPEN)
                .executionMode("MICRO_LIVE")
                .build());
        AdaptiveCapitalDecision live = engine.evaluate(input(CopyExposureAction.OPEN)
                .executionMode("LIVE")
                .build());

        assertFalse(micro.allowAction());
        assertFalse(live.allowAction());
        assertTrue(micro.reasonCodes().contains("MICRO_LIVE_DISABLED"));
        assertTrue(live.reasonCodes().contains("LIVE_DISABLED"));
    }

    @Test
    void leverageNeverMultipliesAllocatedNotional() {
        BigDecimal x5 = engine.targetNotional(new BigDecimal("100"), new BigDecimal("0.4"),
                BigDecimal.ONE, new BigDecimal("5"));
        BigDecimal x20 = engine.targetNotional(new BigDecimal("100"), new BigDecimal("0.4"),
                BigDecimal.ONE, new BigDecimal("20"));

        assertEquals(0, x5.compareTo(new BigDecimal("40")));
        assertEquals(0, x20.compareTo(x5));
    }

    private static AdaptiveCapitalInput.Builder input(CopyExposureAction action) {
        return AdaptiveCapitalInput.builder()
                .strategyKey("0xabc|LONG_ONLY|DIRECTION|LONG")
                .action(action)
                .executionMode("SHADOW")
                .decisionFinal(true)
                .eligibleForShadow(true)
                .metricsFresh(true)
                .sourceEquityAvailable(true)
                .copyGuardAction("ALLOW")
                .operationalState(StrategyOperationalState.ACTIVE)
                .currentCapitalMultiplier(BigDecimal.ONE)
                .requestedCapitalUsd(new BigDecimal("100"))
                .capacityUsd(new BigDecimal("1000"))
                .evidenceScore(new BigDecimal("80"))
                .healthyConsecutiveEvaluations(0)
                .microLiveEnabled(false)
                .liveEnabled(false);
    }
}
