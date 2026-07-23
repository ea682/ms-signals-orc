package com.apunto.engine.service.copy.budget;

import com.apunto.engine.service.copy.budget.CopyBudgetResolver.CopyBudgetDecision;
import com.apunto.engine.service.copy.budget.CopyBudgetResolver.CopyBudgetRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyBudgetResolverTest {

    @Test
    void microLiveUsesOneHundredAtFiveXWithProportionalMarginUsage() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(bd("1000"))
                .microLiveFixedBudgetUsd(bd("100"))
                .leverage(bd("5"))
                .sourceAccountEquityUsd(bd("500000"))
                .sourcePositionMarginUsd(bd("20000"))
                .sourcePositionNotionalUsd(bd("100000"))
                .requireSourceExposure(true)
                .build());

        assertTrue(decision.allowed());
        assertEquals(CopyBudgetResolver.MICRO_LIVE_PROPORTIONAL_PORTFOLIO, decision.budgetMode());
        assertEquals(0, bd("100").compareTo(decision.budgetUsd()));
        assertEquals(0, bd("0.04").compareTo(decision.sourceExposurePct()));
        assertEquals(0, bd("20").compareTo(decision.copyNotionalUsd()));
        assertEquals(0, bd("4").compareTo(decision.copyMarginUsd()));
    }

    @Test
    void microLiveSizesOneFiveTenAndFiftyPercentAndCapsUsageAboveOneHundredPercent() {
        for (String usage : new String[]{"1", "5", "10", "50", "150"}) {
            CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                    .executionMode("MICRO_LIVE")
                    .accountCapitalUsd(bd("1000"))
                    .sourceAccountEquityUsd(bd("100"))
                    .sourcePositionMarginUsd(bd(usage))
                    .sourcePositionNotionalUsd(bd(usage).multiply(bd("10")))
                    .leverage(bd("5"))
                    .build());

            BigDecimal expectedMargin = bd(usage).min(bd("100"));
            assertTrue(decision.allowed(), usage);
            assertEquals(0, expectedMargin.compareTo(decision.copyMarginUsd()), usage);
            assertEquals(0, expectedMargin.multiply(bd("5")).compareTo(decision.copyNotionalUsd()), usage);
        }
    }

    @Test
    void microLiveReplicatesSourceMarginUsageInsteadOfSourceNotionalExposure() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(bd("1000"))
                .sourceAccountEquityUsd(bd("2000"))
                .sourcePositionMarginUsd(bd("100"))
                .sourcePositionNotionalUsd(bd("1000"))
                .leverage(bd("5"))
                .build());

        assertTrue(decision.allowed());
        assertEquals(0, bd("0.05").compareTo(decision.sourceExposurePct()));
        assertEquals(0, bd("5").compareTo(decision.copyMarginUsd()));
        assertEquals(0, bd("25").compareTo(decision.copyNotionalUsd()));
    }

    @Test
    void executableShadowUsesTheSameOneHundredAtFiveXWithoutRealBalanceGate() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("SHADOW")
                .accountCapitalUsd(BigDecimal.ZERO)
                .allocationPct(BigDecimal.ZERO)
                .leverage(bd("20"))
                .sourceAccountEquityUsd(bd("500000"))
                .sourcePositionMarginUsd(bd("20000"))
                .sourcePositionNotionalUsd(bd("100000"))
                .requireSourceExposure(true)
                .build());

        assertTrue(decision.allowed());
        assertEquals(CopyBudgetResolver.EXECUTABLE_SHADOW_PROPORTIONAL_PORTFOLIO, decision.budgetMode());
        assertEquals("SHADOW", decision.executionMode());
        assertEquals(0, bd("100").compareTo(decision.budgetUsd()));
        assertEquals(0, bd("5").compareTo(decision.leverage()));
        assertEquals(0, bd("20").compareTo(decision.copyNotionalUsd()));
        assertEquals(0, bd("4").compareTo(decision.copyMarginUsd()));
        assertFalse(decision.usesAllocationPct());
    }

    @Test
    void microLiveDoesNotApplyLegacyTwentyDollarOrFivePositionLimits() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(bd("1000"))
                .microLiveFixedBudgetUsd(bd("100"))
                .maxMarginPerOperationUsd(bd("20"))
                .maxConcurrentPositions(5)
                .openPositionsCount(12)
                .leverage(bd("5"))
                .sourceAccountEquityUsd(bd("1000"))
                .sourcePositionMarginUsd(bd("120"))
                .sourcePositionNotionalUsd(bd("600"))
                .requireSourceExposure(true)
                .build());

        assertTrue(decision.allowed());
        assertEquals(0, bd("60").compareTo(decision.copyNotionalUsd()));
        assertEquals(0, bd("12").compareTo(decision.copyMarginUsd()));
        assertNull(decision.maxConcurrentPositions());
        assertEquals(0, BigDecimal.ZERO.compareTo(decision.maxMarginPerOperationUsd()));
    }

    @Test
    void microLiveRejectsWhenRealAccountCannotBackOneHundred() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(bd("99.99"))
                .microLiveFixedBudgetUsd(bd("100"))
                .sourceAccountEquityUsd(bd("1000"))
                .sourcePositionMarginUsd(bd("20"))
                .sourcePositionNotionalUsd(bd("100"))
                .leverage(bd("5"))
                .build());

        assertFalse(decision.allowed());
        assertEquals(CopyBudgetResolver.MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE, decision.reasonCode());
    }

    @Test
    void sourceNotionalCannotSubstituteSourceMargin() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(bd("1000"))
                .microLiveFixedBudgetUsd(bd("100"))
                .sourceAccountEquityUsd(bd("1000"))
                .sourcePositionNotionalUsd(bd("500"))
                .leverage(bd("5"))
                .requireSourceExposure(true)
                .build());

        assertFalse(decision.allowed());
        assertEquals(CopyBudgetResolver.BLOCKED_SOURCE_POSITION_MARGIN_MISSING, decision.reasonCode());
    }

    @Test
    void missingEquityBlocksMicroLiveAndLive() {
        CopyBudgetDecision micro = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(bd("1000"))
                .sourcePositionMarginUsd(bd("20"))
                .sourcePositionNotionalUsd(bd("100"))
                .leverage(bd("5"))
                .build());
        CopyBudgetDecision live = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(bd("1000"))
                .allocationPct(BigDecimal.ONE)
                .sourcePositionMarginUsd(bd("20"))
                .sourcePositionNotionalUsd(bd("100"))
                .leverage(bd("5"))
                .build());

        assertEquals(CopyBudgetResolver.BLOCKED_SOURCE_EQUITY_MISSING, micro.reasonCode());
        assertEquals(CopyBudgetResolver.BLOCKED_SOURCE_EQUITY_MISSING, live.reasonCode());
    }

    @Test
    void liveUsesAllocatedCapitalTimesSourceNotionalExposure() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(bd("1000"))
                .allocationPct(bd("0.51"))
                .sourceAccountEquityUsd(bd("100000"))
                .sourcePositionMarginUsd(bd("2000"))
                .sourcePositionNotionalUsd(bd("10000"))
                .leverage(bd("5"))
                .requireSourceExposure(true)
                .build());

        assertTrue(decision.allowed());
        assertEquals(0, bd("510").compareTo(decision.budgetUsd()));
        assertEquals(0, bd("51").compareTo(decision.copyNotionalUsd()));
        assertEquals(0, bd("10.2").compareTo(decision.copyMarginUsd()));
        assertTrue(decision.usesAllocationPct());
    }

    @Test
    void liveUsesSourceMarginRatherThanLeveragedNotional() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(bd("100"))
                .allocationPct(BigDecimal.ONE)
                .sourceAccountEquityUsd(bd("100"))
                .sourcePositionMarginUsd(bd("40"))
                .sourcePositionNotionalUsd(bd("200"))
                .leverage(bd("5"))
                .build());

        assertTrue(decision.allowed());
        assertEquals(0, bd("0.4").compareTo(decision.sourceExposurePct()));
        assertEquals(0, bd("200").compareTo(decision.copyNotionalUsd()));
        assertEquals(0, bd("40").compareTo(decision.copyMarginUsd()));
    }

    @Test
    void exhaustedMicroLiveCapitalBlocksOnlyNewExposure() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(bd("1000"))
                .microLiveFixedBudgetUsd(bd("100"))
                .openMarginUsedUsd(bd("100"))
                .sourceAccountEquityUsd(bd("1000"))
                .sourcePositionMarginUsd(bd("20"))
                .sourcePositionNotionalUsd(bd("100"))
                .leverage(bd("5"))
                .build());

        assertFalse(decision.allowed());
        assertEquals(CopyBudgetResolver.MICRO_LIVE_TOTAL_MARGIN_EXCEEDED, decision.reasonCode());
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
