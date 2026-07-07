package com.apunto.engine.service.copy.budget;

import com.apunto.engine.service.copy.budget.CopyBudgetResolver.CopyBudgetDecision;
import com.apunto.engine.service.copy.budget.CopyBudgetResolver.CopyBudgetRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyBudgetResolverTest {

    @Test
    void microLiveDoesNotUseAllocationPct() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.000001"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .capitalAsset("USDC")
                .build());

        assertTrue(decision.allowed());
        assertEquals(CopyBudgetResolver.FIXED_USD, decision.budgetMode());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals(CopyBudgetResolver.MICRO_LIVE_FIXED_BUDGET_USD, decision.reasonCode());
        assertFalse(decision.usesAllocationPct());
    }

    @Test
    void microLiveDoesNotScaleWithHighCapital() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("100000"))
                .allocationPct(new BigDecimal("0.75"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .capitalAsset("USDT")
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals(CopyBudgetResolver.FIXED_USD, decision.budgetMode());
    }

    @Test
    void microLiveStillUsesFixedBudgetWhenAllocationPctIsZero() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(BigDecimal.ZERO)
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .capitalAsset("USDC")
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertFalse(decision.usesAllocationPct());
    }

    @Test
    void microLiveWithPartialCapitalUsesAvailableAsHardLimit() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("60"))
                .allocationPct(new BigDecimal("0.25"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .capitalAsset("USDC")
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("60.000000000000"), decision.budgetUsd());
        assertEquals(CopyBudgetResolver.MICRO_LIVE_FIXED_BUDGET_USD, decision.reasonCode());
    }

    @Test
    void microLiveWithNoCapitalIsRejectedExplicitly() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(BigDecimal.ZERO)
                .allocationPct(new BigDecimal("0.25"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .capitalAsset("USDC")
                .build());

        assertFalse(decision.allowed());
        assertEquals(BigDecimal.ZERO.setScale(12), decision.budgetUsd());
        assertEquals(CopyBudgetResolver.INSUFFICIENT_BALANCE_FOR_MICRO_LIVE, decision.reasonCode());
    }

    @Test
    void liveUsesAllocationPct() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.51"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .capitalAsset("USDT")
                .build());

        assertTrue(decision.allowed());
        assertEquals(CopyBudgetResolver.WEIGHTED_PERCENTAGE, decision.budgetMode());
        assertEquals(new BigDecimal("510.000000000000"), decision.budgetUsd());
        assertEquals(CopyBudgetResolver.LIVE_WEIGHTED_ALLOCATION_PCT, decision.reasonCode());
        assertTrue(decision.usesAllocationPct());
    }

    @Test
    void liveDistributionBetweenTwoWalletsUsesWeights() {
        CopyBudgetDecision walletA = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.51"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .build());
        CopyBudgetDecision walletB = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.49"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .build());

        assertEquals(new BigDecimal("510.000000000000"), walletA.budgetUsd());
        assertEquals(new BigDecimal("490.000000000000"), walletB.budgetUsd());
    }
}
