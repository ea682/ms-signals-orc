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
    void liveRetainsWeightedSizingAndDoesNotInheritMicroLimits() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("805"))
                .allocationPct(new BigDecimal("0.000001"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .sourceExposurePct(new BigDecimal("0.10"))
                .requireSourceExposure(true)
                .leverage(new BigDecimal("20"))
                .capitalAsset("USDC")
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("0.000805000000"), decision.budgetUsd());
        assertEquals(new BigDecimal("0.000080500000"), decision.copyMarginUsd());
        assertEquals(new BigDecimal("0.001610000000"), decision.copyNotionalUsd());
        assertTrue(decision.usesAllocationPct());
    }

    @Test
    void microLiveRejectsInsteadOfSilentlyReducingOneHundredUsdcTarget() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("60"))
                .allocationPct(new BigDecimal("0.000001"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .capitalAsset("USDC")
                .build());

        assertFalse(decision.allowed());
        assertEquals("MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE", decision.reasonCode());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
    }

    @Test
    void microLiveDoesNotUseAllocationPct() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.000001"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(BigDecimal.ZERO)
                .maxConcurrentPositions(5)
                .openPositionsCount(0)
                .leverage(new BigDecimal("5"))
                .capitalAsset("USDC")
                .build());

        assertTrue(decision.allowed());
        assertEquals(CopyBudgetResolver.MICRO_LIVE_FIXED_PER_OPERATION, decision.budgetMode());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals(new BigDecimal("20.000000000000"), decision.copyMarginUsd());
        assertEquals(new BigDecimal("100.000000000000"), decision.copyNotionalUsd());
        assertEquals(CopyBudgetResolver.MICRO_LIVE_FIXED_PER_OPERATION_USD, decision.reasonCode());
        assertFalse(decision.usesAllocationPct());
    }

    @Test
    void microLiveDoesNotScaleWithHighCapital() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("100000"))
                .allocationPct(new BigDecimal("0.75"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(BigDecimal.ZERO)
                .maxConcurrentPositions(5)
                .openPositionsCount(0)
                .capitalAsset("USDT")
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals(new BigDecimal("20.000000000000"), decision.copyMarginUsd());
        assertEquals(CopyBudgetResolver.MICRO_LIVE_FIXED_PER_OPERATION, decision.budgetMode());
    }

    @Test
    void microLiveStillUsesFixedBudgetWhenAllocationPctIsZero() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(BigDecimal.ZERO)
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(BigDecimal.ZERO)
                .maxConcurrentPositions(5)
                .openPositionsCount(0)
                .capitalAsset("USDC")
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals(new BigDecimal("20.000000000000"), decision.copyMarginUsd());
        assertFalse(decision.usesAllocationPct());
    }

    @Test
    void microLiveStillUsesFixedBudgetWhenAllocationPctIsNull() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(null)
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(BigDecimal.ZERO)
                .maxConcurrentPositions(5)
                .openPositionsCount(0)
                .capitalAsset("USDC")
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals(new BigDecimal("20.000000000000"), decision.copyMarginUsd());
        assertFalse(decision.usesAllocationPct());
    }

    @Test
    void microLiveLegacyRealPercentageDoesNotChangeFixedBudget() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.25"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(BigDecimal.ZERO)
                .maxConcurrentPositions(5)
                .openPositionsCount(0)
                .capitalAsset("USDC")
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals(new BigDecimal("20.000000000000"), decision.copyMarginUsd());
        assertFalse(decision.usesAllocationPct());
    }

    @Test
    void microLiveWithPartialCapitalRejectsExactTarget() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("60"))
                .allocationPct(new BigDecimal("0.25"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(BigDecimal.ZERO)
                .maxConcurrentPositions(5)
                .openPositionsCount(0)
                .capitalAsset("USDC")
                .build());

        assertFalse(decision.allowed());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals(BigDecimal.ZERO.setScale(12), decision.copyMarginUsd());
        assertEquals("MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE", decision.reasonCode());
    }

    @Test
    void microLiveWithNoCapitalIsRejectedExplicitly() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(BigDecimal.ZERO)
                .allocationPct(new BigDecimal("0.25"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(BigDecimal.ZERO)
                .maxConcurrentPositions(5)
                .openPositionsCount(0)
                .capitalAsset("USDC")
                .build());

        assertFalse(decision.allowed());
        assertEquals(new BigDecimal("100.000000000000"), decision.budgetUsd());
        assertEquals("MICRO_LIVE_INSUFFICIENT_AVAILABLE_BALANCE", decision.reasonCode());
    }

    @Test
    void liveUsesAllocationPct() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.51"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .sourceAccountEquityUsd(new BigDecimal("100000"))
                .sourcePositionMarginUsd(new BigDecimal("10000"))
                .requireSourceExposure(true)
                .capitalAsset("USDT")
                .build());

        assertTrue(decision.allowed());
        assertEquals(CopyBudgetResolver.LIVE_SOURCE_EXPOSURE_PERCENT, decision.budgetMode());
        assertEquals(new BigDecimal("510.000000000000"), decision.budgetUsd());
        assertEquals(new BigDecimal("51.000000000000"), decision.copyMarginUsd());
        assertEquals(CopyBudgetResolver.LIVE_SOURCE_EXPOSURE_PERCENT_OF_ALLOCATED_CAPITAL, decision.reasonCode());
        assertTrue(decision.usesAllocationPct());
    }

    @Test
    void liveDistributionBetweenTwoWalletsUsesWeights() {
        CopyBudgetDecision walletA = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.51"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .sourceAccountEquityUsd(new BigDecimal("100000"))
                .sourcePositionMarginUsd(new BigDecimal("10000"))
                .requireSourceExposure(true)
                .build());
        CopyBudgetDecision walletB = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(new BigDecimal("0.49"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .sourceAccountEquityUsd(new BigDecimal("100000"))
                .sourcePositionMarginUsd(new BigDecimal("10000"))
                .requireSourceExposure(true)
                .build());

        assertEquals(new BigDecimal("510.000000000000"), walletA.budgetUsd());
        assertEquals(new BigDecimal("490.000000000000"), walletB.budgetUsd());
        assertEquals(new BigDecimal("51.000000000000"), walletA.copyMarginUsd());
        assertEquals(new BigDecimal("49.000000000000"), walletB.copyMarginUsd());
    }

    @Test
    void microLiveStopsAtTotalCap() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(new BigDecimal("100"))
                .maxConcurrentPositions(5)
                .openPositionsCount(5)
                .leverage(new BigDecimal("5"))
                .build());

        assertFalse(decision.allowed());
        assertEquals("MICRO_LIVE_TOTAL_MARGIN_EXCEEDED", decision.reasonCode());
        assertEquals(BigDecimal.ZERO.setScale(12), decision.copyMarginUsd());
    }

    @Test
    void microLiveDistinguishesConcurrentPositionCapFromMarginCap() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(new BigDecimal("80"))
                .maxConcurrentPositions(5)
                .openPositionsCount(5)
                .leverage(new BigDecimal("5"))
                .build());

        assertFalse(decision.allowed());
        assertEquals("MICRO_LIVE_MAX_CONCURRENT_POSITIONS_EXCEEDED", decision.reasonCode());
    }

    @Test
    void microLiveUsesRemainingCapacityWhenBelowPerOperationCap() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("MICRO_LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .microLiveFixedBudgetUsd(new BigDecimal("100"))
                .maxMarginPerOperationUsd(new BigDecimal("20"))
                .openMarginUsedUsd(new BigDecimal("90"))
                .maxConcurrentPositions(5)
                .openPositionsCount(4)
                .leverage(new BigDecimal("5"))
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("10.000000000000"), decision.copyMarginUsd());
        assertEquals(new BigDecimal("50.000000000000"), decision.copyNotionalUsd());
    }

    @Test
    void liveUsesSourceExposureSizingNotMicroFixedTwenty() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(BigDecimal.ONE)
                .sourceAccountEquityUsd(new BigDecimal("100000"))
                .sourcePositionMarginUsd(new BigDecimal("10000"))
                .leverage(new BigDecimal("5"))
                .requireSourceExposure(true)
                .build());

        assertTrue(decision.allowed());
        assertEquals(new BigDecimal("0.100000000000"), decision.sourceExposurePct());
        assertEquals(new BigDecimal("100.000000000000"), decision.copyMarginUsd());
        assertEquals(new BigDecimal("500.000000000000"), decision.copyNotionalUsd());
        assertEquals(CopyBudgetResolver.LIVE_SOURCE_EXPOSURE_PERCENT_OF_ALLOCATED_CAPITAL, decision.reasonCode());
    }

    @Test
    void liveMissingExposureRejectsBeforeDispatch() {
        CopyBudgetDecision decision = CopyBudgetResolver.resolveBudget(CopyBudgetRequest.builder()
                .executionMode("LIVE")
                .accountCapitalUsd(new BigDecimal("1000"))
                .allocationPct(BigDecimal.ONE)
                .requireSourceExposure(true)
                .build());

        assertFalse(decision.allowed());
        assertEquals(CopyBudgetResolver.SOURCE_EXPOSURE_DATA_MISSING, decision.reasonCode());
    }
}
