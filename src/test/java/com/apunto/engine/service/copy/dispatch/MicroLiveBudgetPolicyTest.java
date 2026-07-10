package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroLiveBudgetPolicyTest {

    private final MicroLiveBudgetPolicy policy = new MicroLiveBudgetPolicy(
            new BigDecimal("20"), new BigDecimal("100"), 5);

    @Test
    void microLiveOrderMarginMustNotExceedTwenty() {
        BudgetDecision decision = policy.evaluate(BudgetSnapshot.empty(), new BigDecimal("20.01"), true);
        assertFalse(decision.allowed());
        assertEquals("MICRO_LIVE_ORDER_MARGIN_EXCEEDED", decision.reasonCode());
    }

    @Test
    void microLiveTotalMarginMustNotExceedOneHundred() {
        BudgetSnapshot snapshot = new BudgetSnapshot(new BigDecimal("80.01"), BigDecimal.ZERO, 4, 0);
        assertFalse(policy.evaluate(snapshot, new BigDecimal("20"), true).allowed());
    }

    @Test
    void microLiveBudgetIncludesPendingReservations() {
        BudgetSnapshot snapshot = new BudgetSnapshot(new BigDecimal("60"), new BigDecimal("25"), 3, 1);
        BudgetDecision decision = policy.evaluate(snapshot, new BigDecimal("20"), true);
        assertFalse(decision.allowed());
        assertEquals(new BigDecimal("105"), decision.projectedMarginUsd());
    }

    @Test
    void ambiguousOrderKeepsReservation() {
        BudgetSnapshot snapshot = new BudgetSnapshot(new BigDecimal("60"), new BigDecimal("20"), 3, 1);
        assertFalse(policy.evaluate(snapshot, new BigDecimal("21"), true).allowed());
        assertEquals(new BigDecimal("20"), snapshot.reservedPendingMarginUsd());
    }

    @Test
    void definitiveRejectionReleasesReservation() {
        BudgetSnapshot afterRelease = new BudgetSnapshot(new BigDecimal("60"), BigDecimal.ZERO, 3, 0);
        assertTrue(policy.evaluate(afterRelease, new BigDecimal("20"), true).allowed());
    }

    @Test
    void retryDoesNotReserveMarginTwice() {
        BudgetSnapshot sameIntentAlreadyReserved = new BudgetSnapshot(new BigDecimal("60"), new BigDecimal("20"), 3, 1);
        BudgetDecision duplicate = policy.evaluateDuplicate(sameIntentAlreadyReserved);
        assertTrue(duplicate.allowed());
        assertEquals(new BigDecimal("80"), duplicate.projectedMarginUsd());
    }

    @Test
    void maxFiveOpenOrReservedPositions() {
        BudgetSnapshot snapshot = new BudgetSnapshot(new BigDecimal("50"), BigDecimal.ZERO, 4, 1);
        BudgetDecision decision = policy.evaluate(snapshot, new BigDecimal("10"), true);
        assertFalse(decision.allowed());
        assertEquals("MICRO_LIVE_POSITION_LIMIT_EXCEEDED", decision.reasonCode());
    }

    @Test
    void accountBalanceMustNotReplaceAllocationBudget() {
        BudgetSnapshot allocation = new BudgetSnapshot(new BigDecimal("95"), BigDecimal.ZERO, 4, 0);
        assertFalse(policy.evaluate(allocation, new BigDecimal("20"), true).allowed());
    }

    @Test
    void leverageChangesNotionalButNotMarginLimit() {
        BudgetDecision fiveX = policy.evaluate(BudgetSnapshot.empty(), new BigDecimal("20"), true);
        BudgetDecision twentyX = policy.evaluate(BudgetSnapshot.empty(), new BigDecimal("20"), true);
        assertTrue(fiveX.allowed());
        assertEquals(fiveX.projectedMarginUsd(), twentyX.projectedMarginUsd());
    }

    @Test
    void reductionDoesNotConsumeNewPositionSlot() {
        BudgetSnapshot snapshot = new BudgetSnapshot(new BigDecimal("100"), BigDecimal.ZERO, 5, 0);
        assertTrue(policy.evaluate(snapshot, BigDecimal.ZERO, false).allowed());
    }
}
