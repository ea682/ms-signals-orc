package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicroLiveBudgetPolicyTest {

    @Test
    void hasNoFixedPerOperationMarginLimit() {
        MicroLiveBudgetPolicy policy = new MicroLiveBudgetPolicy(bd("100"), null);
        BudgetDecision decision = policy.evaluate(BudgetSnapshot.empty(), bd("60"), true);

        assertTrue(decision.allowed());
        assertEquals(0, bd("60").compareTo(decision.projectedMarginUsd()));
    }

    @Test
    void totalUsedAndReservedMarginCannotExceedOneHundred() {
        MicroLiveBudgetPolicy policy = new MicroLiveBudgetPolicy(bd("100"), null);
        BudgetSnapshot snapshot = new BudgetSnapshot(bd("70"), bd("20"), 8, 3);

        BudgetDecision decision = policy.evaluate(snapshot, bd("10.01"), true);

        assertFalse(decision.allowed());
        assertEquals("MICRO_LIVE_TOTAL_MARGIN_EXCEEDED", decision.reasonCode());
    }

    @Test
    void hasNoImplicitFivePositionLimit() {
        MicroLiveBudgetPolicy policy = new MicroLiveBudgetPolicy(bd("100"), null);
        BudgetSnapshot snapshot = new BudgetSnapshot(bd("50"), BigDecimal.ZERO, 12, 0);

        assertTrue(policy.evaluate(snapshot, bd("5"), true).allowed());
    }

    @Test
    void optionalUserPositionLimitRemainsEnforceable() {
        MicroLiveBudgetPolicy policy = new MicroLiveBudgetPolicy(bd("100"), 12);
        BudgetSnapshot snapshot = new BudgetSnapshot(bd("50"), BigDecimal.ZERO, 12, 0);

        BudgetDecision decision = policy.evaluate(snapshot, bd("5"), true);

        assertFalse(decision.allowed());
        assertEquals("SKIPPED_USER_POSITION_LIMIT", decision.reasonCode());
    }

    @Test
    void reductionAndCloseNeverRequireNewBudget() {
        MicroLiveBudgetPolicy policy = new MicroLiveBudgetPolicy(bd("100"), 1);
        BudgetSnapshot exhausted = new BudgetSnapshot(bd("100"), bd("5"), 9, 1);

        assertTrue(policy.evaluate(exhausted, BigDecimal.ZERO, false).allowed());
    }

    @Test
    void duplicateIntentReusesReservation() {
        MicroLiveBudgetPolicy policy = new MicroLiveBudgetPolicy(bd("100"), null);
        BudgetSnapshot snapshot = new BudgetSnapshot(bd("60"), bd("20"), 7, 2);

        BudgetDecision duplicate = policy.evaluateDuplicate(snapshot);

        assertTrue(duplicate.allowed());
        assertEquals(0, bd("80").compareTo(duplicate.projectedMarginUsd()));
    }

    @Test
    void runtimePolicyRequiresOneHundredAtFiveXAndNoLegacyGlobalLimits() {
        assertDoesNotThrow(() -> PostgresCopyDispatchIntentStore.requireV3MicroLivePolicy(
                bd("100"), bd("5"), null, null));
        assertThrows(IllegalStateException.class,
                () -> PostgresCopyDispatchIntentStore.requireV3MicroLivePolicy(
                        bd("99"), bd("5"), null, null));
        assertThrows(IllegalStateException.class,
                () -> PostgresCopyDispatchIntentStore.requireV3MicroLivePolicy(
                        bd("100"), bd("20"), null, null));
        assertThrows(IllegalStateException.class,
                () -> PostgresCopyDispatchIntentStore.requireV3MicroLivePolicy(
                        bd("100"), bd("5"), bd("20"), 5));
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
