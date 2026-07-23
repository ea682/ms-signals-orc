package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.StandardEnvironment;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class B2bRealMoneyExecutionGuardTest {

    private static final UUID MICRO_ACCOUNT_ID =
            UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final UUID LIVE_ACCOUNT_ID =
            UUID.fromString("20000000-0000-0000-0000-000000000002");

    @Test
    void exactTenMarginAndFiftyNotionalAtX5AreAllowedOnlyInB2bProfile() throws Exception {
        B2bRealMoneyGuardProperties properties = enabledProperties();
        B2bRealMoneyExecutionGuard guard = new B2bRealMoneyExecutionGuard(
                properties, b2bEnvironment());

        assertTrue(guard.evaluate(operation("10", "50", 5, "codex-b2b-1")).allowed());
    }

    @Test
    void marginNotionalAndLeverageCannotExceedAbsoluteAuthorization() throws Exception {
        assertEquals("B2B_MARGIN_LIMIT_EXCEEDED", guard().evaluate(
                operation("10.01", "50", 5, "codex-b2b-margin")).reasonCode());
        assertEquals("B2B_NOTIONAL_LIMIT_EXCEEDED", guard().evaluate(
                operation("10", "50.01", 5, "codex-b2b-notional")).reasonCode());
        assertEquals("B2B_MAX_LEVERAGE_EXCEEDED", guard().evaluate(
                operation("10", "50", 6, "codex-b2b-leverage")).reasonCode());
    }

    @Test
    void emergencyStopAndManualPositionCheckFailClosedWhileReduceRemainsAllowed() throws Exception {
        B2bRealMoneyGuardProperties properties = enabledProperties();
        properties.setEmergencyStop(true);
        B2bRealMoneyExecutionGuard guard = new B2bRealMoneyExecutionGuard(
                properties, b2bEnvironment());
        assertEquals("B2B_EMERGENCY_STOP_ACTIVE", guard.evaluate(
                operation("1", "5", 5, "codex-b2b-stop")).reasonCode());

        OperationDto reduce = operation(null, null, 5, "codex-b2b-close");
        reduce.setReduceOnly(true);
        assertTrue(guard.evaluate(reduce).allowed());
    }

    @Test
    void reduceOnlyCannotTargetAnotherUserOrAnUnapprovedSymbol() throws Exception {
        B2bRealMoneyExecutionGuard guard = guard();
        OperationDto otherUser = operation(null, null, 5, "codex-b2b-close-other");
        otherUser.setReduceOnly(true);
        otherUser.setUserId("user-not-authorized");
        assertEquals("B2B_TEST_USER_BLOCKED", guard.evaluate(otherUser).reasonCode());

        OperationDto otherSymbol = operation(null, null, 5, "codex-b2b-close-symbol");
        otherSymbol.setReduceOnly(true);
        otherSymbol.setSymbol("SOLUSDC");
        assertEquals("B2B_SYMBOL_BLOCKED", guard.evaluate(otherSymbol).reasonCode());
    }

    @Test
    void secondSimultaneousPositionIsBlockedAndRetryIsIdempotent() throws Exception {
        B2bRealMoneyExecutionGuard guard = guard();
        OperationDto first = operation("1", "5", 5, "codex-b2b-same");
        assertTrue(guard.evaluate(first).allowed());
        assertTrue(guard.evaluate(first).allowed());
        var second = guard.evaluate(operation("1", "5", 5, "codex-b2b-other"));
        assertFalse(second.allowed());
        assertEquals("B2B_SINGLE_POSITION_LIMIT_REACHED", second.reasonCode());
    }

    @Test
    void aClientOrderIdRetryCannotChangeTheAuthorizedPayload() throws Exception {
        B2bRealMoneyExecutionGuard guard = guard();
        assertTrue(guard.evaluate(operation("1", "5", 5, "codex-b2b-same")).allowed());

        var mutated = guard.evaluate(operation("10.01", "50.01", 5, "codex-b2b-same"));

        assertFalse(mutated.allowed());
        assertEquals("B2B_CLIENT_ORDER_ID_PAYLOAD_CONFLICT", mutated.reasonCode());
    }

    @Test
    void increaseUsesTheReservedPositionAndASecondCycleRequiresVerifiedFlat() throws Exception {
        B2bRealMoneyExecutionGuard guard = guard();
        OperationDto open = operation("1", "5", 5, "codex-b2b-cycle-1");
        assertTrue(guard.evaluate(open).allowed());

        OperationDto increase = operation("1", "5", 5, "codex-b2b-cycle-1-increase");
        increase.setCopyIntent("INCREASE");
        increase.setSourcePositionCycleId(open.getSourcePositionCycleId());
        assertTrue(guard.evaluate(increase).allowed());

        OperationDto secondCycle = operation("1", "5", 5, "codex-b2b-cycle-2");
        assertEquals("B2B_SINGLE_POSITION_LIMIT_REACHED", guard.evaluate(secondCycle).reasonCode());

        assertTrue(guard.releaseAfterVerifiedFlat(open));
        assertTrue(guard.evaluate(secondCycle).allowed());
        assertFalse(open.getSourcePositionCycleId().equals(secondCycle.getSourcePositionCycleId()));
    }

    @Test
    void rejectedPositionDoesNotConsumeTheOrderBudgetReservedForDerisk() throws Exception {
        B2bRealMoneyGuardProperties properties = enabledProperties();
        properties.setMaxOrders(3);
        B2bRealMoneyExecutionGuard guard = new B2bRealMoneyExecutionGuard(properties, b2bEnvironment());
        OperationDto open = operation("1", "5", 5, "codex-b2b-budget-open");
        assertTrue(guard.evaluate(open).allowed());
        assertEquals("B2B_SINGLE_POSITION_LIMIT_REACHED", guard.evaluate(
                operation("1", "5", 5, "codex-b2b-budget-rejected")).reasonCode());

        OperationDto increase = operation("1", "5", 5, "codex-b2b-budget-increase");
        increase.setCopyIntent("INCREASE");
        increase.setSourcePositionCycleId(open.getSourcePositionCycleId());
        assertTrue(guard.evaluate(increase).allowed());

        OperationDto close = operation(null, null, 5, "codex-b2b-budget-close");
        close.setCopyIntent("CLOSE");
        close.setSourcePositionCycleId(open.getSourcePositionCycleId());
        close.setReduceOnly(true);
        assertTrue(guard.evaluate(close).allowed());
    }

    @Test
    void livePurposeCannotUseTheMicroLiveExecutionAccount() throws Exception {
        B2bRealMoneyExecutionGuard guard = guard();
        OperationDto operation = operation("1", "5", 5, "codex-b2b-purpose-mismatch");
        operation.setAccountPurpose("LIVE");

        var decision = guard.evaluate(operation);

        assertFalse(decision.allowed());
        assertEquals("B2B_EXECUTION_ACCOUNT_PURPOSE_MISMATCH", decision.reasonCode());
    }

    @Test
    void increaseCannotPushGlobalMarginOrNotionalPastTheSharedLimit() throws Exception {
        B2bRealMoneyExecutionGuard marginGuard = guard();
        OperationDto marginOpen = operation("6", "25", 5, "codex-b2b-global-margin-open");
        assertTrue(marginGuard.evaluate(marginOpen).allowed());
        OperationDto marginIncrease = operation("5", "20", 5, "codex-b2b-global-margin-increase");
        marginIncrease.setCopyIntent("INCREASE");
        marginIncrease.setSourcePositionCycleId(marginOpen.getSourcePositionCycleId());
        assertEquals("B2B_GLOBAL_MARGIN_LIMIT_EXCEEDED",
                marginGuard.evaluate(marginIncrease).reasonCode());

        B2bRealMoneyExecutionGuard notionalGuard = guard();
        OperationDto notionalOpen = operation("4", "30", 5, "codex-b2b-global-notional-open");
        assertTrue(notionalGuard.evaluate(notionalOpen).allowed());
        OperationDto notionalIncrease = operation("4", "25", 5, "codex-b2b-global-notional-increase");
        notionalIncrease.setCopyIntent("INCREASE");
        notionalIncrease.setSourcePositionCycleId(notionalOpen.getSourcePositionCycleId());
        assertEquals("B2B_GLOBAL_NOTIONAL_LIMIT_EXCEEDED",
                notionalGuard.evaluate(notionalIncrease).reasonCode());
    }

    @Test
    void anOpenPositionInOnePurposeBlocksTheOtherPurpose() throws Exception {
        B2bRealMoneyExecutionGuard guard = guard();
        assertTrue(guard.evaluate(operation("1", "5", 5, "codex-b2b-micro-open")).allowed());
        OperationDto live = operation("1", "5", 5, "codex-b2b-live-open");
        live.setAccountPurpose("LIVE");
        live.setExchangeAccountId(LIVE_ACCOUNT_ID);

        var decision = guard.evaluate(live);

        assertFalse(decision.allowed());
        assertEquals("B2B_ANOTHER_ACCOUNT_HAS_OPEN_POSITION", decision.reasonCode());
    }

    private static B2bRealMoneyExecutionGuard guard() throws Exception {
        return new B2bRealMoneyExecutionGuard(
                enabledProperties(),
                b2bEnvironment());
    }

    private static B2bRealMoneyGuardProperties enabledProperties() throws Exception {
        B2bRealMoneyGuardProperties properties = new B2bRealMoneyGuardProperties();
        properties.setEnabled(true);
        properties.setExplicitAcknowledgement("I_ACCEPT_MAX_10_USDC_REAL_MARGIN");
        properties.setEmergencyStop(false);
        properties.setManualPositionsVerified(true);
        properties.setTestUserId("user-test");
        properties.setMicroLiveExecutionAccountId(MICRO_ACCOUNT_ID);
        properties.setLiveExecutionAccountId(LIVE_ACCOUNT_ID);
        properties.afterPropertiesSet();
        return properties;
    }

    private static OperationDto operation(String margin, String notional, int leverage, String clientId) {
        return OperationDto.builder()
                .userId("user-test")
                .symbol("BTCUSDC")
                .clientOrderId(clientId)
                .exchangeAccountId(MICRO_ACCOUNT_ID)
                .accountPurpose("MICRO_LIVE")
                .sourcePositionCycleId(UUID.nameUUIDFromBytes(clientId.getBytes(StandardCharsets.UTF_8)))
                .copyIntent("OPEN")
                .requestedMarginUsd(margin == null ? null : new BigDecimal(margin))
                .requestedNotionalUsd(notional == null ? null : new BigDecimal(notional))
                .leverage(leverage)
                .build();
    }

    private static StandardEnvironment b2bEnvironment() {
        StandardEnvironment environment = new StandardEnvironment();
        environment.setActiveProfiles("b2b");
        return environment;
    }
}
