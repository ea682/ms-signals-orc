package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.service.copy.certification.LiveEntryAuthorizationDecision;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyRealExecutionGateTest {

    @Test
    void microLiveAndLiveSwitchesAreIndependent() {
        CopyRealExecutionGate gate = gate(true, true, false, false, true);

        assertTrue(gate.evaluate(operation(false), allocation("MICRO_LIVE")).allowed());
        CopyRealExecutionGate.Decision live = gate.evaluate(operation(false), allocation("LIVE"));
        assertFalse(live.allowed());
        assertEquals("LIVE_DISABLED", live.reasonCode());
    }

    @Test
    void masterEntrySwitchBlocksEntriesButNotDeriskOrders() {
        CopyRealExecutionGate gate = gate(false, true, true, true, false);

        assertEquals("COPY_NEW_DISPATCH_DISABLED",
                gate.evaluate(operation(false), allocation("MICRO_LIVE")).reasonCode());
        assertEquals("LIVE_DERISK_ALLOWED",
                gate.evaluate(operation(true), allocation("LIVE")).reasonCode());
    }

    @Test
    void liveReductionDoesNotRequireEntryCanaryButStillRequiresLiveMode() {
        CopyRealExecutionGate gate = gate(true, false, true, false, false);

        CopyRealExecutionGate.Decision decision = gate.evaluate(operation(true), allocation("LIVE"));

        assertTrue(decision.allowed());
        assertEquals("LIVE_DERISK_ALLOWED", decision.reasonCode());
    }

    @Test
    void liveEntryRequiresExactCertificationAndUserAdoption() {
        CopyRealExecutionGate blocked = gate(true, true, true, true, false);
        setField(blocked, "liveEntryAuthorizationGate",
                (com.apunto.engine.service.copy.certification.LiveEntryAuthorizationGate)
                        (operation, allocation) -> LiveEntryAuthorizationDecision.blocked("LIVE_ADOPTION_EXPIRED"));

        assertEquals("LIVE_ADOPTION_EXPIRED",
                blocked.evaluate(operation(false), allocation("LIVE")).reasonCode());

        CopyRealExecutionGate allowed = gate(true, true, true, true, false);
        setField(allowed, "liveEntryAuthorizationGate",
                (com.apunto.engine.service.copy.certification.LiveEntryAuthorizationGate)
                        (operation, allocation) -> LiveEntryAuthorizationDecision.allowed(
                                "LIVE_CERTIFICATION_AND_ADOPTION_VALID", null));

        assertTrue(allowed.evaluate(operation(false), allocation("LIVE")).allowed());
    }

    @Test
    void unknownModeFailsClosed() {
        CopyRealExecutionGate gate = gate(true, true, true, true, false);

        assertEquals("COPY_REAL_EXECUTION_MODE_UNKNOWN",
                gate.evaluate(operation(false), allocation("SHADOW")).reasonCode());
    }

    private CopyRealExecutionGate gate(boolean newDispatch, boolean micro, boolean live,
                                       boolean canary, boolean dryRun) {
        CopyRealExecutionGate gate = new CopyRealExecutionGate();
        setField(gate, "newDispatchEnabled", newDispatch);
        setField(gate, "microLiveEnabled", micro);
        setField(gate, "liveEnabled", live);
        setField(gate, "liveCanaryEnabled", canary);
        setField(gate, "liveDryRun", dryRun);
        setField(gate, "deriskExecutionEnabled", true);
        setField(gate, "liveWhitelistUserIds", "user-1");
        setField(gate, "liveWhitelistWalletIds", "");
        setField(gate, "liveWhitelistSymbols", "");
        setField(gate, "liveWhitelistAllocationIds", "");
        setField(gate, "liveWhitelistStrategyCodes", "");
        return gate;
    }

    private void setField(Object target, String name, Object value) {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            field.set(target, value);
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }

    private OperationDto operation(boolean reduceOnly) {
        return OperationDto.builder().userId("user-1").walletId("0xabc")
                .symbol("BTCUSDC").reduceOnly(reduceOnly).build();
    }

    private UserCopyAllocationEntity allocation(String mode) {
        return UserCopyAllocationEntity.builder().id(505L).executionMode(mode)
                .copyStrategyCode("MOVEMENT_ALL").build();
    }
}
