package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyExecutionAccountsDiagnostics;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyExecutionAccountsDiagnosticsTest {

    @Test
    void eligibleMicroLiveRowsAreNotExecutableWhenNewDispatchIsDisabled() {
        CopyExecutionAccountsDiagnostics diagnostics = CopyExecutionAccountsDiagnostics.fromCounts(
                false,
                true,
                false,
                true,
                false,
                true,
                1, 1, 1, 1, 1, 1,
                1, 0, 1, 0
        );

        assertFalse(diagnostics.hasExecutableAccounts());
        assertEquals(0, diagnostics.eligibleExecutionUsers());
        assertTrue(diagnostics.reasonsIfZero().contains("COPY_NEW_DISPATCH_DISABLED"));
    }

    @Test
    void noExecutableAccountsReportsConcreteReasons() {
        CopyExecutionAccountsDiagnostics diagnostics = CopyExecutionAccountsDiagnostics.fromCounts(
                false,
                false,
                false,
                true,
                true,
                false,
                1,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
        );

        assertFalse(diagnostics.hasExecutableAccounts());
        assertTrue(diagnostics.reasonsIfZero().contains("NO_ACTIVE_BINANCE_API_KEY"));
        assertTrue(diagnostics.reasonsIfZero().contains("NO_CAPITAL_CONFIG"));
        assertTrue(diagnostics.reasonsIfZero().contains("NO_MAX_WALLET_CONFIG"));
        assertTrue(diagnostics.reasonsIfZero().contains("COPY_NEW_DISPATCH_DISABLED"));
        assertTrue(diagnostics.reasonsIfZero().contains("MICRO_LIVE_DISABLED"));
        assertTrue(diagnostics.reasonsIfZero().contains("LIVE_DISABLED"));
        assertTrue(diagnostics.reasonsIfZero().contains("LIVE_DRY_RUN"));
        assertTrue(diagnostics.reasonsIfZero().contains("NO_ACTIVE_EXECUTION_ALLOCATION"));
    }

    @Test
    void liveCanaryWithoutEligibleUserReportsCanaryReason() {
        CopyExecutionAccountsDiagnostics diagnostics = CopyExecutionAccountsDiagnostics.fromCounts(
                true,
                false,
                true,
                false,
                true,
                false,
                1,
                1,
                1,
                1,
                1,
                1,
                0,
                1,
                0,
                0
        );

        assertFalse(diagnostics.hasExecutableAccounts());
        assertTrue(diagnostics.reasonsIfZero().contains("USER_NOT_IN_CANARY"));
    }

    @Test
    void executableMicroLiveAccountClearsReasons() {
        CopyExecutionAccountsDiagnostics diagnostics = CopyExecutionAccountsDiagnostics.fromCounts(
                true,
                true,
                false,
                true,
                false,
                true,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                0,
                1,
                0
        );

        assertTrue(diagnostics.hasExecutableAccounts());
        assertEquals(1, diagnostics.eligibleExecutionUsers());
        assertEquals(1, diagnostics.activeExecutableAllocations());
        assertTrue(diagnostics.reasonsIfZero().isEmpty());
    }
}
