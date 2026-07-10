package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDispatchStatePolicyTest {

    @Test
    void onlyCreatedIntentMayAuthorizeFirstSend() {
        assertTrue(CopyDispatchStatePolicy.mayAuthorizeSend("CREATED"));
        assertFalse(CopyDispatchStatePolicy.mayAuthorizeSend("DISPATCHING"));
        assertFalse(CopyDispatchStatePolicy.mayAuthorizeSend("ACKNOWLEDGED"));
        assertFalse(CopyDispatchStatePolicy.mayAuthorizeSend("PERSISTED"));
        assertFalse(CopyDispatchStatePolicy.mayAuthorizeSend("REJECTED"));
        assertFalse(CopyDispatchStatePolicy.mayAuthorizeSend("MANUAL_REVIEW"));
    }

    @Test
    void terminalStatesCannotReturnToSending() {
        assertFalse(CopyDispatchStatePolicy.mayTransition("PERSISTED", "DISPATCHING"));
        assertFalse(CopyDispatchStatePolicy.mayTransition("REJECTED", "DISPATCHING"));
        assertFalse(CopyDispatchStatePolicy.mayTransition("MANUAL_REVIEW", "DISPATCHING"));
        assertTrue(CopyDispatchStatePolicy.mayTransition("DISPATCHING", "RECONCILING"));
        assertTrue(CopyDispatchStatePolicy.mayTransition("RECONCILING", "FILLED"));
    }

    @Test
    void reconciliationMayDeferWithoutOpeningAPathBackToSend() {
        assertTrue(CopyDispatchStatePolicy.mayTransition("NEW", "NEW"));
        assertTrue(CopyDispatchStatePolicy.mayTransition("RECONCILING", "RECONCILING"));
        assertTrue(CopyDispatchStatePolicy.mayTransition("PERSISTENCE_PENDING", "PERSISTENCE_PENDING"));
        assertFalse(CopyDispatchStatePolicy.mayAuthorizeSend("RECONCILING"));
    }
}
