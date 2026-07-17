package com.apunto.engine.service.copy;

import com.apunto.engine.service.metric.MetricWalletReadModeResolver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyAllocationSafetyPolicyTest {

    @Test
    void v2AlwaysRequiresAllocationAndRejectsGlobalFallbackDespiteMisconfiguration() {
        CopyAllocationSafetyPolicy policy = new CopyAllocationSafetyPolicy(
                new MetricWalletReadModeResolver("V2"),
                false,
                true
        );

        assertTrue(policy.requiresWalletAllocation());
        assertFalse(policy.allowsAllUsersFallback());
    }

    @Test
    void v1AndComparePreserveExplicitRollbackFlags() {
        CopyAllocationSafetyPolicy v1 = new CopyAllocationSafetyPolicy(
                new MetricWalletReadModeResolver("V1"),
                false,
                true
        );
        CopyAllocationSafetyPolicy compare = new CopyAllocationSafetyPolicy(
                new MetricWalletReadModeResolver("COMPARE"),
                true,
                false
        );

        assertFalse(v1.requiresWalletAllocation());
        assertTrue(v1.allowsAllUsersFallback());
        assertTrue(compare.requiresWalletAllocation());
        assertFalse(compare.allowsAllUsersFallback());
    }
}
