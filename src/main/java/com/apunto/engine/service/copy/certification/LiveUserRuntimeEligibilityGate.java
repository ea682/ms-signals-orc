package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.UserCopyAllocationEntity;

@FunctionalInterface
public interface LiveUserRuntimeEligibilityGate {
    Decision evaluate(UserCopyAllocationEntity allocation);

    /**
     * Revalidates the user/account prerequisites immediately before an existing
     * degraded LIVE allocation is reactivated. Implementations may deliberately
     * ignore the allocation's current EXIT_ONLY state for this check.
     */
    default Decision evaluateForActivation(UserCopyAllocationEntity allocation) {
        return evaluate(allocation);
    }

    record Decision(boolean allowed, String reasonCode) {
        public static Decision permit() {
            return new Decision(true, "LIVE_USER_RUNTIME_ELIGIBLE");
        }

        public static Decision block(String reasonCode) {
            return new Decision(false, reasonCode);
        }
    }
}
