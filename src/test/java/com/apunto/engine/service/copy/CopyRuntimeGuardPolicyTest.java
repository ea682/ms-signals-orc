package com.apunto.engine.service.copy;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyRuntimeGuardPolicyTest {

    private final CopyRuntimeGuardPolicy policy = new CopyRuntimeGuardPolicy();

    @Test
    void microLivePromotedAllocationMustIgnoreShadowOnlyGuard() {
        CopyRuntimeGuardPolicy.Decision decision = policy.decide(
                promoted("MICRO_LIVE"),
                CopyStrategyGuardDecision.blocked("METRIC_COPY_GUARD_PAUSE_OPEN", "status=SHADOW_ONLY")
        );

        assertTrue(decision.allowed());
        assertEquals("MICRO_LIVE_PROMOTED_ALLOCATION_SHOULD_NOT_BE_BLOCKED_BY_SHADOW_ONLY", decision.reasonCode());
    }

    @Test
    void livePromotedAllocationMustIgnoreShadowOnlyGuard() {
        CopyRuntimeGuardPolicy.Decision decision = policy.decide(
                promoted("LIVE"),
                CopyStrategyGuardDecision.shadowOnly("SUMMARY_NOT_FINAL_LIVE_BLOCKED", "status=SHADOW_ONLY", 0.0)
        );

        assertTrue(decision.allowed());
        assertEquals("LIVE_PROMOTED_ALLOCATION_SHOULD_NOT_BE_BLOCKED_BY_SHADOW_ONLY", decision.reasonCode());
    }

    @Test
    void directLiveWithoutPromotionStillBlockedByShadowOnly() {
        UserCopyAllocationEntity allocation = active("LIVE");

        CopyRuntimeGuardPolicy.Decision decision = policy.decide(
                allocation,
                CopyStrategyGuardDecision.shadowOnly("SUMMARY_NOT_FINAL_LIVE_BLOCKED", "status=SHADOW_ONLY", 0.0)
        );

        assertFalse(decision.allowed());
        assertEquals("SUMMARY_NOT_FINAL_LIVE_BLOCKED", decision.reasonCode());
    }

    @Test
    void realRiskStillBlocksPromotedMicroLive() {
        CopyRuntimeGuardPolicy.Decision decision = policy.decide(
                promoted("MICRO_LIVE"),
                CopyStrategyGuardDecision.disabled("USER_DISABLED", "manual stop")
        );

        assertFalse(decision.allowed());
        assertEquals("USER_DISABLED", decision.reasonCode());
    }

    private static UserCopyAllocationEntity promoted(String mode) {
        UserCopyAllocationEntity allocation = active(mode);
        allocation.setLinkedShadowAllocationId(18L);
        allocation.setPromotedFromShadowAt(OffsetDateTime.now().minusMinutes(1));
        return allocation;
    }

    private static UserCopyAllocationEntity active(String mode) {
        return UserCopyAllocationEntity.builder()
                .id(505L)
                .idUser(UUID.randomUUID())
                .walletId("0xa445a0a15b1d50fa0c4bfe6796d9447e0da5329d")
                .copyStrategyCode("MOVEMENT_ALL")
                .scopeType("strategy")
                .scopeValue("MOVEMENT_ALL")
                .executionMode(mode)
                .isActive(true)
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .build();
    }
}
