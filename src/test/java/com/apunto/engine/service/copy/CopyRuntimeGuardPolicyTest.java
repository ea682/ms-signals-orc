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
        assertEquals("PROMOTED_FULL_DECISION", decision.guardSource());
    }

    @Test
    void livePromotedAllocationMustIgnoreShadowOnlyGuard() {
        CopyRuntimeGuardPolicy.Decision decision = policy.decide(
                promoted("LIVE"),
                CopyStrategyGuardDecision.shadowOnly("SUMMARY_NOT_FINAL_LIVE_BLOCKED", "status=SHADOW_ONLY", 0.0)
        );

        assertTrue(decision.allowed());
        assertEquals("LIVE_PROMOTED_ALLOCATION_SHOULD_NOT_BE_BLOCKED_BY_SHADOW_ONLY", decision.reasonCode());
        assertEquals("PROMOTED_FULL_DECISION", decision.guardSource());
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

    @Test
    void negativeRequiredWindowPauseOpenBlocksPromotedLive() {
        CopyRuntimeGuardPolicy.Decision decision = policy.decide(
                promoted("LIVE"),
                CopyStrategyGuardDecision.blocked("NEGATIVE_REQUIRED_WINDOW_2W", "window=2w pnl=-10")
        );

        assertFalse(decision.allowed());
        assertEquals("NEGATIVE_REQUIRED_WINDOW_2W", decision.reasonCode());
    }

    @Test
    void negativeRequiredWindowShadowOnlyStillBlocksPromotedLive() {
        CopyRuntimeGuardPolicy.Decision decision = policy.decide(
                promoted("LIVE"),
                CopyStrategyGuardDecision.shadowOnly(
                        "NEGATIVE_REQUIRED_WINDOW_1MO", "window=1mo pnl=-1.45", 0.0)
        );

        assertFalse(decision.allowed());
        assertEquals("NEGATIVE_REQUIRED_WINDOW_1MO", decision.reasonCode());
    }

    @Test
    void softReentryBlocksLiveOpeningsWithoutMicroLiveTarget() {
        CopyStrategyGuardDecision guard = CopyStrategyGuardDecision.shadowRevalidation(
                "NEGATIVE_1MO_SHADOW_REVALIDATION",
                "window=1mo pnl=-10"
        );

        CopyRuntimeGuardPolicy.Decision decision = policy.decide(promoted("LIVE"), guard);

        assertFalse(decision.allowed());
        assertEquals("NEGATIVE_1MO_SHADOW_REVALIDATION", decision.reasonCode());
        assertEquals("SHADOW", guard.targetExecutionMode());
    }

    @Test
    void hardReentryRequiresMicroLiveAgain() {
        CopyStrategyGuardDecision guard = CopyStrategyGuardDecision.microLiveRequiredReentry(
                "NEGATIVE_2MO_HARD_DOWNGRADE",
                "window=2mo pnl=-20"
        );

        CopyRuntimeGuardPolicy.Decision decision = policy.decide(promoted("LIVE"), guard);

        assertFalse(decision.allowed());
        assertEquals("NEGATIVE_2MO_HARD_DOWNGRADE", decision.reasonCode());
        assertEquals("MICRO_LIVE", guard.targetExecutionMode());
    }

    @Test
    void manualReviewBlocksPromotedLive() {
        CopyRuntimeGuardPolicy.Decision decision = policy.decide(
                promoted("LIVE"),
                CopyStrategyGuardDecision.manualReview("NEGATIVE_3MO_MANUAL_REVIEW", "window=3mo pnl=-30")
        );

        assertFalse(decision.allowed());
        assertEquals("NEGATIVE_3MO_MANUAL_REVIEW", decision.reasonCode());
    }

    @Test
    void missingRuntimeGuardFailsClosedForNewExposure() {
        CopyRuntimeGuardPolicy.Decision decision = policy.decide(active("LIVE"), null);

        assertFalse(decision.allowed());
        assertEquals("RUNTIME_GUARD_NOT_AVAILABLE", decision.reasonCode());
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
