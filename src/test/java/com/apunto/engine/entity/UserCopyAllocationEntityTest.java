package com.apunto.engine.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserCopyAllocationEntityTest {

    @Test
    void preservesMicroLiveAsIndependentExecutionMode() {
        assertEquals("MICRO_LIVE", UserCopyAllocationEntity.normalizeExecutionMode("micro-live"));
        assertEquals("MICRO_LIVE", UserCopyAllocationEntity.normalizeExecutionMode("MICRO_LIVE"));
    }

    @Test
    void microLiveIsRealExecutionNotShadow() {
        UserCopyAllocationEntity allocation = UserCopyAllocationEntity.builder()
                .executionMode("MICRO_LIVE")
                .isActive(true)
                .status(UserCopyAllocationEntity.Status.ACTIVE)
                .build();

        assertEquals("MICRO_LIVE", UserCopyAllocationEntity.normalizeExecutionMode(allocation.getExecutionMode()));
        assertTrue(!allocation.isShadowMode());
    }
}
