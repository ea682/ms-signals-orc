package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyIdempotencyKeyFactoryTest {

    private final CopyIdempotencyKeyFactory factory = new CopyIdempotencyKeyFactory();

    @Test
    void sameSourceEventSameAllocationProducesSameKey() {
        CopyDispatchIdentity identity = identity(505L, "MOVEMENT_ALL", "evt-1", "OPEN");

        assertEquals(factory.create(identity), factory.create(identity));
        assertTrue(factory.create(identity).matches("[a-f0-9]{64}"));
    }

    @Test
    void sameSourceEventDifferentStrategyAllocationsMayEachSendOnce() {
        String movementAll = factory.create(identity(505L, "MOVEMENT_ALL", "evt-1", "OPEN"));
        String shortOnly = factory.create(identity(506L, "SHORT_ONLY", "evt-1", "OPEN"));

        assertNotEquals(movementAll, shortOnly);
    }

    @Test
    void sameOriginDifferentImmutableDeltaDoesNotCollide() {
        String firstResize = factory.create(identity(505L, "MOVEMENT_ALL", "resize-seq-1", "INCREASE"));
        String secondResize = factory.create(identity(505L, "MOVEMENT_ALL", "resize-seq-2", "INCREASE"));

        assertNotEquals(firstResize, secondResize);
    }

    @Test
    void openAndCloseForSameSourceIdentityDoNotCollide() {
        String open = factory.create(identity(505L, "MOVEMENT_ALL", "evt-1", "OPEN"));
        String close = factory.create(identity(505L, "MOVEMENT_ALL", "evt-1", "CLOSE"));

        assertNotEquals(open, close);
    }

    @Test
    void executionModesDoNotShareDispatchIntent() {
        CopyDispatchIdentity micro = identity(505L, "MOVEMENT_ALL", "evt-1", "OPEN");
        CopyDispatchIdentity live = new CopyDispatchIdentity(
                micro.userId(), micro.userCopyAllocationId(), "LIVE", micro.strategyCode(),
                micro.scopeType(), micro.scopeValue(), micro.sourceEventId(), micro.copyIntent());

        assertNotEquals(factory.create(micro), factory.create(live));
    }

    private CopyDispatchIdentity identity(Long allocationId, String strategy, String sourceEvent, String intent) {
        return new CopyDispatchIdentity(
                "user-1",
                allocationId,
                "MICRO_LIVE",
                strategy,
                "DIRECTION",
                "SHORT",
                sourceEvent,
                intent
        );
    }
}
