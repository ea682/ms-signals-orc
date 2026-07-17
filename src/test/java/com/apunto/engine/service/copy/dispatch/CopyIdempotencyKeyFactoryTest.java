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

    @Test
    void metricGenerationsDoNotShareDispatchIntent() {
        CopyDispatchIdentity firstGeneration = new CopyDispatchIdentity(
                "user-1", 505L, "MICRO_LIVE", "MOVEMENT_ALL", "ALL", "ALL",
                "generation-1", "evt-1", "OPEN");
        CopyDispatchIdentity nextGeneration = new CopyDispatchIdentity(
                "user-1", 505L, "MICRO_LIVE", "MOVEMENT_ALL", "ALL", "ALL",
                "generation-2", "evt-1", "OPEN");

        assertNotEquals(factory.create(firstGeneration), factory.create(nextGeneration));
    }

    @Test
    void sameKeyAlwaysProducesStableBinanceClientOrderId() {
        CopyDispatchIdentity identity = new CopyDispatchIdentity(
                "user-1", 505L, "MICRO_LIVE", "MOVEMENT_ALL", "ALL", "ALL", "evt-1", "OPEN");
        String key = factory.create(identity);

        String first = factory.clientOrderId(key);
        String afterRestart = new CopyIdempotencyKeyFactory().clientOrderId(key);

        assertEquals(first, afterRestart);
        assertTrue(first.matches("[A-Za-z0-9._-]{1,36}"));
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
