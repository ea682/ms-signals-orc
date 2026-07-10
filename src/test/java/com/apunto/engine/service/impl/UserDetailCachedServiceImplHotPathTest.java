package com.apunto.engine.service.impl;

import com.apunto.engine.service.UserDetailService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserDetailCachedServiceImplHotPathTest {

    @Test
    void cacheOnlyMissFailsClosedWithoutLoadingDatabase() {
        AtomicInteger loads = new AtomicInteger();
        UserDetailService source = () -> {
            loads.incrementAndGet();
            return List.of();
        };
        UserDetailCachedServiceImpl cache = new UserDetailCachedServiceImpl(source);

        assertTrue(cache.getUsersCachedOnly().isEmpty());
        assertEquals(0, loads.get());
    }

    @Test
    void scheduledRefreshPublishesSnapshotOutsideHotPath() {
        AtomicInteger loads = new AtomicInteger();
        UserDetailService source = () -> {
            loads.incrementAndGet();
            return List.of();
        };
        UserDetailCachedServiceImpl cache = new UserDetailCachedServiceImpl(source);

        cache.refreshSnapshotOutOfBand();

        assertEquals(1, loads.get());
        assertTrue(cache.getUsersCachedOnly().isEmpty());
    }
}
