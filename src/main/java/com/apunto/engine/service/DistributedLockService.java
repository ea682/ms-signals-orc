package com.apunto.engine.service;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Distributed lock abstraction for multi-replica deployments.
 *
 * Implementations must guarantee mutual exclusion across replicas
 * for the duration of the action.
 */
public interface DistributedLockService {

    <T> T withLock(String key, Duration maxWait, Supplier<T> action);

    default void withLock(String key, Duration maxWait, Runnable action) {
        withLock(key, maxWait, () -> {
            action.run();
            return null;
        });
    }
}
