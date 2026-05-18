package com.apunto.engine.service.impl;

import com.apunto.engine.service.DistributedLockService;
import com.apunto.engine.shared.exception.SkipExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

@Slf4j
@Service
@ConditionalOnProperty(name = "engine.copy.lock.provider", havingValue = "local", matchIfMissing = true)
public class InMemoryLockServiceImpl implements DistributedLockService {

    private final ConcurrentMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    @Override
    public <T> T withLock(String key, Duration maxWait, Supplier<T> action) {
        if (key == null || key.isBlank()) {
            throw new SkipExecutionException("lock_key_blank", "Lock key vacío/null", null);
        }
        if (action == null) {
            throw new SkipExecutionException("lock_action_null", "Acción del lock es null", com.apunto.engine.shared.util.LogFmt.kv("key", key));
        }

        final long waitMs = maxWait == null ? 0L : Math.max(0L, maxWait.toMillis());
        final ReentrantLock lock = locks.computeIfAbsent(key, ignored -> new ReentrantLock());
        boolean locked = false;
        try {
            locked = lock.tryLock(waitMs, TimeUnit.MILLISECONDS);
            if (!locked) {
                throw new SkipExecutionException("lock_timeout", "Timeout esperando lock local", com.apunto.engine.shared.util.LogFmt.kv("key", key, "maxWaitMs", waitMs));
            }
            return action.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new SkipExecutionException("lock_interrupted", "Interrumpido esperando lock local", com.apunto.engine.shared.util.LogFmt.kv("key", key));
        } finally {
            if (locked) {
                lock.unlock();
            }
            if (!lock.isLocked() && !lock.hasQueuedThreads()) {
                locks.remove(key, lock);
            }
        }
    }
}
