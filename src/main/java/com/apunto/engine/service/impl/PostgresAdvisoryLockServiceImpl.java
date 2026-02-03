package com.apunto.engine.service.impl;

import com.apunto.engine.service.DistributedLockService;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.apunto.engine.shared.exception.SkipExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.function.Supplier;

@Slf4j
@Service
@RequiredArgsConstructor
public class PostgresAdvisoryLockServiceImpl implements DistributedLockService {

    private static final long DEFAULT_RETRY_DELAY_MS = 50L;

    private final DataSource dataSource;

    @Override
    public <T> T withLock(String key, Duration maxWait, Supplier<T> action) {
        if (key == null || key.isBlank()) {
            throw new SkipExecutionException("lock_key_blank");
        }
        if (action == null) {
            throw new SkipExecutionException("lock_action_null");
        }

        final long waitMs = maxWait == null ? 0L : Math.max(0L, maxWait.toMillis());
        final long deadline = System.currentTimeMillis() + waitMs;

        while (true) {
            try (Connection con = dataSource.getConnection()) {
                if (tryLock(con, key)) {
                    try {
                        return action.get();
                    } finally {
                        unlockQuietly(con, key);
                    }
                }
            } catch (SQLException e) {
                throw new EngineException(ErrorCode.EXTERNAL_SERVICE_ERROR, "DB lock error: " + e.getMessage());
            }

            if (System.currentTimeMillis() >= deadline) {
                throw new SkipExecutionException("lock_timeout");
            }

            sleep(DEFAULT_RETRY_DELAY_MS);
        }
    }

    private boolean tryLock(Connection con, String key) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("SELECT pg_try_advisory_lock(hashtext(?))")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    private void unlockQuietly(Connection con, String key) {
        try (PreparedStatement ps = con.prepareStatement("SELECT pg_advisory_unlock(hashtext(?))")) {
            ps.setString(1, key);
            ps.execute();
        } catch (Exception e) {
            log.warn("event=lock.unlock.failed key={} err={}", key, e.toString());
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SkipExecutionException("lock_interrupted");
        }
    }
}
