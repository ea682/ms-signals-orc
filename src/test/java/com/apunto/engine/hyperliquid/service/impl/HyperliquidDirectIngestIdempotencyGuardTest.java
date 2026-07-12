package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.exception.HyperliquidDirectIngestDedupeException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperliquidDirectIngestIdempotencyGuardTest {

    @Test
    void replayWithSameKeyAndPayloadIsHealthyDuplicate() {
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate(1L, 0L);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestIdempotencyGuard guard = guard(jdbc, registry, false);
        HyperliquidMappedDelta delta = delta("same-key", "BTCUSDT");

        assertTrue(guard.tryAcquire(delta, "dedupe-key"));
        assertFalse(guard.tryAcquire(delta, "dedupe-key"));
        assertEquals(1.0d, registry.find("signals.hyperliquid.direct_ingest.distributed_dedupe.total")
                .tag("result", "duplicate").counter().count());
    }

    @Test
    void sameKeyWithDifferentPayloadIsBlockedAsConflict() {
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate(1L, 0L);
        HyperliquidDirectIngestIdempotencyGuard guard = guard(jdbc, new SimpleMeterRegistry(), false);

        assertTrue(guard.tryAcquire(delta("same-key", "BTCUSDT"), "dedupe-key"));
        assertThrows(HyperliquidDirectIngestDedupeException.class,
                () -> guard.tryAcquire(delta("same-key", "ETHUSDT"), "dedupe-key"));
    }

    @Test
    void expiredOrFailedLeaseCanBeReacquiredOnlyForSamePayload() {
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate(2L);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestIdempotencyGuard guard = guard(jdbc, registry, false);

        assertTrue(guard.tryAcquire(delta("lease-key", "BTCUSDT"), "dedupe-key"));
        assertEquals(1.0d, registry.find("signals.hyperliquid.direct_ingest.distributed_dedupe.total")
                .tag("result", "reacquired").counter().count());
    }

    @Test
    void legacyClaimWithoutFingerprintIsSuppressedButNotClassifiedAsHealthyDuplicate() {
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate(0L);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestIdempotencyGuard guard = guard(jdbc, registry, false);

        assertFalse(guard.tryAcquire(delta("legacy-key", "BTCUSDT"), "dedupe-key"));
        assertFalse(jdbc.acquireSql.contains("payload_fingerprint IS NULL"));
        assertTrue(jdbc.acquireSql.contains("payload_fingerprint = EXCLUDED.payload_fingerprint"));
        assertEquals(1.0d, registry.find("distributed_duplicate_total")
                .tag("result", "payload_unverified").counter().count());
        assertEquals(0.0d, registry.find("distributed_duplicate_total")
                .tag("result", "duplicate").counter() == null
                ? 0.0d
                : registry.find("distributed_duplicate_total")
                .tag("result", "duplicate").counter().count());
    }

    @Test
    void differentKeysAreIndependentClaims() {
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate(1L, 1L);
        HyperliquidDirectIngestIdempotencyGuard guard = guard(jdbc, new SimpleMeterRegistry(), false);

        assertTrue(guard.tryAcquire(delta("key-a", "BTCUSDT"), "dedupe-a"));
        assertTrue(guard.tryAcquire(delta("key-b", "BTCUSDT"), "dedupe-b"));
    }

    @Test
    void storageFailureIsFailClosedByDefaultAndExplicitlyFailOpenWhenConfigured() {
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
        jdbc.failure = new DataAccessResourceFailureException("storage unavailable");

        assertThrows(HyperliquidDirectIngestDedupeException.class,
                () -> guard(jdbc, new SimpleMeterRegistry(), false)
                        .tryAcquire(delta("key-a", "BTCUSDT"), "dedupe-a"));
        assertTrue(guard(jdbc, new SimpleMeterRegistry(), true)
                .tryAcquire(delta("key-a", "BTCUSDT"), "dedupe-a"));
    }

    private static HyperliquidDirectIngestIdempotencyGuard guard(
            JdbcTemplate jdbc,
            SimpleMeterRegistry registry,
            boolean failOpen
    ) {
        HyperliquidDirectIngestProperties properties = new HyperliquidDirectIngestProperties();
        properties.setDistributedDedupeEnabled(true);
        properties.setFailOpenOnDedupeError(failOpen);
        properties.setDedupeLeaseTtlMs(30_000L);
        return new HyperliquidDirectIngestIdempotencyGuard(properties, jdbc, registry);
    }

    private static HyperliquidMappedDelta delta(String key, String symbol) {
        return new HyperliquidMappedDelta(
                key,
                "wallet|" + symbol + "|LONG",
                "0xabc",
                symbol,
                "LONG",
                "OPEN",
                null,
                null
        );
    }

    private static final class FakeJdbcTemplate extends JdbcTemplate {
        private final List<Long> acquireResults = new ArrayList<>();
        private int acquireIndex;
        private String storedFingerprint;
        private String acquireSql;
        private RuntimeException failure;

        private FakeJdbcTemplate(Long... results) {
            if (results != null) acquireResults.addAll(List.of(results));
        }

        @Override
        public <T> T queryForObject(String sql, Class<T> requiredType, Object... args) {
            if (failure != null) throw failure;
            acquireSql = sql;
            long result = acquireIndex < acquireResults.size() ? acquireResults.get(acquireIndex++) : 0L;
            if (result > 0L && storedFingerprint == null && args != null && args.length > 8) {
                storedFingerprint = String.valueOf(args[8]);
            }
            return requiredType.cast(result);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
            try {
                Class<?> claimType = Class.forName(
                        HyperliquidDirectIngestIdempotencyGuard.class.getName() + "$ExistingClaim");
                Constructor<?> constructor = claimType.getDeclaredConstructor(String.class, String.class, boolean.class);
                constructor.setAccessible(true);
                return List.of((T) constructor.newInstance(storedFingerprint, "PROCESSED", false));
            } catch (ReflectiveOperationException ex) {
                throw new AssertionError(ex);
            }
        }

        @Override
        public int update(String sql, Object... args) {
            return 1;
        }
    }
}
