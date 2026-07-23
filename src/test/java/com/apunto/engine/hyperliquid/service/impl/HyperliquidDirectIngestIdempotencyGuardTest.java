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
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    void sameAuthoritativeTidWithDifferentPayloadIsConflictNotReplicaDuplicate() throws Exception {
        var objectMapper = new ObjectMapper().findAndRegisterModules();
        var fixture = objectMapper.readTree(getClass().getResourceAsStream(
                "/fixtures/production/anomaly-e-zero-hash-identity.json"));
        var firstNode = fixture.path("first").deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) firstNode)
                .put("economicEventKind", "USER_FILL")
                .put("sourceEstimated", false);
        var firstRequest = objectMapper.treeToValue(
                firstNode,
                com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest.class);
        var conflictingNode = fixture.path("first").deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) conflictingNode)
                .put("sizeQty", 360082.0)
                .put("notionalUsd", 148400.594660)
                .put("economicEventKind", "USER_FILL")
                .put("sourceEstimated", false);
        var conflictingRequest = objectMapper.treeToValue(
                conflictingNode,
                com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest.class);
        HyperliquidMappedDelta first = authoritative(firstRequest);
        HyperliquidMappedDelta conflicting = authoritative(conflictingRequest);
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate(1L, 0L);
        HyperliquidDirectIngestIdempotencyGuard guard =
                guard(jdbc, new SimpleMeterRegistry(), false);

        assertTrue(guard.tryAcquire(first, "same-authoritative-tid"));
        assertThrows(
                HyperliquidDirectIngestDedupeException.class,
                () -> guard.tryAcquire(conflicting, "same-authoritative-tid"));
    }

    @Test
    void sameSourceIdentityWithReplicaDerivedPayloadDifferenceIsAcknowledgedAsDuplicate() {
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate(1L, 0L);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestIdempotencyGuard guard = guard(jdbc, registry, false);
        HyperliquidMappedDelta delta = delta("same-source-key", "BTCUSDT");

        assertTrue(guard.tryAcquire(delta, "replica-a-derived-state"));
        assertFalse(guard.tryAcquire(delta, "replica-b-derived-state"));
        assertEquals(1.0d, registry.find("signals.hyperliquid.direct_ingest.distributed_dedupe.total")
                .tag("result", "replica_payload_divergence").counter().count());
        assertEquals(1.0d, registry.find("replica_payload_divergence_total")
                .tag("delta_type", "open").counter().count());
    }

    @Test
    void sameEconomicPayloadWithReplicaLocalEventIdsIsHealthyDuplicate() {
        FakeJdbcTemplate jdbc = new FakeJdbcTemplate(1L, 0L);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDirectIngestIdempotencyGuard guard = guard(jdbc, registry, false);

        assertTrue(guard.tryAcquire(deltaWithEventId("replica-a-event"), "same-economic-payload"));
        assertFalse(guard.tryAcquire(deltaWithEventId("replica-b-event"), "same-economic-payload"));
        var duplicate = registry.find("signals.hyperliquid.direct_ingest.distributed_dedupe.total")
                .tag("result", "duplicate").counter();
        assertNotNull(duplicate, "replica-local eventId must not create a semantic divergence");
        assertEquals(1.0d, duplicate.count());
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

    private static HyperliquidMappedDelta deltaWithEventId(String eventId) {
        var request = new com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest(
                eventId,
                "same-source-key",
                "HYPERLIQUID_POSITION_RESIZED",
                "RESIZE",
                "hyperliquid",
                "0xabc",
                "account-1",
                "HYPEUSDT",
                "LONG",
                "OPEN",
                new BigDecimal("72.67"),
                new BigDecimal("72.67"),
                new BigDecimal("4214.86"),
                new BigDecimal("1404.953333333333333333"),
                new BigDecimal("58"),
                new BigDecimal("58"),
                new BigDecimal("3"),
                new BigDecimal("4214.86"),
                new BigDecimal("4214.86"),
                null,
                null,
                null,
                null,
                new BigDecimal("58"),
                null,
                "RECOVERED",
                "production_replica_divergence",
                1784740813038L,
                Instant.parse("2026-07-22T17:20:13.446Z"),
                Instant.parse("2026-07-22T17:20:13.447Z"),
                57L,
                318L,
                "wallet|HYPEUSDT|RESIZE|1784740813038|HYPE|867848868617277|hash",
                "trade-sanitized-b001",
                true
        );
        return new HyperliquidMappedDelta(
                "same-source-key",
                "hyperliquid-position:0xabc:HYPEUSDT:LONG",
                "0xabc",
                "HYPEUSDT",
                "LONG",
                "RESIZE",
                null,
                request
        );
    }

    private static HyperliquidMappedDelta authoritative(
            com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest request
    ) {
        return new HyperliquidMappedDelta(
                "hyperliquid:trade:wallet_sanitized_e001:23131303191059",
                "hyperliquid-position:wallet_sanitized_e001:ONDOUSDT:SHORT",
                request.wallet(),
                request.symbol(),
                request.side(),
                request.deltaType(),
                null,
                request
        );
    }

    private static final class FakeJdbcTemplate extends JdbcTemplate {
        private final List<Long> acquireResults = new ArrayList<>();
        private int acquireIndex;
        private String storedFingerprint;
        private String storedWallet;
        private String storedSymbol;
        private Long storedSourceTs;
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
                storedWallet = String.valueOf(args[3]);
                storedSymbol = String.valueOf(args[4]);
                storedSourceTs = args[7] instanceof Number number ? number.longValue() : null;
            }
            return requiredType.cast(result);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
            try {
                Class<?> claimType = Class.forName(
                        HyperliquidDirectIngestIdempotencyGuard.class.getName() + "$ExistingClaim");
                Constructor<?> constructor = claimType.getDeclaredConstructor(
                        String.class, String.class, boolean.class, String.class, String.class, Long.class);
                constructor.setAccessible(true);
                return List.of((T) constructor.newInstance(
                        storedFingerprint, "PROCESSED", false,
                        storedWallet, storedSymbol, storedSourceTs));
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
