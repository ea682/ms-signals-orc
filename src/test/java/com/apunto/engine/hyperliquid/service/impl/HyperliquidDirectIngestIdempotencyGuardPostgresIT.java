package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Explicit PostgreSQL 16 certification for Sentinel active-active conflicts.
 */
class HyperliquidDirectIngestIdempotencyGuardPostgresIT {

    private static DataSource dataSource;
    private static JdbcTemplate jdbc;

    @BeforeAll
    static void applyRelevantProductionMigrations() {
        String url = System.getProperty(
                "hl.it.db.url",
                "jdbc:postgresql://127.0.0.1:55441/hyperliquid_it");
        assertLocalDatabase(url);
        DriverManagerDataSource configured = new DriverManagerDataSource();
        configured.setDriverClassName("org.postgresql.Driver");
        configured.setUrl(url);
        configured.setUsername(System.getProperty(
                "hl.it.db.user", "postgres"));
        configured.setPassword(System.getProperty(
                "hl.it.db.password", "codex_hl_only"));
        dataSource = configured;
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE SCHEMA IF NOT EXISTS futuros_operaciones");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS
                    futuros_operaciones.hyperliquid_direct_ingest_dedupe (
                    idempotency_key varchar(600) PRIMARY KEY,
                    dedupe_key varchar(600) NOT NULL,
                    position_key varchar(300),
                    wallet varchar(180),
                    symbol varchar(40),
                    side varchar(20),
                    delta_type varchar(30),
                    source_ts_ms bigint,
                    status varchar(30) NOT NULL DEFAULT 'PROCESSING',
                    attempt_count integer NOT NULL DEFAULT 1,
                    duplicate_count integer NOT NULL DEFAULT 0,
                    first_seen_at timestamptz NOT NULL DEFAULT now(),
                    last_seen_at timestamptz NOT NULL DEFAULT now(),
                    lease_until timestamptz,
                    processed_at timestamptz,
                    failed_at timestamptz,
                    last_reason_code varchar(120),
                    last_error_class varchar(180),
                    last_error_message varchar(1000),
                    CONSTRAINT chk_hl_direct_dedupe_status_it CHECK (
                        status IN (
                            'PROCESSING', 'PROCESSED', 'FAILED', 'REJECTED'))
                )
                """);
        migrate(
                "db/migration/"
                        + "V202607110003__hyperliquid_dedupe_payload_fingerprint.sql",
                "db/migration/"
                        + "V202607250001__hyperliquid_replica_payload_conflict.sql");
    }

    @BeforeEach
    void clearIsolatedFixture() {
        jdbc.execute("""
                TRUNCATE TABLE
                    futuros_operaciones.hyperliquid_replica_payload_conflict,
                    futuros_operaciones.hyperliquid_direct_ingest_dedupe
                RESTART IDENTITY
                """);
    }

    @Test
    void replicaLocalKeysAreHealthyDuplicateInPostgres() {
        HyperliquidDirectIngestIdempotencyGuard guard = guard();
        HyperliquidMappedDelta first =
                delta("source-identity-1", "replica-a-position", "LONG", "OPEN");
        HyperliquidMappedDelta second =
                delta("source-identity-1", "replica-b-position", "LONG", "OPEN");

        assertTrue(guard.tryAcquire(first, "replica-a-dedupe"));
        assertFalse(guard.tryAcquire(second, "replica-b-dedupe"));

        assertEquals(1, count(
                "futuros_operaciones.hyperliquid_direct_ingest_dedupe"));
        assertEquals(0, count(
                "futuros_operaciones.hyperliquid_replica_payload_conflict"));
        assertEquals(1, jdbc.queryForObject("""
                SELECT duplicate_count
                FROM futuros_operaciones.hyperliquid_direct_ingest_dedupe
                WHERE idempotency_key = 'source-identity-1'
                """, Integer.class));
    }

    @Test
    void realSemanticDivergenceIsPersistedButNeverAcquiredTwice() {
        HyperliquidDirectIngestIdempotencyGuard guard = guard();
        HyperliquidMappedDelta first =
                delta("source-identity-2", "replica-a-position", "LONG", "OPEN");
        HyperliquidMappedDelta conflicting =
                delta("source-identity-2", "replica-b-position", "SHORT", "CLOSE");

        assertTrue(guard.tryAcquire(first, "replica-a-dedupe"));
        assertFalse(guard.tryAcquire(conflicting, "replica-b-dedupe"));

        assertEquals(1, count(
                "futuros_operaciones.hyperliquid_direct_ingest_dedupe"));
        assertEquals(1, count(
                "futuros_operaciones.hyperliquid_replica_payload_conflict"));
        assertEquals("UNRESOLVED", jdbc.queryForObject("""
                SELECT resolution_status
                FROM futuros_operaciones.hyperliquid_replica_payload_conflict
                WHERE idempotency_key = 'source-identity-2'
                """, String.class));
        String differing = jdbc.queryForObject("""
                SELECT differing_fields::text
                FROM futuros_operaciones.hyperliquid_replica_payload_conflict
                WHERE idempotency_key = 'source-identity-2'
                """, String.class);
        assertTrue(differing.contains("side"));
        assertTrue(differing.contains("deltaType"));
    }

    @Test
    void concurrentSignalsReplicasGrantExactlyOneEconomicClaim()
            throws Exception {
        HyperliquidMappedDelta input =
                delta("source-identity-3", "stable-position", "LONG", "OPEN");
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<Boolean> first = executor.submit(() -> {
                start.await(5, TimeUnit.SECONDS);
                return guard().tryAcquire(input, "replica-a");
            });
            Future<Boolean> second = executor.submit(() -> {
                start.await(5, TimeUnit.SECONDS);
                return guard().tryAcquire(input, "replica-b");
            });
            start.countDown();
            List<Boolean> outcomes = List.of(
                    first.get(10, TimeUnit.SECONDS),
                    second.get(10, TimeUnit.SECONDS));

            assertEquals(1L, outcomes.stream().filter(Boolean::booleanValue).count());
            assertEquals(1, count(
                    "futuros_operaciones.hyperliquid_direct_ingest_dedupe"));
        }
    }

    private HyperliquidDirectIngestIdempotencyGuard guard() {
        HyperliquidDirectIngestProperties properties =
                new HyperliquidDirectIngestProperties();
        properties.setDistributedDedupeEnabled(true);
        properties.setFailOpenOnDedupeError(false);
        properties.setDedupeLeaseTtlMs(60_000L);
        return new HyperliquidDirectIngestIdempotencyGuard(
                properties,
                new JdbcTemplate(dataSource),
                new SimpleMeterRegistry(),
                new ObjectMapper().findAndRegisterModules());
    }

    private HyperliquidMappedDelta delta(
            String identity,
            String positionKey,
            String side,
            String deltaType
    ) {
        return new HyperliquidMappedDelta(
                identity,
                positionKey,
                "0xabc",
                "HYPEUSDT",
                side,
                deltaType,
                null,
                null);
    }

    private int count(String table) {
        Integer value = jdbc.queryForObject(
                "SELECT count(*) FROM " + table, Integer.class);
        return value == null ? 0 : value;
    }

    private static void migrate(String... resources) {
        ResourceDatabasePopulator migration = new ResourceDatabasePopulator();
        migration.setContinueOnError(false);
        for (String resource : resources) {
            migration.addScript(new ClassPathResource(resource));
        }
        migration.execute(dataSource);
    }

    private static void assertLocalDatabase(String jdbcUrl) {
        URI uri = URI.create(jdbcUrl.replaceFirst("^jdbc:", ""));
        String host = uri.getHost();
        if (!"127.0.0.1".equals(host) && !"localhost".equalsIgnoreCase(host)) {
            throw new IllegalStateException(
                    "PostgreSQL integration test refuses non-local host: "
                            + host);
        }
    }
}
