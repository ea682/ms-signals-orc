package com.apunto.engine.service.metric;

import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static com.apunto.engine.dto.client.MetricStrategySnapshotTestFixtures.completeMatrix;

class MetricV2SnapshotPostgresIntegrationTest {

    private static final String WINDOWS = "1d,3d,1w,2w,3w,1mo,2mo,3mo,6mo,9mo,1y,2y,all";

    @Test
    void migrationBackfillsCanonicalIdentityAndSnapshotRoundTripsAtomically() throws Exception {
        String url = System.getProperty("metric.v2.pg.url");
        assumeTrue(url != null && !url.isBlank(), "metric.v2.pg.url enables the isolated PostgreSQL test");
        String user = System.getProperty("metric.v2.pg.user", "postgres");
        String password = System.getProperty("metric.v2.pg.password", "");
        DataSource dataSource = new DriverManagerDataSource(url, user, password);

        preparePreMigrationSchema(dataSource);

        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .schemas("futuros_operaciones")
                .defaultSchema("futuros_operaciones")
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .baselineVersion("202607130006")
                .load();
        flyway.migrate();

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        for (String mode : List.of("SHADOW", "MICRO_LIVE", "LIVE")) {
            assertEquals(1, insertSimulationJob(jdbc, mode));
        }
        assertEquals(
                "0xabc|MOVEMENT_ALL|ALL|ALL",
                jdbc.queryForObject("SELECT strategy_key FROM futuros_operaciones.user_copy_allocation WHERE id = 1", String.class)
        );
        assertEquals(
                "0xabc|LONG_ONLY|DIRECTION|LONG",
                jdbc.queryForObject("SELECT strategy_key FROM futuros_operaciones.shadow_copy_allocation WHERE id = 1", String.class)
        );
        assertEquals(
                "0xabc|LOW_LEVERAGE_ONLY|LEVERAGE_RANGE|<=5",
                jdbc.queryForObject("SELECT profile_key FROM futuros_operaciones.copy_wallet_profile WHERE id = 1", String.class)
        );

        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        PostgresMetricV2SnapshotPersistence persistence = new PostgresMetricV2SnapshotPersistence(
                jdbc,
                new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
                mapper
        );
        MetricStrategySnapshotDto summary = snapshot(false, false);
        MetricStrategySnapshotDto full = snapshot(true, false);
        MetricStrategySnapshotDto guard = snapshot(true, true);
        Instant fetchedAt = Instant.now();
        MetricV2SnapshotStore.Snapshot expected = new MetricV2SnapshotStore.Snapshot(
                Map.of(summary.getStrategyKey(), summary),
                Map.of(full.getStrategyKey(), full),
                Map.of(guard.getStrategyKey(), guard),
                Map.of("0xabc", "gen-1"),
                fetchedAt,
                fetchedAt,
                fetchedAt
        );

        persistence.replace(expected, Duration.ofMinutes(10));
        MetricV2SnapshotStore.Snapshot restored = persistence.load().orElseThrow();

        assertEquals(3, jdbc.queryForObject(
                "SELECT count(*) FROM futuros_operaciones.metric_strategy_snapshot_v2",
                Integer.class
        ));
        assertEquals(expected.summaryByKey().keySet(), restored.summaryByKey().keySet());
        assertEquals(expected.fullByKey().keySet(), restored.fullByKey().keySet());
        assertEquals(expected.guardByKey().keySet(), restored.guardByKey().keySet());
        assertEquals("gen-1", restored.walletGenerations().get("0xabc"));
        assertTrue(restored.fullByKey().values().iterator().next().isEligibleForShadow());
        assertFalse(restored.summaryByKey().values().iterator().next().isEligibleForShadow());

        MetricV2SnapshotStore.Snapshot second = aggregate("gen-2", Instant.now());
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<? extends Future<?>> writes = java.util.stream.IntStream.range(0, 12)
                    .mapToObj(index -> executor.submit(() -> {
                        start.await();
                        persistence.replace(index % 2 == 0 ? expected : second, Duration.ofMinutes(10));
                        return null;
                    }))
                    .toList();
            start.countDown();
            for (Future<?> write : writes) write.get();
        } finally {
            executor.shutdownNow();
        }
        assertEquals(3, jdbc.queryForObject(
                "SELECT count(*) FROM futuros_operaciones.metric_strategy_snapshot_v2",
                Integer.class
        ));
        assertEquals(1, jdbc.queryForObject(
                "SELECT count(DISTINCT generation_id) FROM futuros_operaciones.metric_strategy_snapshot_v2",
                Integer.class
        ));
    }

    private static void preparePreMigrationSchema(DataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT current_database()")) {
                rs.next();
                String database = rs.getString(1);
                if (database == null || !database.startsWith("metric_v2_test_")) {
                    throw new IllegalStateException("REFUSING_NON_ISOLATED_DATABASE:" + database);
                }
            }
            statement.execute("DROP SCHEMA IF EXISTS futuros_operaciones CASCADE");
            statement.execute("CREATE SCHEMA futuros_operaciones");
            statement.execute("""
                    CREATE TABLE futuros_operaciones.user_copy_allocation (
                        id bigint PRIMARY KEY, id_user uuid NOT NULL, wallet_id varchar(128) NOT NULL,
                        copy_strategy_code varchar(64), scope_type varchar(32), scope_value varchar(160),
                        strategy_key varchar(420), ends_at timestamptz, is_active boolean NOT NULL DEFAULT true
                    )
                    """);
            statement.execute("""
                    CREATE TABLE futuros_operaciones.shadow_copy_allocation (
                        id bigint PRIMARY KEY, id_user uuid NOT NULL, wallet_id varchar(128) NOT NULL,
                        copy_strategy_code varchar(64), scope_type varchar(32), scope_value varchar(160),
                        strategy_key varchar(420), shadow_version integer NOT NULL DEFAULT 1,
                        ends_at timestamptz, is_active boolean NOT NULL DEFAULT true
                    )
                    """);
            statement.execute("""
                    CREATE TABLE futuros_operaciones.copy_wallet_profile (
                        id bigint PRIMARY KEY, wallet_id varchar(128) NOT NULL, copy_profile_code varchar(64),
                        scope_type varchar(32), scope_value varchar(160), profile_key varchar(420)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE futuros_operaciones.shadow_copy_operation (
                        id_operation uuid PRIMARY KEY, id_wallet_origin varchar(128) NOT NULL,
                        copy_strategy_code varchar(64), scope_type varchar(32), scope_value varchar(160),
                        strategy_key varchar(420), parsymbol varchar(40), type_operation varchar(20),
                        is_active boolean NOT NULL DEFAULT true
                    )
                    """);
            statement.execute("""
                    CREATE TABLE futuros_operaciones.shadow_copy_operation_event (
                        id_event uuid PRIMARY KEY, id_wallet_origin varchar(128) NOT NULL,
                        copy_strategy_code varchar(64), scope_type varchar(32), scope_value varchar(160),
                        strategy_key varchar(420)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE futuros_operaciones.shadow_position_state (
                        id uuid PRIMARY KEY, wallet_id varchar(128) NOT NULL, copy_strategy_code varchar(64),
                        scope_type varchar(32), scope_value varchar(160), strategy_key varchar(420),
                        parsymbol varchar(40), position_side varchar(20), status varchar(32)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE futuros_operaciones.copy_dispatch_intent (
                        id uuid PRIMARY KEY
                    )
                    """);
            statement.execute("""
                    CREATE TABLE futuros_operaciones.copy_operation_event (
                        id_event uuid PRIMARY KEY,
                        source_movement_key varchar(600),
                        execution_mode varchar(20),
                        event_time timestamptz NOT NULL DEFAULT now()
                    )
                    """);
            statement.execute("SET search_path TO futuros_operaciones, public");
            ScriptUtils.executeSqlScript(
                    connection,
                    new ClassPathResource("db/migration/V202607130003__copy_simulation_certification_v3.sql")
            );
            statement.execute("""
                    INSERT INTO futuros_operaciones.user_copy_allocation
                        (id, id_user, wallet_id, copy_strategy_code, scope_type, scope_value, strategy_key)
                    VALUES
                        (1, '00000000-0000-0000-0000-000000000001', '0xAbC', 'movement-all', 'strategy', 'MOVEMENT_ALL', 'legacy-user')
                    """);
            statement.execute("""
                    INSERT INTO futuros_operaciones.shadow_copy_allocation
                        (id, id_user, wallet_id, copy_strategy_code, scope_type, scope_value, strategy_key)
                    VALUES
                        (1, '00000000-0000-0000-0000-000000000001', '0xAbC', 'long_only', 'default', 'LONG_ONLY', 'legacy-shadow')
                    """);
            statement.execute("""
                    INSERT INTO futuros_operaciones.copy_wallet_profile
                        (id, wallet_id, copy_profile_code, scope_type, scope_value, profile_key)
                    VALUES (1, '0xAbC', 'low_leverage_only', 'strategy', '<=5', 'legacy-profile')
                    """);
        }
    }

    private static int insertSimulationJob(JdbcTemplate jdbc, String executionMode) {
        return jdbc.update("""
                INSERT INTO futuros_operaciones.copy_simulation_job_v3 (
                    id, idempotency_key, input_hash, source_event_id, source_snapshot_version,
                    allocation_id, user_id, wallet_id, strategy_code, strategy_version,
                    scope_type, scope_value, sizing_policy_version, symbol_mapping_version,
                    execution_mode, input_snapshot, strategy_key, generation_id, generation_status
                ) VALUES (
                    ?, ?, repeat('a', 64), ?, 1,
                    NULL, 'user-1', '0xabc', 'MOVEMENT_ALL', 'v1',
                    'ALL', 'ALL', 'sizing-v1', 'symbol-v1',
                    ?, '{}'::jsonb, '0xabc|MOVEMENT_ALL|ALL|ALL', 'gen-1', 'KNOWN'
                )
                """,
                UUID.randomUUID(),
                "metric-v2-pg-mode-" + executionMode,
                "metric-v2-pg-event-" + executionMode,
                executionMode
        );
    }

    private static MetricStrategySnapshotDto snapshot(boolean full, boolean guard) {
        return snapshot("gen-1", full, guard);
    }

    private static MetricV2SnapshotStore.Snapshot aggregate(String generation, Instant fetchedAt) {
        MetricStrategySnapshotDto summary = snapshot(generation, false, false);
        MetricStrategySnapshotDto full = snapshot(generation, true, false);
        MetricStrategySnapshotDto guard = snapshot(generation, true, true);
        return new MetricV2SnapshotStore.Snapshot(
                Map.of(summary.getStrategyKey(), summary),
                Map.of(full.getStrategyKey(), full),
                Map.of(guard.getStrategyKey(), guard),
                Map.of("0xabc", generation),
                fetchedAt,
                fetchedAt,
                fetchedAt
        );
    }

    private static MetricStrategySnapshotDto snapshot(String generation, boolean full, boolean guard) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(1);
        MetricStrategySnapshotDto dto = MetricStrategySnapshotDto.builder()
                .metricVersion(2)
                .sourceVersion(MetricStrategySnapshotDto.SOURCE_VERSION)
                .generationId(generation)
                .generationStatus("ACTIVE")
                .readMode("V2")
                .responseSource(MetricStrategySnapshotDto.RESPONSE_SOURCE)
                .calculatorVersion("wallet-strategy-financial-v3.0.0")
                .policyVersion("wallet-strategy-race-v3.0.0")
                .coveragePct(100.0)
                .evidenceStatus("PASSED")
                .factPayloadLoaded(full && !guard)
                .generationActivatedAt(now.minusMinutes(5))
                .computedAt(now)
                .dataAsOf(now)
                .walletId("0xabc")
                .strategyCode("MOVEMENT_ALL")
                .scopeType("ALL")
                .scopeValue("ALL")
                .strategyKey("0xabc|MOVEMENT_ALL|ALL|ALL")
                .certificationStatus(full ? "CERTIFIED" : "CANDIDATE")
                .degradationState("ACTIVE")
                .allowNewEntries(full)
                .decisionFinal(full)
                .qualityFlags(List.of())
                .reasonCodes(List.of())
                .completeCycles(40)
                .historyDays(60)
                .dataFreshnessSeconds(1L)
                .coverage(MetricStrategySnapshotDto.CoverageDto.builder()
                        .status("COMPLETE").complete(true).completeCycles(40)
                        .factsReturned(40).factsAvailable(40).truncated(false).build())
                .unknownEconomicFields(List.of())
                .evaluationMode(full
                        ? MetricStrategySnapshotDto.EvaluationMode.FULL
                        : MetricStrategySnapshotDto.EvaluationMode.SUMMARY)
                .decisionUse(full ? "SHADOW" : "DISCOVERY_ONLY")
                .requiresFullSimulation(!full)
                .allowsMoney(false)
                .eligibleForShadow(full)
                .build();
        if (guard) dto.setWindows(allWindows());
        if (full && !guard) {
            dto.setSimulationMatrix(completeMatrix(
                    "0xabc|MOVEMENT_ALL|ALL|ALL", generation));
        }
        return dto;
    }

    private static Map<String, MetricStrategySnapshotDto.WindowDto> allWindows() {
        Map<String, MetricStrategySnapshotDto.WindowDto> values = new LinkedHashMap<>();
        for (String window : WINDOWS.split(",")) {
            values.put(window, MetricStrategySnapshotDto.WindowDto.builder()
                    .mature(true).complete(true).cycles(40).pnlNetUsd(10.0)
                    .coveragePct(100.0).reasonCodes(List.of()).build());
        }
        return values;
    }
}
