package com.apunto.engine.service.copy.allocation;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.PostgreSQLContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveAllocationPercentagePostgres16IntegrationTest {

    private static final UUID SAFE_MICRO_USER = UUID.fromString("00000000-0000-0000-0000-000000000505");
    private static final UUID AMBIGUOUS_MICRO_USER = UUID.fromString("00000000-0000-0000-0000-000000000506");
    private static final UUID SUSPICIOUS_LIVE_USER = UUID.fromString("00000000-0000-0000-0000-000000000507");
    private static final UUID AMBIGUOUS_LIVE_USER = UUID.fromString("00000000-0000-0000-0000-000000000508");
    private static final UUID INVALID_LIVE_USER = UUID.fromString("00000000-0000-0000-0000-000000000509");

    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static PostgresLiveAllocationDistributionService service;

    @BeforeAll
    static void startPostgresAndMigrate() throws Exception {
        String externalUrl = System.getProperty("copy.postgres.test.jdbc-url");
        HikariConfig config = new HikariConfig();
        if (externalUrl == null || externalUrl.isBlank()) {
            postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("allocation_contract_test")
                    .withUsername("copy_test")
                    .withPassword("copy_test");
            try {
                postgres.start();
            } catch (RuntimeException unavailable) {
                Assumptions.assumeTrue(false, "Docker unavailable for PostgreSQL 16 allocation contract test");
            }
            config.setJdbcUrl(postgres.getJdbcUrl());
            config.setUsername(postgres.getUsername());
            config.setPassword(postgres.getPassword());
        } else {
            config.setJdbcUrl(externalUrl);
            config.setUsername(System.getProperty("copy.postgres.test.username", "postgres"));
            config.setPassword(System.getProperty("copy.postgres.test.password", ""));
        }
        config.setMaximumPoolSize(4);
        dataSource = new HikariDataSource(config);
        jdbc = new JdbcTemplate(dataSource);

        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            try (ResultSet version = statement.executeQuery("show server_version_num")) {
                assertTrue(version.next());
                int versionNumber = Integer.parseInt(version.getString(1));
                assertTrue(versionNumber >= 160000 && versionNumber < 170000,
                        "integration test requires PostgreSQL 16, got " + versionNumber);
            }
            statement.execute("create schema futuros_operaciones");
            statement.execute("""
                    create table futuros_operaciones.shadow_copy_allocation (
                        id bigint primary key,
                        allocation_pct numeric(9, 6) not null default 0,
                        target_live_allocation_pct numeric(9, 6)
                    )
                    """);
            statement.execute("""
                    create table futuros_operaciones.user_copy_allocation (
                        id bigserial primary key,
                        id_user uuid not null,
                        wallet_id varchar(128) not null,
                        copy_strategy_code varchar(64) not null default 'MOVEMENT_ALL',
                        scope_type varchar(32) not null default 'all',
                        scope_value varchar(160) not null default 'ALL',
                        allocation_pct numeric(9, 6) not null,
                        status varchar(40) not null default 'active',
                        is_active boolean not null default true,
                        ends_at timestamptz,
                        execution_mode varchar(16) not null,
                        linked_shadow_allocation_id bigint,
                        promoted_from_shadow_at timestamptz,
                        status_reason varchar(160),
                        status_updated_at timestamptz,
                        updated_at timestamptz not null default now()
                    )
                    """);
            statement.execute("""
                    create table futuros_operaciones.user_wallet_copy_plan (
                        id bigserial primary key,
                        id_user uuid not null,
                        wallet_lc text not null,
                        allocation_pct numeric(9, 6) not null,
                        allocated_capital_usd numeric(18, 8),
                        created_at timestamptz not null default now(),
                        updated_at timestamptz not null default now(),
                        constraint uq_user_wallet_copy_plan_user_wallet unique (id_user, wallet_lc),
                        constraint chk_user_wallet_copy_plan_pct
                            check (allocation_pct >= 0 and allocation_pct <= 1)
                    )
                    """);
        }
        seedLegacyRows();
        executeMigration();
        executeMigration();
        service = new PostgresLiveAllocationDistributionService(
                jdbc, new SimpleMeterRegistry(), Duration.ofMinutes(5));
    }

    @AfterAll
    static void stopPostgres() {
        if (dataSource != null) dataSource.close();
        if (postgres != null && postgres.isRunning()) postgres.stop();
    }

    @Test
    void migrationClassifiesOnlyProvenSentinelsAndIsLogicallyIdempotent() {
        AllocationRow safeMicro = allocation(505L);
        assertEquals("MICRO_LIVE", safeMicro.executionMode());
        assertEquals(null, safeMicro.allocationPct());
        assertEquals("FIXED_CAPITAL", safeMicro.sizingMode());
        assertEquals("FIXED_MICRO_BUDGET", safeMicro.source());

        AllocationRow ambiguousMicro = allocation(506L);
        assertEquals(new BigDecimal("0.000001"), ambiguousMicro.allocationPct());
        assertEquals("FIXED_CAPITAL", ambiguousMicro.sizingMode());
        assertEquals("LEGACY_MICRO_PCT_IGNORED", ambiguousMicro.source());

        AllocationRow suspiciousLive = allocation(507L);
        assertEquals("paused_by_risk", suspiciousLive.status());
        assertEquals("LEGACY_LIVE_ALLOCATION_PCT_INVALID", suspiciousLive.statusReason());
        assertEquals("LEGACY_MICRO_LIVE_SENTINEL", suspiciousLive.source());

        AllocationRow ambiguousLive = allocation(508L);
        assertEquals("active", ambiguousLive.status());
        assertEquals(new BigDecimal("0.000001"), ambiguousLive.allocationPct());
        assertEquals("LEGACY_ECONOMIC_ALLOCATION", ambiguousLive.source());

        AllocationRow invalidLive = allocation(509L);
        assertEquals("paused_by_risk", invalidLive.status());
        assertEquals("LEGACY_LIVE_ALLOCATION_PCT_INVALID", invalidLive.statusReason());

        PlanRow plan = jdbc.queryForObject("""
                        select allocation_pct, allocated_capital_usd, sizing_mode, allocation_pct_source
                        from futuros_operaciones.user_wallet_copy_plan
                        where id_user = ? and wallet_lc = '0xsafe'
                        """,
                (rs, rowNum) -> new PlanRow(
                        rs.getBigDecimal("allocation_pct"),
                        rs.getBigDecimal("allocated_capital_usd"),
                        rs.getString("sizing_mode"),
                        rs.getString("allocation_pct_source")),
                SAFE_MICRO_USER);
        assertNotNull(plan);
        assertEquals(null, plan.allocationPct());
        assertEquals(new BigDecimal("100.00000000"), plan.allocatedCapital());
        assertEquals("FIXED_CAPITAL", plan.sizingMode());
        assertEquals("FIXED_MICRO_BUDGET", plan.source());
    }

    @Test
    void constraintsAllowFixedMicroAndTracedTinyLiveButRejectNewSentinelMicroAndInvalidLive() {
        UUID fixedUser = UUID.randomUUID();
        int fixedInserted = jdbc.update("""
                insert into futuros_operaciones.user_copy_allocation(
                    id_user, wallet_id, copy_strategy_code, scope_type, scope_value,
                    allocation_pct, sizing_mode, allocation_pct_source,
                    status, is_active, execution_mode, updated_at
                ) values (?, '0xfixed', 'MOVEMENT_ALL', 'all', 'ALL',
                          null, 'FIXED_CAPITAL', 'FIXED_MICRO_BUDGET',
                          'active', true, 'MICRO_LIVE', now())
                """, fixedUser);
        assertEquals(1, fixedInserted);

        assertThrows(DataAccessException.class, () -> jdbc.update("""
                insert into futuros_operaciones.user_copy_allocation(
                    id_user, wallet_id, copy_strategy_code, scope_type, scope_value,
                    allocation_pct, sizing_mode, allocation_pct_source,
                    status, is_active, execution_mode, updated_at
                ) values (?, '0xsentinel', 'MOVEMENT_ALL', 'all', 'ALL',
                          0.000001, 'FIXED_CAPITAL', 'FIXED_MICRO_BUDGET',
                          'active', true, 'MICRO_LIVE', now())
                """, UUID.randomUUID()));

        assertThrows(DataAccessException.class, () -> jdbc.update("""
                insert into futuros_operaciones.user_copy_allocation(
                    id_user, wallet_id, copy_strategy_code, scope_type, scope_value,
                    allocation_pct, sizing_mode, allocation_pct_source,
                    status, is_active, execution_mode, updated_at
                ) values (?, '0xlive-null', 'MOVEMENT_ALL', 'all', 'ALL',
                          null, 'PERCENTAGE', 'SIGNALS_CURRENT_LIVE_DISTRIBUTION',
                          'active', true, 'LIVE', now())
                """, UUID.randomUUID()));

        UUID sourceId = UUID.randomUUID();
        int tinyLive = jdbc.update("""
                insert into futuros_operaciones.user_copy_allocation(
                    id_user, wallet_id, copy_strategy_code, scope_type, scope_value,
                    allocation_pct, sizing_mode, allocation_pct_source, allocation_pct_source_id,
                    allocation_pct_calculated_at, allocation_pct_valid_until, wallet_total_allocation_pct,
                    status, is_active, execution_mode, updated_at
                ) values (?, '0xtiny-live', 'MOVEMENT_ALL', 'all', 'ALL',
                          0.000001, 'PERCENTAGE', 'SIGNALS_CURRENT_LIVE_DISTRIBUTION', ?,
                          now(), now() + interval '5 minutes', 0.000001,
                          'active', true, 'LIVE', now())
                """, UUID.randomUUID(), sourceId);
        assertEquals(1, tinyLive);
    }

    @Test
    void exactStrategyDistributionPreventsWalletPercentageMultiplication() {
        UUID userId = UUID.randomUUID();
        OffsetDateTime calculatedAt = OffsetDateTime.now().minusSeconds(1);
        UUID distributionId = service.publish(userId, List.of(
                new LiveAllocationDistributionEntry(
                        "0xwallet", "MOVEMENT_ALL", "all", "ALL", new BigDecimal("0.060000")),
                new LiveAllocationDistributionEntry(
                        "0xwallet", "SHORT_ONLY", "direction", "SHORT", new BigDecimal("0.040000"))
        ), calculatedAt);

        LiveAllocationPercentageResolution movement = service.resolve(new LiveAllocationPercentageRequest(
                userId, "0xwallet", "MOVEMENT_ALL", "all", "ALL", OffsetDateTime.now()));
        LiveAllocationPercentageResolution shortOnly = service.resolve(new LiveAllocationPercentageRequest(
                userId, "0xwallet", "SHORT_ONLY", "direction", "SHORT", OffsetDateTime.now()));

        assertTrue(movement.validForLive());
        assertTrue(shortOnly.validForLive());
        assertEquals(distributionId, movement.sourceId());
        assertEquals(new BigDecimal("0.060000"), movement.percentage());
        assertEquals(new BigDecimal("0.040000"), shortOnly.percentage());
        assertEquals(new BigDecimal("0.100000"), movement.walletTotalPercentage());
        assertEquals(new BigDecimal("0.100000"), shortOnly.walletTotalPercentage());

        BigDecimal persistedWalletSum = jdbc.queryForObject("""
                select sum(strategy_allocation_pct)
                from futuros_operaciones.live_allocation_distribution_detail
                where distribution_id = ? and wallet_lc = '0xwallet'
                """, BigDecimal.class, distributionId);
        assertEquals(new BigDecimal("0.100000"), persistedWalletSum);

        LiveAllocationPercentageResolution mismatch = service.resolve(new LiveAllocationPercentageRequest(
                userId, "0xwallet", "LONG_ONLY", "direction", "LONG", OffsetDateTime.now()));
        assertFalse(mismatch.resolved());
        assertEquals("LIVE_ALLOCATION_PCT_SCOPE_MISMATCH", mismatch.reasonCode());
    }

    @Test
    void stagedFailedStaleEmptyAndAggregateMismatchAllFailClosed() {
        UUID stagedUser = UUID.randomUUID();
        OffsetDateTime calculatedAt = OffsetDateTime.now().minusSeconds(1);
        LiveAllocationDistributionPublication staged = service.stage(stagedUser, List.of(
                new LiveAllocationDistributionEntry(
                        "0xstage", "MOVEMENT_ALL", "all", "ALL", new BigDecimal("0.100000"))
        ), calculatedAt);
        LiveAllocationPercentageRequest stagedRequest = new LiveAllocationPercentageRequest(
                stagedUser, "0xstage", "MOVEMENT_ALL", "all", "ALL", OffsetDateTime.now());
        assertEquals("LIVE_DISTRIBUTION_NOT_AVAILABLE", service.resolve(stagedRequest).reasonCode());
        service.complete(staged.distributionId());
        assertTrue(service.resolve(stagedRequest).resolved());

        UUID failedUser = UUID.randomUUID();
        LiveAllocationDistributionPublication failed = service.stage(failedUser, List.of(
                new LiveAllocationDistributionEntry(
                        "0xfail", "MOVEMENT_ALL", "all", "ALL", new BigDecimal("0.100000"))
        ), OffsetDateTime.now().minusSeconds(1));
        service.fail(failed.distributionId(), "COPY_DISTRIBUTION_UNIT_FAILED");
        assertEquals("LIVE_DISTRIBUTION_NOT_AVAILABLE", service.resolve(new LiveAllocationPercentageRequest(
                failedUser, "0xfail", "MOVEMENT_ALL", "all", "ALL", OffsetDateTime.now())).reasonCode());

        UUID emptyUser = UUID.randomUUID();
        service.publish(emptyUser, List.of(), OffsetDateTime.now().minusSeconds(1));
        assertEquals("LIVE_ALLOCATION_PCT_MISSING", service.resolve(new LiveAllocationPercentageRequest(
                emptyUser, "0xempty", "MOVEMENT_ALL", "all", "ALL", OffsetDateTime.now())).reasonCode());

        UUID staleUser = UUID.randomUUID();
        service.publish(staleUser, List.of(new LiveAllocationDistributionEntry(
                "0xstale", "MOVEMENT_ALL", "all", "ALL", new BigDecimal("0.100000"))),
                OffsetDateTime.now().minusMinutes(10));
        assertEquals("LIVE_ALLOCATION_PCT_STALE", service.resolve(new LiveAllocationPercentageRequest(
                staleUser, "0xstale", "MOVEMENT_ALL", "all", "ALL", OffsetDateTime.now())).reasonCode());

        UUID aggregateUser = UUID.randomUUID();
        UUID aggregateRun = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        jdbc.update("""
                insert into futuros_operaciones.live_allocation_distribution_run(
                    distribution_id, id_user, source, status, user_total_allocation_pct,
                    calculated_at, valid_until
                ) values (?, ?, 'SIGNALS_CURRENT_LIVE_DISTRIBUTION', 'COMPLETED', 0.05, ?, ?)
                """, aggregateRun, aggregateUser, now.minusSeconds(1), now.plusMinutes(5));
        jdbc.update("""
                insert into futuros_operaciones.live_allocation_distribution_detail(
                    distribution_id, id_user, wallet_lc, strategy_code, scope_type, scope_value,
                    strategy_allocation_pct, wallet_total_allocation_pct
                ) values (?, ?, '0xaggregate', 'MOVEMENT_ALL', 'all', 'ALL', 0.06, 0.06)
                """, aggregateRun, aggregateUser);
        assertEquals("LIVE_ALLOCATION_PCT_TOTAL_EXCEEDED", service.resolve(new LiveAllocationPercentageRequest(
                aggregateUser, "0xaggregate", "MOVEMENT_ALL", "all", "ALL", now)).reasonCode());
    }

    @Test
    void publisherRejectsAmbiguousOrOverAllocatedDistributions() {
        assertThrows(IllegalArgumentException.class, () -> service.publish(UUID.randomUUID(), List.of(
                new LiveAllocationDistributionEntry(
                        "0xa", "MOVEMENT_ALL", "all", "ALL", new BigDecimal("0.600000")),
                new LiveAllocationDistributionEntry(
                        "0xb", "SHORT_ONLY", "direction", "SHORT", new BigDecimal("0.600000"))
        ), OffsetDateTime.now()));

        assertThrows(IllegalArgumentException.class, () -> service.publish(UUID.randomUUID(), List.of(
                new LiveAllocationDistributionEntry(
                        "0xa", "MOVEMENT_ALL", "all", "ALL", new BigDecimal("0.100000")),
                new LiveAllocationDistributionEntry(
                        "0xa", "MOVEMENT_ALL", "all", "ALL", new BigDecimal("0.200000"))
        ), OffsetDateTime.now()));

        assertThrows(IllegalArgumentException.class, () -> new LiveAllocationDistributionEntry(
                "0xa", "MOVEMENT_ALL", "all", "ALL", new BigDecimal("0.1234567")));
    }

    @Test
    void concurrentStrategyPlanUpsertsCreateOneWalletPlanWithoutUniqueViolation() throws Exception {
        UUID userId = UUID.randomUUID();
        String wallet = "0xconcurrent-plan";
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Integer> first = pool.submit(() -> upsertAndLockPlan(userId, wallet, start));
            Future<Integer> second = pool.submit(() -> upsertAndLockPlan(userId, wallet, start));
            start.countDown();

            assertEquals(1, first.get(10, TimeUnit.SECONDS) + second.get(10, TimeUnit.SECONDS));
            Integer rows = jdbc.queryForObject("""
                    select count(*) from futuros_operaciones.user_wallet_copy_plan
                    where id_user = ? and wallet_lc = ?
                    """, Integer.class, userId, wallet);
            assertEquals(1, rows);
        } finally {
            pool.shutdownNow();
        }
    }

    private static void seedLegacyRows() {
        jdbc.update("insert into futuros_operaciones.shadow_copy_allocation(id, allocation_pct, target_live_allocation_pct) values (1, 0, null), (2, 0, null)");
        jdbc.update("""
                insert into futuros_operaciones.user_copy_allocation(
                    id, id_user, wallet_id, allocation_pct, status, is_active, execution_mode,
                    linked_shadow_allocation_id, promoted_from_shadow_at, updated_at
                ) values
                    (505, ?, '0xsafe', 0.000001, 'active', true, 'MICRO_LIVE', 1, now() - interval '1 day', now()),
                    (506, ?, '0xambiguous-micro', 0.000001, 'active', true, 'MICRO_LIVE', null, now() - interval '1 day', now()),
                    (507, ?, '0xsuspicious-live', 0.000001, 'active', true, 'LIVE', 2, now() - interval '1 day', now()),
                    (508, ?, '0xambiguous-live', 0.000001, 'active', true, 'LIVE', null, now() - interval '1 day', now()),
                    (509, ?, '0xinvalid-live', 0, 'active', true, 'LIVE', null, now() - interval '1 day', now())
                """, SAFE_MICRO_USER, AMBIGUOUS_MICRO_USER, SUSPICIOUS_LIVE_USER,
                AMBIGUOUS_LIVE_USER, INVALID_LIVE_USER);
        jdbc.update("""
                insert into futuros_operaciones.user_wallet_copy_plan(
                    id_user, wallet_lc, allocation_pct, allocated_capital_usd
                ) values (?, '0xsafe', 0.000001, 100)
                """, SAFE_MICRO_USER);
    }

    private static void executeMigration() {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator(
                new ClassPathResource("db/migration/V202607110007__allocation_percentage_contract.sql"));
        populator.execute(dataSource);
    }

    private static int upsertAndLockPlan(UUID userId, String wallet, CountDownLatch start) throws Exception {
        start.await(5, TimeUnit.SECONDS);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            int inserted;
            try (PreparedStatement upsert = connection.prepareStatement("""
                    insert into futuros_operaciones.user_wallet_copy_plan(
                        id_user, wallet_lc, allocation_pct, allocated_capital_usd,
                        sizing_mode, allocation_pct_source, created_at, updated_at
                    ) values (?, ?, null, 100, 'FIXED_CAPITAL', 'FIXED_MICRO_BUDGET', now(), now())
                    on conflict (id_user, wallet_lc) do nothing
                    """)) {
                upsert.setObject(1, userId);
                upsert.setString(2, wallet);
                inserted = upsert.executeUpdate();
            }
            try (PreparedStatement lock = connection.prepareStatement("""
                    select id from futuros_operaciones.user_wallet_copy_plan
                    where id_user = ? and wallet_lc = ?
                    for update
                    """)) {
                lock.setObject(1, userId);
                lock.setString(2, wallet);
                try (ResultSet row = lock.executeQuery()) {
                    assertTrue(row.next());
                }
            }
            connection.commit();
            return inserted;
        }
    }

    private static AllocationRow allocation(long id) {
        return jdbc.queryForObject("""
                        select execution_mode, allocation_pct, sizing_mode, allocation_pct_source,
                               status, status_reason
                        from futuros_operaciones.user_copy_allocation
                        where id = ?
                        """,
                (rs, rowNum) -> new AllocationRow(
                        rs.getString("execution_mode"),
                        rs.getBigDecimal("allocation_pct"),
                        rs.getString("sizing_mode"),
                        rs.getString("allocation_pct_source"),
                        rs.getString("status"),
                        rs.getString("status_reason")),
                id);
    }

    private record AllocationRow(
            String executionMode,
            BigDecimal allocationPct,
            String sizingMode,
            String source,
            String status,
            String statusReason
    ) {
    }

    private record PlanRow(
            BigDecimal allocationPct,
            BigDecimal allocatedCapital,
            String sizingMode,
            String source
    ) {
    }
}
