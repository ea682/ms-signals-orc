package com.apunto.engine.service.copy.coverage;

import com.apunto.engine.entity.ShadowCopyOperationEventEntity;
import com.apunto.engine.repository.ShadowCopyOperationEventRepository;
import com.apunto.engine.repository.ShadowCoverageCountsProjection;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.support.JpaRepositoryFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowCoveragePostgres16IntegrationTest {

    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private static EntityManagerFactory entityManagerFactory;
    private static boolean externalReadOnly;

    @BeforeAll
    static void startPostgres() throws Exception {
        String externalUrl = System.getProperty("copy.postgres.test.jdbc-url");
        externalReadOnly = externalUrl != null && !externalUrl.isBlank();

        HikariConfig hikari = new HikariConfig();
        if (externalReadOnly) {
            hikari.setJdbcUrl(externalUrl);
            hikari.setUsername(System.getProperty("copy.postgres.test.username"));
            hikari.setPassword(System.getProperty("copy.postgres.test.password"));
            hikari.setReadOnly(true);
        } else {
            postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("copy_trading_test")
                    .withUsername("copy_test")
                    .withPassword("copy_test");
            try {
                postgres.start();
            } catch (RuntimeException unavailable) {
                Assumptions.assumeTrue(false,
                        "Docker unavailable; configure copy.postgres.test.jdbc-url for PostgreSQL 16 read-only verification");
            }
            hikari.setJdbcUrl(postgres.getJdbcUrl());
            hikari.setUsername(postgres.getUsername());
            hikari.setPassword(postgres.getPassword());
        }
        hikari.setMaximumPoolSize(2);
        dataSource = new HikariDataSource(hikari);

        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            try (ResultSet version = statement.executeQuery("show server_version_num")) {
                assertTrue(version.next());
                int versionNumber = Integer.parseInt(version.getString(1));
                assertTrue(versionNumber >= 160000 && versionNumber < 170000,
                        "integration test requires PostgreSQL 16, got " + versionNumber);
            }
            if (!externalReadOnly) {
                statement.execute("create schema futuros_operaciones");
                statement.execute("""
                        create table futuros_operaciones.shadow_copy_operation_event (
                            id_event uuid primary key,
                            shadow_allocation_id bigint not null,
                            decision varchar(24) not null,
                            event_time timestamptz not null
                        )
                        """);
            }
        }

        LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
        factory.setDataSource(dataSource);
        factory.setPackagesToScan("com.apunto.engine.entity");
        factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        factory.setJpaPropertyMap(Map.of(
                "hibernate.hbm2ddl.auto", "none",
                "hibernate.default_schema", "futuros_operaciones",
                "hibernate.jdbc.time_zone", "UTC"
        ));
        factory.afterPropertiesSet();
        entityManagerFactory = factory.getObject();
    }

    @AfterAll
    static void stopPostgres() {
        if (entityManagerFactory != null) entityManagerFactory.close();
        if (dataSource != null) dataSource.close();
        if (postgres != null && postgres.isRunning()) postgres.stop();
    }

    @Test
    void syntheticNativeProjectionPreservesUtcFractionsAndWindowBoundaries() {
        Instant start = Instant.parse("2026-07-01T00:00:00.123456Z");
        Instant middle = Instant.parse("2026-07-01T00:00:01.987654Z");
        Instant end = Instant.parse("2026-07-01T00:00:02.654321Z");
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            SyntheticCoverageProjectionRepository repository = new JpaRepositoryFactory(entityManager)
                    .getRepository(SyntheticCoverageProjectionRepository.class);
            List<ShadowCoverageCountsProjection> rows = repository.projectWindow(
                    OffsetDateTime.ofInstant(start, ZoneOffset.UTC),
                    OffsetDateTime.ofInstant(middle, ZoneOffset.UTC),
                    OffsetDateTime.ofInstant(end, ZoneOffset.UTC));

            assertEquals(1, rows.size());
            ShadowCoverageCountsProjection row = rows.getFirst();
            assertInstanceOf(Instant.class, row.getOldestEventTime());
            assertInstanceOf(Instant.class, row.getNewestEventTime());
            assertEquals(start, row.getOldestEventTime());
            assertEquals(end, row.getNewestEventTime());
            assertEquals(2L, row.getSimulatedEvents());
            assertEquals(1L, row.getSkippedEvents());
            assertEquals(0L, row.getErrorEvents());
        } finally {
            entityManager.close();
        }
    }

    @Test
    void productionRollingQueryProjectsInstantWithoutTemporalFallback() throws Exception {
        QueryWindow window = externalReadOnly ? existingReadOnlyWindow() : seededWindow();
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        try {
            ShadowCopyOperationEventRepository repository = new JpaRepositoryFactory(entityManager)
                    .getRepository(ShadowCopyOperationEventRepository.class);
            List<ShadowCoverageCountsProjection> rows = repository.findRollingCoverageBatch(
                    List.of(window.allocationId()),
                    OffsetDateTime.ofInstant(window.start(), ZoneOffset.UTC),
                    OffsetDateTime.ofInstant(window.end(), ZoneOffset.UTC),
                    500);

            assertFalse(rows.isEmpty());
            ShadowCoverageCountsProjection row = rows.getFirst();
            assertInstanceOf(Instant.class, row.getOldestEventTime());
            assertInstanceOf(Instant.class, row.getNewestEventTime());
            assertTrue(!row.getOldestEventTime().isBefore(window.start()));
            assertTrue(!row.getNewestEventTime().isAfter(window.end()));
        } finally {
            entityManager.close();
        }
    }

    private static QueryWindow seededWindow() throws Exception {
        Instant start = Instant.parse("2026-07-01T00:00:00.123456Z");
        Instant middle = Instant.parse("2026-07-01T00:00:01.987654Z");
        Instant end = Instant.parse("2026-07-01T00:00:02.654321Z");
        insert(41L, "SIMULATED", start);
        insert(41L, "SKIPPED", middle);
        insert(41L, "SIMULATED", end);
        insert(41L, "SIMULATED", start.minusNanos(1_000));
        insert(41L, "SIMULATED", end.plusNanos(1_000));
        return new QueryWindow(41L, start, end);
    }

    private static QueryWindow existingReadOnlyWindow() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("""
                     select shadow_allocation_id, min(event_time), max(event_time)
                     from futuros_operaciones.shadow_copy_operation_event
                     where shadow_allocation_id is not null
                       and decision in ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR')
                     group by shadow_allocation_id
                     order by count(*) desc
                     limit 1
                     """)) {
            Assumptions.assumeTrue(rows.next(), "PostgreSQL 16 database has no SHADOW coverage rows");
            return new QueryWindow(
                    rows.getLong(1),
                    rows.getObject(2, OffsetDateTime.class).toInstant(),
                    rows.getObject(3, OffsetDateTime.class).toInstant());
        }
    }

    private static void insert(long allocationId, String decision, Instant eventTime) throws Exception {
        try (Connection connection = dataSource.getConnection(); PreparedStatement insert = connection.prepareStatement("""
                insert into futuros_operaciones.shadow_copy_operation_event(
                    id_event, shadow_allocation_id, decision, event_time
                ) values (gen_random_uuid(), ?, ?, ?)
                """)) {
            insert.setLong(1, allocationId);
            insert.setString(2, decision);
            insert.setObject(3, OffsetDateTime.ofInstant(eventTime, ZoneOffset.UTC));
            insert.executeUpdate();
        }
    }

    private record QueryWindow(long allocationId, Instant start, Instant end) {
    }

    private interface SyntheticCoverageProjectionRepository
            extends Repository<ShadowCopyOperationEventEntity, UUID> {

        @Query(value = """
                with source(shadow_allocation_id, decision, event_time) as (
                    values
                        (41::bigint, 'SIMULATED'::varchar, cast(:windowStart as timestamptz)),
                        (41::bigint, 'SKIPPED'::varchar, cast(:middle as timestamptz)),
                        (41::bigint, 'SIMULATED'::varchar, cast(:windowEnd as timestamptz)),
                        (41::bigint, 'ERROR'::varchar, cast(null as timestamptz))
                )
                select shadow_allocation_id as "shadowAllocationId",
                       count(*) filter (where decision = 'SIMULATED') as "simulatedEvents",
                       count(*) filter (where decision = 'RECORDED') as "recordedEvents",
                       count(*) filter (where decision = 'SKIPPED') as "skippedEvents",
                       count(*) filter (where decision = 'ERROR') as "errorEvents",
                       min(event_time) as "oldestEventTime",
                       max(event_time) as "newestEventTime"
                from source
                where event_time >= :windowStart and event_time <= :windowEnd
                group by shadow_allocation_id
                """, nativeQuery = true)
        List<ShadowCoverageCountsProjection> projectWindow(
                @Param("windowStart") OffsetDateTime windowStart,
                @Param("middle") OffsetDateTime middle,
                @Param("windowEnd") OffsetDateTime windowEnd
        );
    }
}
