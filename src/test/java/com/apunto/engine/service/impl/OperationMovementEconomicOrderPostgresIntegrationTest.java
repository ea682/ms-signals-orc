package com.apunto.engine.service.impl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.URI;
import java.sql.Connection;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class OperationMovementEconomicOrderPostgresIntegrationTest {
    private static final OffsetDateTime EVENT_TIME =
            OffsetDateTime.parse("2026-07-23T05:18:53.601Z");
    private static DataSource dataSource;
    private static JdbcTemplate jdbc;

    @BeforeAll
    static void configurePostgres() throws Exception {
        String url = System.getProperty("copy.postgres.test.jdbc-url");
        assumeTrue(url != null && !url.isBlank(),
                "copy.postgres.test.jdbc-url is required");
        URI uri = URI.create(url.substring("jdbc:".length()));
        assumeTrue(InetAddress.getByName(uri.getHost()).isLoopbackAddress(),
                "local PostgreSQL is required");

        DriverManagerDataSource configured = new DriverManagerDataSource(
                url,
                System.getProperty("copy.postgres.test.username", "postgres"),
                System.getProperty("copy.postgres.test.password", "")
        );
        dataSource = configured;
        jdbc = new JdbcTemplate(configured);
        jdbc.execute("CREATE SCHEMA IF NOT EXISTS futuros_operaciones");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS futuros_operaciones.operation_movement_event (
                  movement_key text PRIMARY KEY,
                  position_key text NOT NULL,
                  event_time timestamptz NOT NULL,
                  source_sequence bigint,
                  date_creation timestamptz NOT NULL DEFAULT now()
                )
                """);
        new ResourceDatabasePopulator(new ClassPathResource(
                "db/migration/V202607230001__operation_movement_economic_order.sql"
        )).execute(configured);
    }

    @BeforeEach
    void resetProbeRows() {
        jdbc.update("""
                DELETE FROM futuros_operaciones.operation_movement_event
                WHERE position_key LIKE 'pg-order-test|%'
                """);
    }

    @Test
    void postgresOrdersLegacyNullThenSourceSequenceThenMovementKey() {
        insert("movement-seq-2", 2L, EVENT_TIME.minusSeconds(2));
        insert("movement-legacy-null", null, EVENT_TIME.plusSeconds(5));
        insert("movement-seq-1-z", 1L, EVENT_TIME.plusSeconds(3));
        insert("movement-seq-1-a", 1L, EVENT_TIME.plusSeconds(4));

        List<String> ordered = jdbc.queryForList("""
                SELECT movement_key
                FROM futuros_operaciones.operation_movement_event
                WHERE position_key='pg-order-test|BTCUSDT|LONG'
                ORDER BY event_time,
                         COALESCE(source_sequence, (-9223372036854775807 - 1)),
                         movement_key
                """, String.class);

        assertEquals(List.of(
                "movement-legacy-null",
                "movement-seq-1-a",
                "movement-seq-1-z",
                "movement-seq-2"
        ), ordered);
    }

    @Test
    void advisoryLockIsExclusivePerPositionAndIndependentAcrossPositions() throws Exception {
        String position = "pg-order-test|BTCUSDT|LONG";
        try (Connection owner = dataSource.getConnection();
             Connection contender = dataSource.getConnection()) {
            owner.setAutoCommit(false);
            contender.setAutoCommit(false);
            advisoryLock(owner, position);

            assertFalse(tryAdvisoryLock(contender, position));
            assertTrue(tryAdvisoryLock(
                    contender, "pg-order-test|ETHUSDT|LONG"));

            contender.rollback();
            owner.commit();
        }

        try (Connection afterCommit = dataSource.getConnection()) {
            afterCommit.setAutoCommit(false);
            assertTrue(tryAdvisoryLock(afterCommit, position));
            afterCommit.rollback();
        }
    }

    @Test
    void sixteenWorkersSerializeOnePositionWithoutDeadlockAndLeaveTotalOrder() throws Exception {
        int workers = 16;
        CyclicBarrier start = new CyclicBarrier(workers);
        AtomicInteger activeHolders = new AtomicInteger();
        AtomicInteger maximumHolders = new AtomicInteger();
        var pool = Executors.newFixedThreadPool(workers);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int worker = 0; worker < workers; worker++) {
                int sourceSequence = workers - worker;
                tasks.add(() -> {
                    start.await();
                    try (Connection connection = dataSource.getConnection()) {
                        connection.setAutoCommit(false);
                        try (var timeout = connection.createStatement()) {
                            timeout.execute("SET LOCAL lock_timeout='10s'");
                            timeout.execute("SET LOCAL statement_timeout='12s'");
                        }
                        advisoryLock(
                                connection,
                                "pg-order-test|BTCUSDT|LONG"
                        );
                        int holders = activeHolders.incrementAndGet();
                        maximumHolders.accumulateAndGet(holders, Math::max);
                        try {
                            Thread.sleep(10);
                            try (var insert = connection.prepareStatement("""
                                    INSERT INTO futuros_operaciones.operation_movement_event (
                                      movement_key,position_key,event_time,
                                      source_sequence,date_creation
                                    ) VALUES (?,?,?,?,clock_timestamp())
                                    """)) {
                                insert.setString(1, "worker-" + sourceSequence);
                                insert.setString(
                                        2, "pg-order-test|BTCUSDT|LONG");
                                insert.setObject(3, EVENT_TIME);
                                insert.setLong(4, sourceSequence);
                                insert.executeUpdate();
                            }
                        } finally {
                            activeHolders.decrementAndGet();
                        }
                        connection.commit();
                    }
                    return null;
                });
            }

            var futures = pool.invokeAll(tasks, 15, TimeUnit.SECONDS);
            assertTrue(futures.stream().noneMatch(
                    java.util.concurrent.Future::isCancelled));
            for (var future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertEquals(1, maximumHolders.get());
        assertEquals(
                java.util.stream.LongStream.rangeClosed(1, workers)
                        .boxed()
                        .toList(),
                jdbc.queryForList("""
                        SELECT source_sequence
                        FROM futuros_operaciones.operation_movement_event
                        WHERE position_key='pg-order-test|BTCUSDT|LONG'
                        ORDER BY event_time,source_sequence,movement_key
                        """, Long.class)
        );
        assertTrue(Duration.between(
                jdbc.queryForObject("""
                        SELECT min(date_creation)
                        FROM futuros_operaciones.operation_movement_event
                        WHERE position_key='pg-order-test|BTCUSDT|LONG'
                        """, OffsetDateTime.class),
                jdbc.queryForObject("""
                        SELECT max(date_creation)
                        FROM futuros_operaciones.operation_movement_event
                        WHERE position_key='pg-order-test|BTCUSDT|LONG'
                        """, OffsetDateTime.class)
        ).compareTo(Duration.ofSeconds(15)) < 0);
    }

    private void insert(
            String movementKey,
            Long sourceSequence,
            OffsetDateTime physicalCreation
    ) {
        jdbc.update("""
                INSERT INTO futuros_operaciones.operation_movement_event (
                  movement_key,position_key,event_time,source_sequence,date_creation
                ) VALUES (?,'pg-order-test|BTCUSDT|LONG',?,?,?)
                """, movementKey, EVENT_TIME, sourceSequence, physicalCreation);
    }

    private static void advisoryLock(
            Connection connection,
            String positionKey
    ) throws Exception {
        try (var statement = connection.prepareStatement("""
                SELECT pg_advisory_xact_lock(hashtextextended(?, 0))
                """)) {
            statement.setString(1, positionKey);
            statement.execute();
        }
    }

    private static boolean tryAdvisoryLock(
            Connection connection,
            String positionKey
    ) throws Exception {
        try (var statement = connection.prepareStatement("""
                SELECT pg_try_advisory_xact_lock(hashtextextended(?, 0))
                """)) {
            statement.setString(1, positionKey);
            try (var result = statement.executeQuery()) {
                result.next();
                return result.getBoolean(1);
            }
        }
    }
}
