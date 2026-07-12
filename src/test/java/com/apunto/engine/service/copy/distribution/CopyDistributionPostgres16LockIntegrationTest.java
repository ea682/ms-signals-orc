package com.apunto.engine.service.copy.distribution;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDistributionPostgres16LockIntegrationTest {

    private static PostgreSQLContainer<?> postgres;

    @BeforeAll
    static void verifyPostgres16() throws Exception {
        if (System.getProperty("copy.postgres.test.jdbc-url") == null) {
            postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("copy_trading_test")
                    .withUsername("copy_test")
                    .withPassword("copy_test");
            try {
                postgres.start();
            } catch (RuntimeException unavailable) {
                Assumptions.assumeTrue(false,
                        "Docker unavailable; configure copy.postgres.test.jdbc-url for PostgreSQL 16 read-only lock verification");
            }
        }
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet version = statement.executeQuery("show server_version_num")) {
            assertTrue(version.next());
            int versionNumber = Integer.parseInt(version.getString(1));
            assertTrue(versionNumber >= 160000 && versionNumber < 170000);
        }
    }

    @AfterAll
    static void stopContainer() {
        if (postgres != null && postgres.isRunning()) postgres.stop();
    }

    @Test
    void sameProfileKeySerializesLogicalReplicas() throws Exception {
        assertSameKeyContends("shadow-profile:wallet-a|MOVEMENT_ALL|all|ALL");
    }

    @Test
    void shadowAndDistributionUseTheSameCanonicalProfileLock() throws Exception {
        assertSameKeyContends("shadow-profile:wallet-a|SHORT_ONLY|direction|SHORT");
    }

    @Test
    void promotionAndDistributionUseTheSameCanonicalProfileLock() throws Exception {
        assertSameKeyContends("shadow-profile:wallet-a|SYMBOL_SPECIALIST|symbol|HYPEUSDT");
    }

    @Test
    void distinctProfileKeysAndSlowWalletDoNotBlockEachOther() throws Exception {
        try (Connection slow = transaction(); Connection independent = transaction()) {
            advisoryLock(slow, "shadow-profile:wallet-slow|MOVEMENT_ALL|all|ALL");
            setLocalTimeout(independent, "500ms");
            long startedNs = System.nanoTime();
            advisoryLock(independent, "shadow-profile:wallet-fast|MOVEMENT_ALL|all|ALL");
            long elapsedMs = Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
            assertTrue(elapsedMs < 400L, "independent profile waited " + elapsedMs + "ms");
            independent.rollback();
            slow.rollback();
        }
    }

    @Test
    void rollbackReleasesOnlyTheFailedUnitLock() throws Exception {
        String key = "shadow-profile:wallet-a|LONG_ONLY|direction|LONG";
        try (Connection failedUnit = transaction(); Connection nextUnit = transaction()) {
            advisoryLock(failedUnit, key);
            failedUnit.rollback();
            setLocalTimeout(nextUnit, "500ms");
            advisoryLock(nextUnit, key);
            nextUnit.rollback();
        }
    }

    @Test
    void controlledAdvisoryLockDeadlockProducesSqlState40P01() throws Exception {
        try (Connection first = transaction(); Connection second = transaction()) {
            advisoryLock(first, "shadow-profile:deadlock-a");
            advisoryLock(second, "shadow-profile:deadlock-b");
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<String> firstResult = pool.submit(() -> crossLock(first, "shadow-profile:deadlock-b", start));
                Future<String> secondResult = pool.submit(() -> crossLock(second, "shadow-profile:deadlock-a", start));
                start.countDown();
                Set<String> outcomes = Set.of(
                        firstResult.get(10, TimeUnit.SECONDS),
                        secondResult.get(10, TimeUnit.SECONDS));
                assertTrue(outcomes.contains("40P01"));
                assertTrue(outcomes.contains("OK"));
            } finally {
                pool.shutdownNow();
                rollbackQuietly(first);
                rollbackQuietly(second);
            }
        }
    }

    private static void assertSameKeyContends(String key) throws Exception {
        try (Connection holder = transaction(); Connection waiter = transaction()) {
            advisoryLock(holder, key);
            setLocalTimeout(waiter, "100ms");
            SQLException timeout = assertThrows(SQLException.class, () -> advisoryLock(waiter, key));
            assertEquals("55P03", timeout.getSQLState());
            waiter.rollback();
            holder.rollback();
        }
        try (Connection afterRollback = transaction()) {
            setLocalTimeout(afterRollback, "500ms");
            advisoryLock(afterRollback, key);
            afterRollback.rollback();
        }
    }

    private static String crossLock(Connection connection, String key, CountDownLatch start) {
        try {
            start.await(5, TimeUnit.SECONDS);
            advisoryLock(connection, key);
            connection.commit();
            return "OK";
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            return ex.getSQLState();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            rollbackQuietly(connection);
            return "INTERRUPTED";
        }
    }

    private static Connection transaction() throws SQLException {
        Connection connection = connection();
        connection.setReadOnly(true);
        connection.setAutoCommit(false);
        return connection;
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), username(), password());
    }

    private static void advisoryLock(Connection connection, String key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select pg_advisory_xact_lock(hashtextextended(cast(? as text), 0))")) {
            statement.setString(1, key);
            statement.executeQuery().close();
        }
    }

    private static void setLocalTimeout(Connection connection, String timeout) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("set local lock_timeout='" + timeout + "'");
        }
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // Best effort cleanup in a test-only connection.
        }
    }

    private static String jdbcUrl() {
        String configured = System.getProperty("copy.postgres.test.jdbc-url");
        return configured == null ? postgres.getJdbcUrl() : configured;
    }

    private static String username() {
        String configured = System.getProperty("copy.postgres.test.username");
        return configured == null ? postgres.getUsername() : configured;
    }

    private static String password() {
        String configured = System.getProperty("copy.postgres.test.password");
        return configured == null ? postgres.getPassword() : configured;
    }
}
