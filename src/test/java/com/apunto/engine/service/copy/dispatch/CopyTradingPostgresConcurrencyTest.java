package com.apunto.engine.service.copy.dispatch;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyTradingPostgresConcurrencyTest {

    private static PostgreSQLContainer<?> postgres;

    @BeforeAll
    static void createSchema() throws SQLException {
        if (System.getProperty("copy.postgres.test.jdbc-url") == null) {
            postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("copy_trading_test")
                    .withUsername("copy_test")
                    .withPassword("copy_test");
            try {
                postgres.start();
            } catch (RuntimeException unavailable) {
                Assumptions.assumeTrue(false,
                        "Docker unavailable and no copy.postgres.test.jdbc-url was provided");
            }
        }
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            try (ResultSet database = statement.executeQuery("select current_database()")) {
                assertTrue(database.next());
                if (!"copy_trading_test".equals(database.getString(1))) {
                    throw new IllegalStateException(
                            "Copy PostgreSQL concurrency tests require the dedicated copy_trading_test database");
                }
            }
            statement.execute("""
                    create table if not exists copy_dispatch_intent (
                        id uuid primary key,
                        idempotency_key text not null unique,
                        id_user text not null,
                        user_copy_allocation_id bigint not null,
                        wallet_id text not null,
                        execution_mode text not null check (execution_mode in ('MICRO_LIVE', 'LIVE')),
                        status text not null check (status in (
                            'CREATED', 'CLAIMED', 'DISPATCHING', 'ACKNOWLEDGED', 'NEW',
                            'PARTIALLY_FILLED', 'FILLED', 'RECONCILING', 'PERSISTENCE_PENDING',
                            'PERSISTED', 'REJECTED', 'FAILED_FINAL', 'CANCELLED', 'MANUAL_REVIEW')),
                        reservation_status text not null check (reservation_status in (
                            'UNRESERVED', 'PENDING', 'CONFIRMED', 'RELEASED')),
                        requested_margin_usd numeric(38, 12) not null default 0,
                        reserved_position_count smallint not null default 0,
                        request_hash text not null,
                        client_order_id text not null,
                        binance_order_id bigint,
                        copy_operation_id uuid,
                        copy_operation_event_id uuid,
                        average_price_status text not null default 'NOT_AVAILABLE',
                        attempts integer not null default 0,
                        next_reconciliation_at timestamptz,
                        claimed_by text,
                        created_at timestamptz not null default clock_timestamp(),
                        updated_at timestamptz not null default clock_timestamp()
                    )
                    """);
            statement.execute("alter table copy_dispatch_intent add column if not exists wallet_id text not null default 'wallet-a'");
            statement.execute("""
                    create index if not exists ix_copy_dispatch_intent_reconciliation
                    on copy_dispatch_intent(status, next_reconciliation_at, updated_at)
                    """);
            statement.execute("""
                    create index if not exists ix_copy_dispatch_intent_wallet_budget
                    on copy_dispatch_intent(id_user, lower(wallet_id), execution_mode, reservation_status)
                    """);
            statement.execute("""
                    create table if not exists copy_operation (
                        id_operation uuid primary key,
                        dispatch_intent_id uuid unique,
                        id_user text not null,
                        user_copy_allocation_id bigint not null,
                        id_wallet_origin text not null,
                        execution_mode text not null,
                        size_usd numeric(38, 12) not null,
                        leverage numeric(18, 8) not null,
                        is_active boolean not null,
                        is_shadow boolean not null
                    )
                    """);
            statement.execute("alter table copy_operation add column if not exists id_wallet_origin text not null default 'wallet-a'");
            statement.execute("""
                    create table if not exists copy_operation_event (
                        id_event uuid primary key,
                        dispatch_intent_id uuid,
                        event_type text not null,
                        qty_executed numeric(38, 18),
                        resulting_qty numeric(38, 18),
                        client_order_id text,
                        date_creation timestamptz not null default clock_timestamp()
                    )
                    """);
            statement.execute("""
                    create unique index if not exists ux_copy_operation_event_dispatch_progress
                    on copy_operation_event(
                        dispatch_intent_id, event_type,
                        coalesce(qty_executed, 0), coalesce(resulting_qty, 0))
                    where dispatch_intent_id is not null
                    """);
            statement.execute("""
                    create unique index if not exists ux_copy_operation_event_legacy_client_order_id
                    on copy_operation_event(client_order_id)
                    where client_order_id is not null and dispatch_intent_id is null
                    """);
            statement.execute("""
                    create table if not exists shadow_event_dead_letter (
                        idempotency_key text primary key,
                        payload_json jsonb not null,
                        error_code text not null,
                        attempt_count integer not null,
                        status text not null,
                        first_failed_at timestamptz not null default clock_timestamp(),
                        last_failed_at timestamptz not null default clock_timestamp()
                    )
                    """);
        }
    }

    @AfterAll
    static void stopContainer() {
        if (postgres != null && postgres.isRunning()) {
            postgres.stop();
        }
    }

    @BeforeEach
    void clean() throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute("truncate table copy_operation_event, copy_operation, copy_dispatch_intent, shadow_event_dead_letter");
        }
    }

    @Test
    void sortedProfileAdvisoryLocksAvoidDeadlockAcrossLogicalReplicas() throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<String> first = pool.submit(() -> lockProfilesInCanonicalOrder(start, List.of("profile-b", "profile-a")));
            Future<String> second = pool.submit(() -> lockProfilesInCanonicalOrder(start, List.of("profile-a", "profile-b")));
            start.countDown();
            assertEquals("OK", first.get(10, TimeUnit.SECONDS));
            assertEquals("OK", second.get(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void independentProfileKeysDoNotBlockEachOther() throws SQLException {
        try (Connection first = connection(); Connection second = connection()) {
            first.setAutoCommit(false);
            second.setAutoCommit(false);
            advisoryLock(first, "wallet-a|MOVEMENT_ALL|ALL|ALL");
            try (Statement statement = second.createStatement()) {
                statement.execute("set local lock_timeout='100ms'");
            }
            advisoryLock(second, "wallet-b|MOVEMENT_ALL|ALL|ALL");
            second.rollback();
            first.rollback();
        }
    }

    @Test
    void exhaustedShadowEventUpsertRemainsSingleRecoverableRecord() throws SQLException {
        upsertShadowDeadLetter("shadow-idem-1", 3);
        upsertShadowDeadLetter("shadow-idem-1", 2);

        assertEquals(1L, scalarLong("select count(*) from shadow_event_dead_letter"));
        assertEquals(3L, scalarLong("select attempt_count from shadow_event_dead_letter where idempotency_key='shadow-idem-1'"));
        assertEquals("RECOVERABLE", scalarString("select status from shadow_event_dead_letter where idempotency_key='shadow-idem-1'"));
    }

    @Test
    void twoWorkersCannotClaimTheSameIntentAndSkipLockedDoesNotWait() throws Exception {
        UUID intentId = insertIntent("claim-once", "CREATED", "UNRESERVED", BigDecimal.ZERO);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch firstLocked = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        try {
            Future<Integer> first = pool.submit(() -> claimAndHold(intentId, firstLocked, releaseFirst));
            assertTrue(firstLocked.await(5, TimeUnit.SECONDS));
            Future<Integer> second = pool.submit(() -> claimOnce(intentId));
            assertEquals(0, second.get(5, TimeUnit.SECONDS));
            releaseFirst.countDown();
            assertEquals(1, first.get(5, TimeUnit.SECONDS));
        } finally {
            releaseFirst.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void twoReplicasSerializeWalletBudgetAcrossDifferentStrategies() throws Exception {
        insertActiveOperation(new BigDecimal("80"));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Boolean> first = pool.submit(() -> reserve(start, "reserve-a", new BigDecimal("20"), 505L));
            Future<Boolean> second = pool.submit(() -> reserve(start, "reserve-b", new BigDecimal("20"), 506L));
            start.countDown();
            List<Boolean> outcomes = List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
            assertEquals(1, outcomes.stream().filter(Boolean::booleanValue).count());
            assertEquals(0, totalCommittedAndReserved().compareTo(new BigDecimal("100")));
            assertEquals(1L, scalarLong("select count(*) from copy_dispatch_intent where reservation_status='PENDING'"));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void identicalPartialsPersistOnlyOneProgressRow() throws Exception {
        UUID intentId = insertIntent("partial-same", "PARTIALLY_FILLED", "PENDING", BigDecimal.TEN);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<Integer> first = pool.submit(() -> insertProgress(start, intentId, "1.25", "1.25"));
            Future<Integer> second = pool.submit(() -> insertProgress(start, intentId, "1.25", "1.25"));
            start.countDown();
            assertEquals(1, first.get(5, TimeUnit.SECONDS) + second.get(5, TimeUnit.SECONDS));
            assertEquals(1L, scalarLong("select count(*) from copy_operation_event"));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void distinctCumulativePartialsPersistIndependentProgressRows() throws Exception {
        UUID intentId = insertIntent("partial-distinct", "PARTIALLY_FILLED", "PENDING", BigDecimal.TEN);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Integer> first = pool.submit(() -> insertProgress(start, intentId, "1.25", "1.25"));
            Future<Integer> second = pool.submit(() -> insertProgress(start, intentId, "2.00", "2.00"));
            start.countDown();
            assertEquals(2, first.get(5, TimeUnit.SECONDS) + second.get(5, TimeUnit.SECONDS));
            assertEquals(2L, scalarLong("select count(*) from copy_operation_event"));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void replayAfterRestartReusesDurableIntentAndPayloadHash() throws SQLException {
        UUID original = insertIntent("restart-replay", "DISPATCHING", "PENDING", BigDecimal.TEN);
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement("""
                insert into copy_dispatch_intent(
                    id,idempotency_key,id_user,user_copy_allocation_id,wallet_id,execution_mode,status,
                    reservation_status,requested_margin_usd,request_hash,client_order_id)
                values (?,?,?,?,?,?,'CREATED','UNRESERVED',?,?,?)
                on conflict(idempotency_key) do nothing
                """)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setString(2, "restart-replay");
            statement.setString(3, "user-1");
            statement.setLong(4, 505L);
            statement.setString(5, "wallet-a");
            statement.setString(6, "MICRO_LIVE");
            statement.setBigDecimal(7, BigDecimal.TEN);
            statement.setString(8, "hash-restart-replay");
            statement.setString(9, "client-restart-replay");
            assertEquals(0, statement.executeUpdate());
        }
        assertEquals(original, scalarUuid("select id from copy_dispatch_intent where idempotency_key='restart-replay'"));
        assertEquals("hash-restart-replay", scalarString("select request_hash from copy_dispatch_intent where idempotency_key='restart-replay'"));
    }

    @Test
    void lateAcknowledgementCannotLeaveTerminalManualReview() throws SQLException {
        UUID intentId = insertIntent("late-ack", "MANUAL_REVIEW", "PENDING", BigDecimal.TEN);
        String status = scalarString("select status from copy_dispatch_intent where id='" + intentId + "'");
        assertThrows(IllegalStateException.class,
                () -> CopyDispatchStatePolicy.requireTransition(status, "ACKNOWLEDGED"));
        assertEquals("MANUAL_REVIEW", scalarString("select status from copy_dispatch_intent where id='" + intentId + "'"));
    }

    @Test
    void manualReviewIsAcceptedAndKeepsAmbiguousReservationPending() throws SQLException {
        UUID intentId = insertIntent("manual-review", "MANUAL_REVIEW", "PENDING", BigDecimal.TEN);
        assertEquals("MANUAL_REVIEW|PENDING", scalarString("""
                select status || '|' || reservation_status
                from copy_dispatch_intent
                where id='%s'
                """.formatted(intentId)));
        assertEquals(0L, scalarLong("""
                select count(*) from copy_dispatch_intent
                where status='MANUAL_REVIEW' and reservation_status='RELEASED'
                """));
    }

    @Test
    void controlledDeadlockAbortsExactlyOneTransaction() throws Exception {
        UUID firstId = insertIntent("deadlock-a", "CREATED", "UNRESERVED", BigDecimal.ZERO);
        UUID secondId = insertIntent("deadlock-b", "CREATED", "UNRESERVED", BigDecimal.ZERO);
        CountDownLatch firstLocksHeld = new CountDownLatch(2);
        CountDownLatch cross = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<String> first = pool.submit(() -> deadlockSide(firstId, secondId, firstLocksHeld, cross));
            Future<String> second = pool.submit(() -> deadlockSide(secondId, firstId, firstLocksHeld, cross));
            assertTrue(firstLocksHeld.await(5, TimeUnit.SECONDS));
            cross.countDown();
            Set<String> outcomes = Set.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS));
            assertTrue(outcomes.contains("40P01"), "one transaction must be selected as the deadlock victim");
            assertTrue(outcomes.contains("OK"), "the surviving transaction must commit");
        } finally {
            cross.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void statementTimeoutCancelsBoundedLocalProbe() throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("set local statement_timeout='100ms'");
            SQLException error = assertThrows(SQLException.class,
                    () -> statement.executeQuery("select pg_sleep(1)"));
            assertEquals("57014", error.getSQLState());
            connection.rollback();
        }
    }

    @Test
    void lockTimeoutFailsInsteadOfWaitingIndefinitely() throws SQLException {
        UUID intentId = insertIntent("lock-timeout", "CREATED", "UNRESERVED", BigDecimal.ZERO);
        try (Connection holder = connection(); Connection waiter = connection()) {
            holder.setAutoCommit(false);
            waiter.setAutoCommit(false);
            lockIntent(holder, intentId);
            try (Statement statement = waiter.createStatement()) {
                statement.execute("set local lock_timeout='100ms'");
            }
            SQLException error = assertThrows(SQLException.class, () -> lockIntent(waiter, intentId));
            assertEquals("55P03", error.getSQLState());
            waiter.rollback();
            holder.rollback();
        }
    }

    @Test
    void hikariPoolExhaustionHonorsConnectionTimeout() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl());
        config.setUsername(username());
        config.setPassword(password());
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(250);
        try (HikariDataSource dataSource = new HikariDataSource(config);
             Connection held = dataSource.getConnection()) {
            long started = System.nanoTime();
            assertThrows(SQLException.class, dataSource::getConnection);
            assertTrue(Duration.ofNanos(System.nanoTime() - started).toMillis() >= 200);
        }
    }

    @Test
    void ledgerInsertDisappearsAfterTransactionRollback() throws SQLException {
        UUID intentId = insertIntent("ledger-rollback", "FILLED", "PENDING", BigDecimal.TEN);
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            insertProgress(connection, intentId, "1", "1");
            connection.rollback();
        }
        assertEquals(0L, scalarLong("select count(*) from copy_operation_event"));
    }

    @Test
    void committedReservationSurvivesProcessLossAndStillConsumesBudget() throws SQLException {
        insertIntent("crash-after-reservation", "DISPATCHING", "PENDING", new BigDecimal("20"));
        assertEquals(0, scalarDecimal("""
                select coalesce(sum(requested_margin_usd),0)
                from copy_dispatch_intent
                where reservation_status='PENDING'
                """).compareTo(new BigDecimal("20")));
        assertEquals(0L, scalarLong("select count(*) from copy_operation"));
    }

    @Test
    void recoveredPersistenceIsIdempotentForOperationAndLedger() throws SQLException {
        UUID intentId = insertIntent("persistence-idempotent", "FILLED", "PENDING", BigDecimal.TEN);
        UUID operationId = UUID.randomUUID();
        persistOperationOnce(operationId, intentId);
        persistOperationOnce(UUID.randomUUID(), intentId);
        try (Connection first = connection(); Connection replay = connection()) {
            insertProgress(first, intentId, "1", "1");
            insertProgress(replay, intentId, "1", "1");
        }
        assertEquals(1L, scalarLong("select count(*) from copy_operation"));
        assertEquals(1L, scalarLong("select count(*) from copy_operation_event"));
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), username(), password());
    }

    private String lockProfilesInCanonicalOrder(CountDownLatch start, List<String> keys) {
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            start.await(5, TimeUnit.SECONDS);
            for (String key : keys.stream().sorted().toList()) {
                advisoryLock(connection, key);
            }
            connection.commit();
            return "OK";
        } catch (SQLException sql) {
            return sql.getSQLState();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return "INTERRUPTED";
        }
    }

    private static void advisoryLock(Connection connection, String key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select pg_advisory_xact_lock(hashtextextended(cast(? as text), 0))")) {
            statement.setString(1, "shadow-profile:" + key);
            statement.execute();
        }
    }

    private void upsertShadowDeadLetter(String idempotencyKey, int attempts) throws SQLException {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement("""
                insert into shadow_event_dead_letter(idempotency_key,payload_json,error_code,attempt_count,status)
                values (?, '{}'::jsonb, 'SQLSTATE_40P01', ?, 'RECOVERABLE')
                on conflict(idempotency_key) do update set
                    attempt_count=greatest(shadow_event_dead_letter.attempt_count, excluded.attempt_count),
                    status='RECOVERABLE',
                    last_failed_at=clock_timestamp()
                """)) {
            statement.setString(1, idempotencyKey);
            statement.setInt(2, attempts);
            statement.executeUpdate();
        }
    }

    private static String jdbcUrl() {
        String configured = System.getProperty("copy.postgres.test.jdbc-url");
        return configured == null ? postgres.getJdbcUrl() : configured;
    }

    private static String username() {
        String configured = System.getProperty("copy.postgres.test.username");
        if (configured != null) return configured;
        return System.getProperty("copy.postgres.test.jdbc-url") == null ? postgres.getUsername() : "copy_test";
    }

    private static String password() {
        String configured = System.getProperty("copy.postgres.test.password");
        if (configured != null) return configured;
        return System.getProperty("copy.postgres.test.jdbc-url") == null ? postgres.getPassword() : "";
    }

    private UUID insertIntent(String key, String status, String reservation, BigDecimal margin) throws SQLException {
        UUID id = UUID.randomUUID();
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement("""
                insert into copy_dispatch_intent(
                    id,idempotency_key,id_user,user_copy_allocation_id,wallet_id,execution_mode,status,
                    reservation_status,requested_margin_usd,request_hash,client_order_id)
                values (?,?,?,?,?,?,?,?,?,?,?)
                """)) {
            statement.setObject(1, id);
            statement.setString(2, key);
            statement.setString(3, "user-1");
            statement.setLong(4, 505L);
            statement.setString(5, "wallet-a");
            statement.setString(6, "MICRO_LIVE");
            statement.setString(7, status);
            statement.setString(8, reservation);
            statement.setBigDecimal(9, margin);
            statement.setString(10, "hash-" + key);
            statement.setString(11, "client-" + key);
            statement.executeUpdate();
        }
        return id;
    }

    private void insertActiveOperation(BigDecimal margin) throws SQLException {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement("""
                insert into copy_operation(
                    id_operation,id_user,user_copy_allocation_id,id_wallet_origin,execution_mode,
                    size_usd,leverage,is_active,is_shadow)
                values (?,?,?,?,'MICRO_LIVE',?,1,true,false)
                """)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setString(2, "user-1");
            statement.setLong(3, 505L);
            statement.setString(4, "wallet-a");
            statement.setBigDecimal(5, margin);
            statement.executeUpdate();
        }
    }

    private boolean reserve(CountDownLatch start, String key, BigDecimal requested, long allocationId) throws Exception {
        assertTrue(start.await(5, TimeUnit.SECONDS));
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement lock = connection.prepareStatement(
                    "select pg_advisory_xact_lock(hashtextextended(?,0))")) {
                lock.setString(1, "user-1|wallet-a|MICRO_LIVE");
                lock.executeQuery().close();
            }
            BigDecimal current;
            try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery("""
                    select
                      (select coalesce(sum(size_usd/nullif(leverage,0)),0) from copy_operation
                       where id_user='user-1' and lower(id_wallet_origin)=lower('wallet-a')
                         and execution_mode='MICRO_LIVE' and is_active=true and is_shadow=false)
                      +
                      (select coalesce(sum(requested_margin_usd),0) from copy_dispatch_intent
                       where id_user='user-1' and lower(wallet_id)=lower('wallet-a')
                         and execution_mode='MICRO_LIVE' and reservation_status='PENDING')
                    """)) {
                result.next();
                current = result.getBigDecimal(1);
            }
            if (current.add(requested).compareTo(new BigDecimal("100")) > 0) {
                connection.rollback();
                return false;
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    insert into copy_dispatch_intent(
                        id,idempotency_key,id_user,user_copy_allocation_id,wallet_id,execution_mode,status,
                        reservation_status,requested_margin_usd,request_hash,client_order_id)
                    values (?,?, 'user-1',?,?,'MICRO_LIVE','DISPATCHING','PENDING',?,?,?)
                    """)) {
                insert.setObject(1, UUID.randomUUID());
                insert.setString(2, key);
                insert.setLong(3, allocationId);
                insert.setString(4, "wallet-a");
                insert.setBigDecimal(5, requested);
                insert.setString(6, "hash-" + key);
                insert.setString(7, "client-" + key);
                insert.executeUpdate();
            }
            connection.commit();
            return true;
        }
    }

    private BigDecimal totalCommittedAndReserved() throws SQLException {
        return scalarDecimal("""
                select
                  (select coalesce(sum(size_usd/nullif(leverage,0)),0) from copy_operation where is_active=true)
                  +
                  (select coalesce(sum(requested_margin_usd),0) from copy_dispatch_intent
                   where reservation_status='PENDING')
                """);
    }

    private int claimAndHold(UUID id, CountDownLatch locked, CountDownLatch release) throws Exception {
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            int rows = claim(connection, id);
            locked.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            connection.commit();
            return rows;
        }
    }

    private int claimOnce(UUID id) throws SQLException {
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            int rows = claim(connection, id);
            connection.commit();
            return rows;
        }
    }

    private int claim(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                select id from copy_dispatch_intent
                where id=? and status='CREATED'
                for update skip locked
                """)) {
            statement.setObject(1, id);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? 1 : 0;
            }
        }
    }

    private int insertProgress(CountDownLatch start, UUID intentId, String qty, String resulting) throws Exception {
        assertTrue(start.await(5, TimeUnit.SECONDS));
        try (Connection connection = connection()) {
            return insertProgress(connection, intentId, qty, resulting);
        }
    }

    private int insertProgress(Connection connection, UUID intentId, String qty, String resulting) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
                insert into copy_operation_event(
                    id_event,dispatch_intent_id,event_type,qty_executed,resulting_qty,client_order_id)
                values (?,?,'OPEN',?,?,?)
                on conflict do nothing
                """)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setObject(2, intentId);
            statement.setBigDecimal(3, new BigDecimal(qty));
            statement.setBigDecimal(4, new BigDecimal(resulting));
            statement.setString(5, "progress-" + intentId + '-' + qty);
            return statement.executeUpdate();
        }
    }

    private String deadlockSide(UUID first, UUID second, CountDownLatch locks, CountDownLatch cross) throws Exception {
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("set local deadlock_timeout='100ms'");
                statement.execute("set local statement_timeout='5s'");
            }
            incrementAttempts(connection, first);
            locks.countDown();
            assertTrue(cross.await(5, TimeUnit.SECONDS));
            try {
                incrementAttempts(connection, second);
                connection.commit();
                return "OK";
            } catch (SQLException ex) {
                connection.rollback();
                return ex.getSQLState();
            }
        }
    }

    private void incrementAttempts(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "update copy_dispatch_intent set attempts=attempts+1 where id=?")) {
            statement.setObject(1, id);
            statement.executeUpdate();
        }
    }

    private void lockIntent(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select id from copy_dispatch_intent where id=? for update")) {
            statement.setObject(1, id);
            try (ResultSet ignored = statement.executeQuery()) {
                assertTrue(ignored.next());
            }
        }
    }

    private void persistOperationOnce(UUID operationId, UUID intentId) throws SQLException {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement("""
                insert into copy_operation(
                    id_operation,dispatch_intent_id,id_user,user_copy_allocation_id,id_wallet_origin,
                    execution_mode,size_usd,leverage,is_active,is_shadow)
                values (?,?, 'user-1',505,'wallet-a','MICRO_LIVE',10,1,true,false)
                on conflict(dispatch_intent_id) do nothing
                """)) {
            statement.setObject(1, operationId);
            statement.setObject(2, intentId);
            statement.executeUpdate();
        }
    }

    private long scalarLong(String sql) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private BigDecimal scalarDecimal(String sql) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getBigDecimal(1);
        }
    }

    private String scalarString(String sql) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getString(1);
        }
    }

    private UUID scalarUuid(String sql) throws SQLException {
        try (Connection connection = connection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getObject(1, UUID.class);
        }
    }
}
