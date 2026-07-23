package com.apunto.engine.service.copy.account;

import com.apunto.engine.testsupport.ProductionBaselinePostgres;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ExecutionAccountCapacityPostgres16IntegrationTest {

    @Test
    void microCapacityIsAtomicAndReleaseRequiresFlatAndAccountCannotBeRebound() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("copy_account_capacity_test")
                .withUsername("copy_test")
                .withPassword("copy_test")) {
            postgres.start();
            ProductionBaselinePostgres.restoreAndMigrate(postgres);

            UUID userId = UUID.randomUUID();
            UUID accountId = UUID.randomUUID();
            seedUserAndAccount(postgres, userId, accountId, "MICRO_LIVE");

            seedCapacity(postgres, accountId, "1000", 10, 0);

            CountDownLatch ready = new CountDownLatch(11);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService workers = Executors.newFixedThreadPool(11);
            List<Future<String>> outcomes = new ArrayList<>();
            try {
                for (int index = 0; index < 11; index++) {
                    int slot = index;
                    outcomes.add(workers.submit(() -> reserve(postgres, userId, accountId, slot, ready, start)));
                }
                ready.await();
                start.countDown();

                List<String> results = new ArrayList<>();
                for (Future<String> outcome : outcomes) {
                    results.add(outcome.get());
                }
                assertEquals(10, results.stream().filter("RESERVED"::equals).count(), results::toString);
                assertEquals(1, results.stream().filter("MICRO_LIVE_CAPACITY_EXHAUSTED"::equals).count(),
                        results::toString);
            } finally {
                workers.shutdownNow();
            }

            try (Connection connection = connection(postgres); Statement statement = connection.createStatement()) {
                long allocationId = scalar(statement, """
                        select min(id) from futuros_operaciones.user_copy_allocation
                        where exchange_account_id = '%s'
                        """.formatted(accountId));
                statement.executeUpdate("""
                        insert into futuros_operaciones.copy_operation (
                            id_orden, id_order_origin, id_wallet_origin, parsymbol, type_operation,
                            leverage, size_usd, size_par, price_entry, id_user,
                            user_copy_allocation_id, execution_mode, is_shadow, is_active
                        ) values (
                            'order-capacity', 'origin-capacity', 'wallet-capacity', 'BTCUSDT', 'BUY',
                            5, 50, 0.001, 50000, '%s', %d, 'MICRO_LIVE', false, true
                        )
                        """.formatted(userId, allocationId));

                assertSqlReason("MICRO_LIVE_RELEASE_REQUIRES_FLAT", () -> statement.executeUpdate("""
                        update futuros_operaciones.user_copy_allocation
                        set status = 'closed', is_active = false, ends_at = now()
                        where id = %d
                        """.formatted(allocationId)));

                statement.executeUpdate("""
                        update futuros_operaciones.copy_operation set is_active = false
                        where user_copy_allocation_id = %d
                        """.formatted(allocationId));
                assertEquals(1, statement.executeUpdate("""
                        update futuros_operaciones.user_copy_allocation
                        set status = 'closed', is_active = false, ends_at = now()
                        where id = %d
                        """.formatted(allocationId)));

                UUID otherUserId = UUID.randomUUID();
                UUID otherAccountId = UUID.randomUUID();
                seedUserAndAccount(postgres, otherUserId, otherAccountId, "MICRO_LIVE");
                assertSqlReason("EXECUTION_ACCOUNT_REBIND_FORBIDDEN", () -> statement.executeUpdate("""
                        update futuros_operaciones.user_copy_allocation
                        set exchange_account_id = '%s'
                        where id = %d
                        """.formatted(otherAccountId, allocationId)));

                UUID reservedUserId = UUID.randomUUID();
                UUID reservedAccountId = UUID.randomUUID();
                seedUserAndAccount(postgres, reservedUserId, reservedAccountId, "MICRO_LIVE");
                seedCapacity(postgres, reservedAccountId, "500", 5, 1);
                for (int slot = 0; slot < 4; slot++) {
                    assertEquals("RESERVED", reserve(postgres, reservedUserId, reservedAccountId,
                            100 + slot, null, null));
                }
                assertSqlReason("MICRO_LIVE_CAPACITY_EXHAUSTED", () -> statement.executeUpdate(
                        allocationInsert(reservedUserId, reservedAccountId, 104, "SHADOW_PROMOTION")));
                assertEquals(1, statement.executeUpdate(allocationInsert(
                        reservedUserId, reservedAccountId, 105, "MICRO_LIVE_RECERTIFICATION_ACTIVE")));

                UUID rollbackUserId = UUID.randomUUID();
                UUID rollbackAccountId = UUID.randomUUID();
                seedUserAndAccount(postgres, rollbackUserId, rollbackAccountId, "MICRO_LIVE");
                seedCapacity(postgres, rollbackAccountId, "100", 1, 0);
                connection.setAutoCommit(false);
                statement.executeUpdate(allocationInsert(rollbackUserId, rollbackAccountId, 200,
                        "SHADOW_PROMOTION"));
                connection.rollback();
                connection.setAutoCommit(true);
                assertEquals(1, statement.executeUpdate(allocationInsert(
                        rollbackUserId, rollbackAccountId, 201, "SHADOW_PROMOTION")));

                UUID priorityUserId = UUID.randomUUID();
                UUID priorityAccountId = UUID.randomUUID();
                UUID priorityCertificationId = UUID.randomUUID();
                seedUserAndAccount(postgres, priorityUserId, priorityAccountId, "MICRO_LIVE");
                seedCapacity(postgres, priorityAccountId, "200", 2, 0);
                seedCertification(postgres, priorityCertificationId, "priority-wallet");
                assertEquals("RESERVED", reserve(postgres, priorityUserId, priorityAccountId,
                        300, null, null));
                statement.executeUpdate("""
                        insert into futuros_operaciones.micro_live_recertification_request (
                            certification_id, wallet_id, strategy_code, strategy_version,
                            user_id, execution_account_id, priority, status, reason_code,
                            idempotency_key
                        ) values (
                            '%1$s', 'priority-wallet', 'MOVEMENT_ALL', 'v1', '%2$s', '%3$s',
                            100, 'PENDING_CAPACITY', 'MICRO_LIVE_RECERTIFICATION_PENDING_CAPACITY',
                            'priority-%1$s'
                        )
                        """.formatted(priorityCertificationId, priorityUserId, priorityAccountId));
                assertSqlReason("MICRO_LIVE_RECERTIFICATION_PENDING_PRIORITY",
                        () -> statement.executeUpdate(allocationInsert(
                                priorityUserId, priorityAccountId, 301, "SHADOW_PROMOTION")));
                assertEquals(1, statement.executeUpdate(allocationInsert(
                        priorityUserId, priorityAccountId, 302, "MICRO_LIVE_RECERTIFICATION_ACTIVE")));
            }
        }
    }

    private static String reserve(PostgreSQLContainer<?> postgres, UUID userId, UUID accountId, int slot,
                                  CountDownLatch ready, CountDownLatch start) throws Exception {
        if (ready != null) ready.countDown();
        if (start != null) start.await();
        try (Connection connection = connection(postgres); Statement statement = connection.createStatement()) {
            statement.executeUpdate(allocationInsert(userId, accountId, slot, "SHADOW_PROMOTION"));
            return "RESERVED";
        } catch (SQLException error) {
            if (error.getMessage().contains("MICRO_LIVE_CAPACITY_EXHAUSTED")) {
                return "MICRO_LIVE_CAPACITY_EXHAUSTED";
            }
            throw error;
        }
    }

    private static String allocationInsert(UUID userId, UUID accountId, int slot, String statusReason) {
        return """
                insert into futuros_operaciones.user_copy_allocation (
                    wallet_id, score, id_user, execution_mode, status, status_reason, is_active,
                    copy_strategy_code, scope_type, scope_value, strategy_key,
                    sizing_mode, allocation_pct_source, exchange_account_id, reserved_capital_usd
                ) values (
                    'wallet-%1$d', 100, '%2$s', 'MICRO_LIVE', 'active', '%4$s', true,
                    'MOVEMENT_ALL', 'ALL', 'slot-%1$d', 'capacity-%3$s-slot-%1$d',
                    'FIXED_CAPITAL', 'FIXED_MICRO_BUDGET', '%3$s', 100
                )
                """.formatted(slot, userId, accountId, statusReason);
    }

    private static void seedUserAndAccount(PostgreSQLContainer<?> postgres, UUID userId, UUID accountId,
                                           String purpose) throws SQLException {
        try (Connection connection = connection(postgres); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    insert into futuros_operaciones.users (id, email)
                    values ('%s', '%s@example.test')
                    """.formatted(userId, userId));
            statement.executeUpdate("""
                    insert into futuros_operaciones.user_api_keys (
                        id_user_api_keys, user_id, exchange, api_key, api_secret, label,
                        account_purpose, active
                    ) values ('%s', '%s', 'BINANCE', 'test-key', 'test-secret', '%s', '%s', true)
                    """.formatted(accountId, userId, purpose, purpose));
        }
    }

    private static void seedCapacity(PostgreSQLContainer<?> postgres, UUID accountId, String equity,
                                     int effectiveCapacity, int reservedRecertificationSlots) throws SQLException {
        try (Connection connection = connection(postgres); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    insert into futuros_operaciones.micro_live_account_capacity (
                        execution_account_id, asset, authoritative_equity_usd, available_balance_usd,
                        safety_buffer_usd, eligible_capital_usd, budget_per_allocation_usd,
                        theoretical_capacity, effective_capacity, configured_max,
                        reserved_recertification_slots, observed_at, valid_until
                    ) values (
                        '%1$s', 'USDC', %2$s, %2$s, 0, %2$s, 100,
                        %3$d, %3$d, 0, %4$d, now(), now() + interval '5 minutes'
                    )
                    """.formatted(accountId, equity, effectiveCapacity, reservedRecertificationSlots));
        }
    }

    private static void seedCertification(PostgreSQLContainer<?> postgres, UUID certificationId,
                                          String walletId) throws SQLException {
        try (Connection connection = connection(postgres); Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    insert into futuros_operaciones.strategy_live_certification (
                        id, creation_key, wallet_id, strategy_code, strategy_version,
                        scope_type, scope_value, capital_band_min, capital_band_max,
                        target_leverage, exchange, quote_asset, sizing_policy_version,
                        symbol_mapping_version, fee_model_version, funding_model_version,
                        slippage_model_version, liquidity_model_version, evidence_level,
                        certification_status, evidence_snapshot, created_by, creation_reason
                    ) values (
                        '%1$s', 'capacity-%1$s', '%2$s', 'MOVEMENT_ALL', 'v1', 'ALL', 'ALL',
                        0, 1000, 5, 'BINANCE', 'USDC', 's1', 'm1', 'f1', 'fund1', 'slip1',
                        'liq1', 'REAL_VALIDATED', 'LIVE_DEGRADED', '{}'::jsonb,
                        'integration-test', 'capacity-priority-test'
                    )
                    """.formatted(certificationId, walletId));
        }
    }

    private static Connection connection(PostgreSQLContainer<?> postgres) throws SQLException {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private static long scalar(Statement statement, String sql) throws SQLException {
        try (var result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private static void assertSqlReason(String reason, SqlAction action) throws Exception {
        try {
            action.run();
            fail("expected SQL rejection " + reason);
        } catch (SQLException error) {
            assertTrue(error.getMessage().contains(reason), error::getMessage);
        }
    }

    @FunctionalInterface
    private interface SqlAction {
        void run() throws Exception;
    }
}
