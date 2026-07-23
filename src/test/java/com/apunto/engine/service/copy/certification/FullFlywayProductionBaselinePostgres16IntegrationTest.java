package com.apunto.engine.service.copy.certification;

import com.apunto.engine.testsupport.ProductionBaselinePostgres;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullFlywayProductionBaselinePostgres16IntegrationTest {

    @Test
    void productionSchemaBaselineValidatesEveryHistoricalMigrationAndAppliesLifecycleMigrations() throws Exception {
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                .withDatabaseName("copy_full_flyway_test")
                .withUsername("copy_test")
                .withPassword("copy_test")) {
            postgres.start();
            MigrateResult result = ProductionBaselinePostgres.restoreAndMigrate(postgres);
            assertEquals(8, result.migrationsExecuted,
                    "the production baseline must receive exactly the lifecycle migrations");
            MigrationInfo[] pending = ProductionBaselinePostgres.flyway(postgres).info().pending();
            assertEquals(0, pending.length,
                    "all real Flyway migrations must be applied, pending=" + Arrays.toString(pending));
            assertLifecycleSchema(postgres);
        }
    }

    private static void assertLifecycleSchema(PostgreSQLContainer<?> postgres) throws Exception {
        try (Connection connection = java.sql.DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             Statement statement = connection.createStatement()) {
            assertEquals(1L, scalar(statement, """
                    select count(*) from information_schema.tables
                    where table_schema = 'futuros_operaciones'
                      and table_name = 'user_wallet_copy_preference'
                    """));
            assertEquals(1L, scalar(statement, """
                    select count(*) from information_schema.tables
                    where table_schema = 'futuros_operaciones'
                      and table_name = 'copy_round_trip_execution_quality'
                    """));
            assertEquals(1L, scalar(statement, """
                    select count(*) from information_schema.tables
                    where table_schema = 'futuros_operaciones'
                      and table_name = 'copy_execution_job'
                    """), "the enabled execution worker requires its durable queue table");
            assertEquals(2L, scalar(statement, """
                    select count(*) from information_schema.columns
                    where table_schema = 'futuros_operaciones'
                      and table_name = 'user_copy_allocation'
                      and column_name in ('activation_at', 'live_certification_id')
                    """));
            assertEquals(1L, scalar(statement, """
                    select count(*) from information_schema.tables
                    where table_schema = 'futuros_operaciones'
                      and table_name = 'copy_position_ownership'
                    """));
            assertEquals(1L, scalar(statement, """
                    select count(*) from information_schema.tables
                    where table_schema = 'futuros_operaciones'
                      and table_name = 'copy_flip_saga'
                    """));
            assertEquals(1L, scalar(statement, """
                    select count(*) from information_schema.tables
                    where table_schema = 'futuros_operaciones'
                      and table_name = 'shedlock'
                    """));
            assertEquals(2L, scalar(statement, """
                    select count(*) from information_schema.tables
                    where table_schema = 'futuros_operaciones'
                      and table_name in ('micro_live_account_capacity',
                                         'micro_live_recertification_request')
                    """));
            assertEquals(2L, scalar(statement, """
                    select count(*) from information_schema.columns
                    where table_schema = 'futuros_operaciones'
                      and table_name = 'user_api_keys'
                      and column_name in ('exchange_account_ref', 'identity_verified_at')
                    """));
            assertEquals(8L, scalar(statement, """
                    select count(*) from futuros_operaciones.flyway_schema_history
                    where version in ('202607170001', '202607170002', '202607180001', '202607180002',
                                      '202607180003', '202607180004', '202607180005',
                                      '202607180006') and success
                    """));
            assertEquals(0L, scalar(statement, """
                    select count(*)
                    from pg_constraint
                    where connamespace = 'futuros_operaciones'::regnamespace
                      and conname in (
                        'chk_user_api_keys_account_purpose',
                        'fk_user_copy_allocation_exchange_account',
                        'fk_copy_operation_exchange_account',
                        'fk_copy_operation_event_exchange_account',
                        'fk_copy_dispatch_intent_exchange_account',
                        'fk_copy_economic_cycle_exchange_account',
                        'chk_real_allocation_execution_account',
                        'chk_micro_live_reserved_capital'
                      )
                      and not convalidated
                    """), "execution-account constraints must certify the migrated history");
        }
    }

    private static long scalar(Statement statement, String sql) throws Exception {
        try (ResultSet rows = statement.executeQuery(sql)) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }
}
