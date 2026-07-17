package com.apunto.engine.hyperliquid;

import com.apunto.engine.entity.OperationMovementEventEntity;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OperationMovementEventPostgresArrayIntegrationTest {

    private static PostgreSQLContainer<?> postgres;
    private static StandardServiceRegistry registry;
    private static SessionFactory sessionFactory;
    private static String jdbcUrl;
    private static String username;
    private static String password;
    private static boolean externalDatabase;

    @BeforeAll
    static void startPostgresAndValidateSchema() throws Exception {
        jdbcUrl = System.getProperty("copy.postgres.test.jdbc-url");
        externalDatabase = jdbcUrl != null && !jdbcUrl.isBlank();
        if (externalDatabase) {
            username = System.getProperty("copy.postgres.test.username");
            password = System.getProperty("copy.postgres.test.password");
        } else {
            postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("signals_array_test")
                    .withUsername("signals_test")
                    .withPassword("signals_test");
            try {
                postgres.start();
            } catch (RuntimeException unavailable) {
                Assumptions.assumeTrue(false,
                        "Docker unavailable; configure copy.postgres.test.jdbc-url for PostgreSQL validation");
            }
            jdbcUrl = postgres.getJdbcUrl();
            username = postgres.getUsername();
            password = postgres.getPassword();
        }

        try (Connection connection = DriverManager.getConnection(
                jdbcUrl, username, password);
             Statement statement = connection.createStatement()) {
            if (!externalDatabase) {
                statement.execute("create schema futuros_operaciones");
                statement.execute(CREATE_MOVEMENT_TABLE);
            }
        }

        registry = new StandardServiceRegistryBuilder()
                .applySetting(AvailableSettings.JAKARTA_JDBC_URL, jdbcUrl)
                .applySetting(AvailableSettings.JAKARTA_JDBC_USER, username)
                .applySetting(AvailableSettings.JAKARTA_JDBC_PASSWORD, password)
                .applySetting(AvailableSettings.JAKARTA_HBM2DDL_DATABASE_ACTION, "validate")
                .applySetting(AvailableSettings.DEFAULT_SCHEMA, "futuros_operaciones")
                .build();
        sessionFactory = new MetadataSources(registry)
                .addAnnotatedClass(OperationMovementEventEntity.class)
                .buildMetadata()
                .buildSessionFactory();
    }

    @AfterAll
    static void stopPostgres() {
        if (sessionFactory != null) sessionFactory.close();
        if (registry != null) StandardServiceRegistryBuilder.destroy(registry);
        if (postgres != null && postgres.isRunning()) postgres.stop();
    }

    @Test
    void textArrayRoundTripsLifecycleFlagsWithoutJsonConversion() {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        List<String> expected = List.of("FILL_DELAYED", "SOURCE_ESTIMATED");
        OperationMovementEventEntity entity = OperationMovementEventEntity.builder()
                .idOrderOrigin(UUID.randomUUID())
                .movementKey("array-contract-" + UUID.randomUUID())
                .positionKey("array-contract-" + UUID.randomUUID())
                .idWalletOrigin("0xarray")
                .parsymbol("BTCUSDC")
                .typeOperation("LONG")
                .eventType("OPEN")
                .deltaType("OPEN")
                .lifecycleQualityFlags(expected.toArray(String[]::new))
                .eventTime(now)
                .dateCreation(now)
                .build();

        sessionFactory.inSession(session -> {
            var transaction = session.beginTransaction();
            try {
                session.persist(entity);
                session.flush();
                UUID eventId = entity.getIdEvent();
                session.clear();

                OperationMovementEventEntity loaded = session.find(OperationMovementEventEntity.class, eventId);
                assertEquals(expected, List.of(loaded.getLifecycleQualityFlags()));
            } finally {
                transaction.rollback();
            }
        });
    }

    private static final String CREATE_MOVEMENT_TABLE = """
            create table futuros_operaciones.operation_movement_event (
                id_event uuid primary key,
                id_order_origin uuid not null,
                movement_key varchar(600) not null,
                idempotency_key varchar(600),
                position_key varchar(300) not null,
                id_wallet_origin varchar(180) not null,
                parsymbol varchar(40) not null,
                type_operation varchar(20) not null,
                event_type varchar(30) not null,
                delta_type varchar(30) not null,
                source_event_type varchar(30), status varchar(30),
                size_qty numeric(38,18), signed_size_qty numeric(38,18),
                previous_size_qty numeric(38,18), resulting_size_qty numeric(38,18),
                delta_size_qty numeric(38,18), notional_usd numeric(38,18),
                margin_used_usd numeric(38,18), entry_price numeric(38,18),
                mark_price numeric(38,18), exit_price numeric(38,18),
                realized_pnl_usd numeric(38,18), leverage numeric(38,18),
                raw_notional_usd numeric(38,18), position_notional_usd numeric(38,18),
                closed_notional_usd numeric(38,18), closed_margin_used_usd numeric(38,18),
                effective_close_qty numeric(38,18), effective_entry_price numeric(38,18),
                effective_exit_price numeric(38,18), effective_realized_pnl_usd numeric(38,18),
                normalization_status varchar(40), normalization_reason varchar(180),
                economic_event_kind varchar(40), economic_event_version integer,
                source_event_id varchar(600), source_sequence bigint,
                source_fee_usd numeric(38,18), funding_pnl_usd numeric(38,18),
                execution_price_basis varchar(80), notional_basis varchar(80),
                lifecycle_quality_flags text[], source_estimated boolean,
                wallet_version bigint, snapshot_version bigint,
                source_ts timestamptz, detected_at timestamptz, published_at timestamptz,
                event_time timestamptz not null, trace_id varchar(128), source varchar(80),
                reason_code varchar(120), copy_eligible_users integer,
                copy_submitted_tasks integer, copy_business_skipped integer,
                copy_fallback_jobs integer, copy_fallback_used boolean,
                raw jsonb, date_creation timestamptz not null
            )
            """;
}
