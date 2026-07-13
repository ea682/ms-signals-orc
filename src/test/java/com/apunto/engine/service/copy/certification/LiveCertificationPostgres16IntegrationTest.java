package com.apunto.engine.service.copy.certification;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.service.copy.dispatch.CopyDispatchIdentity;
import com.apunto.engine.service.copy.dispatch.CopyDispatchPayloadConflictRecord;
import com.apunto.engine.service.copy.dispatch.CopyDispatchPayloadConflictRecorder;
import com.apunto.engine.service.copy.dispatch.CopyDispatchPayloadSnapshotFactory;
import com.apunto.engine.service.copy.dispatch.CopyDispatchRequest;
import com.apunto.engine.service.copy.dispatch.PostgresCopyDispatchPayloadConflictStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.PostgreSQLContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveCertificationPostgres16IntegrationTest {

    private static final UUID USER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;

    @BeforeAll
    static void startPostgres() throws Exception {
        String externalUrl = System.getProperty("copy.postgres.test.jdbc-url");
        HikariConfig config = new HikariConfig();
        if (externalUrl == null || externalUrl.isBlank()) {
            postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("live_certification_test")
                    .withUsername("copy_test")
                    .withPassword("copy_test");
            try {
                postgres.start();
            } catch (RuntimeException unavailable) {
                Assumptions.assumeTrue(false, "Docker unavailable for PostgreSQL certification test");
            }
            config.setJdbcUrl(postgres.getJdbcUrl());
            config.setUsername(postgres.getUsername());
            config.setPassword(postgres.getPassword());
        } else {
            config.setJdbcUrl(externalUrl);
            config.setUsername(System.getProperty("copy.postgres.test.username", "postgres"));
            config.setPassword(System.getProperty("copy.postgres.test.password", ""));
        }
        dataSource = new HikariDataSource(config);
        jdbc = new JdbcTemplate(dataSource);
        Integer version = jdbc.queryForObject("show server_version_num", Integer.class);
        assertTrue(version != null && version >= 160000,
                "integration test requires PostgreSQL 16 or newer");
        createRuntimePrerequisites();
        ResourceDatabasePopulator migration = new ResourceDatabasePopulator(new ClassPathResource(
                "db/migration/V202607130003__copy_simulation_certification_v3.sql"));
        migration.execute(dataSource);
        migration.execute(dataSource);
        ResourceDatabasePopulator conflictMigration = new ResourceDatabasePopulator(new ClassPathResource(
                "db/migration/V202607130004__copy_dispatch_payload_conflict_v3.sql"));
        conflictMigration.execute(dataSource);
        conflictMigration.execute(dataSource);
        ResourceDatabasePopulator positionLimitMigration = new ResourceDatabasePopulator(new ClassPathResource(
                "db/migration/V202607130005__copy_dispatch_user_position_limit_snapshot_v3.sql"));
        positionLimitMigration.execute(dataSource);
        positionLimitMigration.execute(dataSource);
    }

    @AfterAll
    static void stopPostgres() {
        if (dataSource != null) dataSource.close();
        if (postgres != null) postgres.stop();
    }

    @Test
    void manualCertificationAdoptionActivationAndHotGateUseTheSameAllocation() {
        ObjectMapper mapper = new ObjectMapper();
        PostgresLiveCertificationCatalogStore catalogStore =
                new PostgresLiveCertificationCatalogStore(jdbc, mapper);
        PostgresLiveCertificationTransitionStore transitionStore =
                new PostgresLiveCertificationTransitionStore(jdbc, mapper);
        LiveCertificationIdentity identity = new LiveCertificationIdentity(
                "0xa445", "MOVEMENT_ALL", "copy-strategy-v3", "ALL", "ALL",
                new BigDecimal("100"), new BigDecimal("250"), new BigDecimal("5"),
                "BINANCE", "USDC", "proportional-portfolio-v3", "binance-symbol-map-v3",
                "binance-fee-v3", "binance-funding-v3", "binance-slippage-v3",
                "order-book-liquidity-v3");

        ManualLiveCertificationCatalogService catalogService =
                new ManualLiveCertificationCatalogService(catalogStore, transitionStore);
        LiveCertificationCreateResult created = catalogService.create(new LiveCertificationCreateCommand(
                identity, LiveEvidenceLevel.MICRO_LIVE_CALIBRATED,
                LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                "integration-operator", "reviewed", Map.of("sampleCount", 30), "pg-create-1"));
        assertTrue(created.created());

        ManualLiveCertificationService transitionService = new ManualLiveCertificationService(
                transitionStore, new LiveCertificationTransitionPolicy());
        LiveCertificationTransitionResult approved = transitionService.transition(
                new LiveCertificationTransitionCommand(
                        created.certification().id(), 0L,
                        LiveCertificationStatus.MICRO_LIVE_VALIDATING,
                        LiveCertificationStatus.LIVE_APPROVED, false,
                        "integration-operator", "approved", Map.of("ticket", "CERT-1"),
                        "pg-approve-1"));
        assertTrue(approved.applied());

        Long allocationId = insertMicroLiveAllocation();
        PostgresLiveUserAdoptionStore adoptionStore = new PostgresLiveUserAdoptionStore(jdbc, mapper);
        LiveUserAdoptionApplicationService adoptionService = new LiveUserAdoptionApplicationService(
                catalogStore,
                new LiveUserAdoptionPersistenceService(new LiveUserAdoptionValidator(), adoptionStore));
        OffsetDateTime now = OffsetDateTime.now();
        LiveUserAdoptionResult adoption = adoptionService.validateAndPersist(new LiveUserAdoptionCommand(
                created.certification().id(), USER_ID, allocationId,
                new BigDecimal("300"), new BigDecimal("100"), new BigDecimal("5"), "USDC",
                "ISOLATED", "ISOLATED", true, true, true,
                now.minusSeconds(1), now.plusMinutes(10)));
        assertTrue(adoption.persisted());
        assertTrue(adoption.decision().valid());

        ManualLiveAllocationActivationService activationService =
                new ManualLiveAllocationActivationService(new PostgresLiveAllocationActivationStore(jdbc));
        LiveAllocationActivationResult activation = activationService.activate(
                new LiveAllocationActivationCommand(allocationId, created.certification().id(),
                        "integration-operator", "activate", "pg-activate-1"));
        assertTrue(activation.activated());
        assertEquals("LIVE", jdbc.queryForObject(
                "select execution_mode from futuros_operaciones.user_copy_allocation where id = ?",
                String.class, allocationId));

        LiveEntryAuthorizationRequest request = new LiveEntryAuthorizationRequest(
                USER_ID, allocationId, "0xa445", "MOVEMENT_ALL", "copy-strategy-v3", "ALL", "ALL",
                new BigDecimal("100"), new BigDecimal("5"), "BINANCE", "USDC",
                "proportional-portfolio-v3", "binance-symbol-map-v3", "binance-fee-v3",
                "binance-funding-v3", "binance-slippage-v3", "order-book-liquidity-v3");
        LiveEntryAuthorizationDecision gate = new LiveEntryAuthorizationService(
                new PostgresLiveCertificationReadStore(jdbc)).evaluate(request, OffsetDateTime.now());
        assertTrue(gate.allowed(), gate.reasonCode());

        assertEquals(2L, jdbc.queryForObject(
                "select count(*) from strategy_live_certification_audit", Long.class));
        assertEquals(1L, jdbc.queryForObject(
                "select count(*) from live_allocation_activation_audit", Long.class));
    }

    @Test
    void repeatedPayloadConflictIsOneOpenAlertAndPreservesPendingReservation() {
        UUID intentId = UUID.randomUUID();
        String key = "b".repeat(64);
        jdbc.update("""
                INSERT INTO futuros_operaciones.copy_dispatch_intent (
                    id, idempotency_key, request_hash, status, reservation_status, updated_at
                ) VALUES (?, ?, 'old-hash', 'NEW', 'PENDING', now())
                """, intentId, key);
        CopyDispatchIntentEntity intent = CopyDispatchIntentEntity.builder()
                .id(intentId).idempotencyKey(key).idUser("user-1").userCopyAllocationId(55L)
                .executionMode("LIVE").walletId("0xa445").strategyCode("MOVEMENT_ALL")
                .scopeType("ALL").scopeValue("ALL").sourceEventId("event-conflict")
                .sourceEventType("SOURCE_OPEN").copyIntent("OPEN").idOrderOrigin("origin-1")
                .symbol("BTCUSDC").side("BUY").positionSide("LONG").reduceOnly(false)
                .requestedQty(BigDecimal.ONE).requestedMarginUsd(new BigDecimal("20"))
                .requestedNotionalUsd(new BigDecimal("100")).referencePrice(new BigDecimal("100"))
                .requestedLeverage(5).clientOrderId("ct_existing").requestHash("old-hash")
                .status("NEW").build();
        OperationDto operation = OperationDto.builder().originId("origin-1").symbol("BTCUSDC")
                .quantity("2").clientOrderId("ct_incoming").userId("user-1").walletId("0xa445").build();
        CopyDispatchRequest request = new CopyDispatchRequest(key,
                new CopyDispatchIdentity("user-1", 55L, "LIVE", "MOVEMENT_ALL",
                        "ALL", "ALL", "event-conflict", "OPEN"),
                operation, "0xa445", "BTCUSDC", "BUY", "LONG", false,
                new BigDecimal("2"), new BigDecimal("20"), new BigDecimal("200"),
                new BigDecimal("100"), 5, null, true, "SOURCE_OPEN", "new-hash", "trace-pg");
        CopyDispatchPayloadConflictRecorder recorder = new CopyDispatchPayloadConflictRecorder(
                new PostgresCopyDispatchPayloadConflictStore(jdbc, new ObjectMapper()),
                new CopyDispatchPayloadSnapshotFactory());

        CopyDispatchPayloadConflictRecord first = recorder.record(intent, request);
        CopyDispatchPayloadConflictRecord repeated = recorder.record(intent, request);

        assertTrue(first.manualReviewRequired());
        assertTrue(!repeated.manualReviewRequired());
        assertEquals("MANUAL_REVIEW|PENDING", jdbc.queryForObject("""
                SELECT status || '|' || reservation_status
                FROM futuros_operaciones.copy_dispatch_intent WHERE id = ?
                """, String.class, intentId));
        assertEquals(1L, jdbc.queryForObject("""
                SELECT count(*) FROM futuros_operaciones.copy_dispatch_payload_conflict
                WHERE dispatch_intent_id = ? AND incoming_hash = 'new-hash'
                """, Long.class, intentId));
        assertEquals(2, jdbc.queryForObject("""
                SELECT conflict_count FROM futuros_operaciones.copy_dispatch_payload_conflict
                WHERE dispatch_intent_id = ? AND incoming_hash = 'new-hash'
                """, Integer.class, intentId));
    }

    private static Long insertMicroLiveAllocation() {
        return jdbc.queryForObject("""
                INSERT INTO futuros_operaciones.user_copy_allocation (
                    id_user, wallet_id, copy_strategy_code, scope_type, scope_value,
                    execution_mode, status, is_active, updated_at
                ) VALUES (?, '0xa445', 'MOVEMENT_ALL', 'ALL', 'ALL', 'MICRO_LIVE', 'active', TRUE, now())
                RETURNING id
                """, Long.class, USER_ID);
    }

    private static void createRuntimePrerequisites() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create schema futuros_operaciones");
            statement.execute("""
                    create table futuros_operaciones.user_copy_allocation (
                        id bigserial primary key,
                        id_user uuid not null,
                        wallet_id varchar(128) not null,
                        copy_strategy_code varchar(64) not null,
                        scope_type varchar(32) not null,
                        scope_value varchar(160) not null,
                        execution_mode varchar(16) not null,
                        status varchar(40) not null,
                        is_active boolean not null,
                        ends_at timestamptz,
                        status_reason varchar(160),
                        status_updated_at timestamptz,
                        updated_at timestamptz not null
                    )
                    """);
            statement.execute("""
                    create table futuros_operaciones.copy_operation (
                        id_operation uuid primary key,
                        user_copy_allocation_id bigint,
                        is_active boolean not null
                    )
                    """);
            statement.execute("""
                    create table futuros_operaciones.copy_dispatch_intent (
                        id uuid primary key,
                        idempotency_key varchar(64),
                        request_hash varchar(64),
                        user_copy_allocation_id bigint,
                        status varchar(32) not null,
                        reservation_status varchar(24) not null,
                        last_error_code varchar(80),
                        last_error_detail varchar(1000),
                        next_reconciliation_at timestamptz,
                        updated_at timestamptz not null
                    )
                    """);
        }
    }
}
