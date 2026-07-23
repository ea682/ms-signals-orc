package com.apunto.engine.service.copy.certification;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.service.copy.dispatch.CopyDispatchIdentity;
import com.apunto.engine.service.copy.dispatch.CopyDispatchPayloadConflictRecord;
import com.apunto.engine.service.copy.dispatch.CopyDispatchPayloadConflictRecorder;
import com.apunto.engine.service.copy.dispatch.CopyDispatchPayloadSnapshotFactory;
import com.apunto.engine.service.copy.dispatch.CopyDispatchRequest;
import com.apunto.engine.service.copy.dispatch.PostgresCopyDispatchPayloadConflictStore;
import com.apunto.engine.testsupport.ProductionBaselinePostgres;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveCertificationPostgres16IntegrationTest {

    private static final UUID USER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID MICRO_ACCOUNT_ID = UUID.fromString("22222222-2222-2222-2222-222222222223");
    private static final UUID LIVE_ACCOUNT_ID = UUID.fromString("22222222-2222-2222-2222-222222222224");
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
            ProductionBaselinePostgres.restoreAndMigrate(postgres);
            config.setJdbcUrl(postgres.getJdbcUrl());
            config.setUsername(postgres.getUsername());
            config.setPassword(postgres.getPassword());
        } else {
            config.setJdbcUrl(externalUrl);
            config.setUsername(System.getProperty("copy.postgres.test.username", "postgres"));
            config.setPassword(System.getProperty("copy.postgres.test.password", ""));
        }
        config.setSchema("futuros_operaciones");
        dataSource = new HikariDataSource(config);
        jdbc = new JdbcTemplate(dataSource);
        seedExecutionAccounts();
        Integer version = jdbc.queryForObject("show server_version_num", Integer.class);
        assertTrue(version != null && version >= 160000,
                "integration test requires PostgreSQL 16 or newer");
    }

    @AfterAll
    static void stopPostgres() {
        if (dataSource != null) dataSource.close();
        if (postgres != null) postgres.stop();
    }

    @Test
    void certificationClosesMicroAndActivatesANewAdoptedLiveAllocation() {
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
                        "integration-operator", "approved", Map.of(
                                "ticket", "CERT-1",
                                "automaticPolicyPassed", true,
                                "realMicroLiveEvidence", true,
                                "validMicroLiveTests", 1),
                        "pg-approve-1"));
        assertTrue(approved.applied());

        Long microAllocationId = insertMicroLiveAllocation();
        Long liveAllocationId = insertPendingLiveAllocation(created.certification().id());
        assertTrue(!microAllocationId.equals(liveAllocationId));
        PostgresLiveUserAdoptionStore adoptionStore = new PostgresLiveUserAdoptionStore(jdbc, mapper);
        LiveUserAdoptionApplicationService adoptionService = new LiveUserAdoptionApplicationService(
                catalogStore,
                new LiveUserAdoptionPersistenceService(new LiveUserAdoptionValidator(), adoptionStore));
        OffsetDateTime now = OffsetDateTime.now();
        LiveUserAdoptionResult adoption = adoptionService.validateAndPersist(new LiveUserAdoptionCommand(
                created.certification().id(), USER_ID, liveAllocationId,
                new BigDecimal("300"), new BigDecimal("100"), new BigDecimal("5"), "USDC",
                "ISOLATED", "ISOLATED", true, true, true,
                now.minusSeconds(1), now.plusMinutes(10)));
        assertTrue(adoption.persisted());
        assertTrue(adoption.decision().valid());

        ManualLiveAllocationActivationService activationService =
                new ManualLiveAllocationActivationService(new PostgresLiveAllocationActivationStore(jdbc));
        LiveAllocationActivationResult activation = activationService.activate(
                new LiveAllocationActivationCommand(liveAllocationId, created.certification().id(),
                        "integration-operator", "activate", "pg-activate-1"));
        assertTrue(activation.activated(), activation.reasonCode());
        assertEquals("LIVE|active|true", jdbc.queryForObject("""
                select execution_mode || '|' || lower(status) || '|' || is_active
                from futuros_operaciones.user_copy_allocation where id = ?
                """, String.class, liveAllocationId));
        assertEquals("MICRO_LIVE|closed|false", jdbc.queryForObject("""
                select execution_mode || '|' || lower(status) || '|' || is_active
                from futuros_operaciones.user_copy_allocation where id = ?
                """, String.class, microAllocationId));
        assertEquals(liveAllocationId, jdbc.queryForObject("""
                select allocation_id from futuros_operaciones.user_live_certification_adoption
                where certification_id = ? and user_id = ?
                """, Long.class, created.certification().id(), USER_ID));

        LiveEntryAuthorizationRequest request = new LiveEntryAuthorizationRequest(
                USER_ID, liveAllocationId, "0xa445", "MOVEMENT_ALL", "copy-strategy-v3", "ALL", "ALL",
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
                    id, idempotency_key, id_user, execution_mode, source_event_id,
                    copy_intent, symbol, client_order_id, request_hash, status,
                    reservation_status, created_at, updated_at
                ) VALUES (?, ?, 'user-1', 'LIVE', 'event-conflict',
                          'OPEN', 'BTCUSDC', 'ct_existing', 'old-hash', 'NEW',
                          'PENDING', now(), now())
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
                    execution_mode, status, is_active, score, strategy_key,
                    sizing_mode, allocation_pct_source, exchange_account_id,
                    reserved_capital_usd, updated_at
                ) VALUES (?, '0xa445', 'MOVEMENT_ALL', 'ALL', 'ALL', 'MICRO_LIVE',
                          'active', TRUE, 90, '0xa445|MOVEMENT_ALL|ALL|ALL',
                          'FIXED_CAPITAL', 'FIXED_MICRO_BUDGET', ?, 100, now())
                RETURNING id
                """, Long.class, USER_ID, MICRO_ACCOUNT_ID);
    }

    private static Long insertPendingLiveAllocation(UUID certificationId) {
        return jdbc.queryForObject("""
                INSERT INTO futuros_operaciones.user_copy_allocation (
                    id_user, wallet_id, allocation_pct, score, copy_strategy_code,
                    scope_type, scope_value, strategy_key, execution_mode, status,
                    is_active, sizing_mode, allocation_pct_source, allocation_pct_source_id,
                    allocation_pct_calculated_at, allocation_pct_valid_until,
                    wallet_total_allocation_pct, leverage_override, capital_asset,
                    resolved_quote_asset, live_certification_id, updated_at, activation_at
                    , exchange_account_id
                ) VALUES (?, '0xa445', 0.10, 90, 'MOVEMENT_ALL', 'ALL', 'ALL',
                          '0xa445|MOVEMENT_ALL|ALL|ALL', 'LIVE', 'paused', TRUE,
                          'PERCENTAGE', 'SIGNALS_CURRENT_LIVE_DISTRIBUTION', ?,
                          now(), now() + interval '10 minutes', 0.10, 5, 'USDC', 'USDC',
                          ?, now(), now(), ?)
                RETURNING id
                """, Long.class, USER_ID, UUID.randomUUID(), certificationId, LIVE_ACCOUNT_ID);
    }

    private static void seedExecutionAccounts() {
        jdbc.update("""
                INSERT INTO futuros_operaciones.users (id, email)
                VALUES (?, 'live-certification@example.test')
                ON CONFLICT (id) DO NOTHING
                """, USER_ID);
        jdbc.update("""
                INSERT INTO futuros_operaciones.user_api_keys (
                    id_user_api_keys, user_id, exchange, api_key, api_secret, label,
                    account_purpose, active
                ) VALUES (?, ?, 'BINANCE', 'micro-key', 'micro-secret', 'MICRO_LIVE',
                          'MICRO_LIVE', true)
                ON CONFLICT DO NOTHING
                """, MICRO_ACCOUNT_ID, USER_ID);
        jdbc.update("""
                INSERT INTO futuros_operaciones.user_api_keys (
                    id_user_api_keys, user_id, exchange, api_key, api_secret, label,
                    account_purpose, active
                ) VALUES (?, ?, 'BINANCE', 'live-key', 'live-secret', 'LIVE', 'LIVE', true)
                ON CONFLICT DO NOTHING
                """, LIVE_ACCOUNT_ID, USER_ID);
        jdbc.update("""
                INSERT INTO futuros_operaciones.micro_live_account_capacity (
                    execution_account_id, asset, authoritative_equity_usd, available_balance_usd,
                    safety_buffer_usd, eligible_capital_usd, budget_per_allocation_usd,
                    theoretical_capacity, effective_capacity, configured_max,
                    reserved_recertification_slots, observed_at, valid_until
                ) VALUES (?, 'USDC', 500, 500, 0, 500, 100, 5, 5, 0, 0,
                          now(), now() + interval '5 minutes')
                ON CONFLICT (execution_account_id) DO UPDATE SET
                    authoritative_equity_usd = excluded.authoritative_equity_usd,
                    available_balance_usd = excluded.available_balance_usd,
                    eligible_capital_usd = excluded.eligible_capital_usd,
                    theoretical_capacity = excluded.theoretical_capacity,
                    effective_capacity = excluded.effective_capacity,
                    observed_at = excluded.observed_at,
                    valid_until = excluded.valid_until
                """, MICRO_ACCOUNT_ID);
    }

}
