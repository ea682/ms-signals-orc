package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperationMovementEventRecordCommand;
import com.apunto.engine.entity.OperationMovementEventEntity;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.apunto.engine.outbox.service.MetricMovementOutboxService;
import com.apunto.engine.repository.OperationMovementEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationMovementEconomicNormalizationTest {

    @Test
    void blockedProductionFlipIsAuditOnlyAndCarriesNoPnl() throws Exception {
        OperationMovementEventServiceImpl service = service();
        HyperliquidMappedDelta mapped = productionIncompleteFlip();
        Method fromMapped = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "fromMappedDelta",
                HyperliquidMappedDelta.class,
                HyperliquidDirectCopyDispatchResult.class,
                String.class
        );
        fromMapped.setAccessible(true);
        OperationMovementEventRecordCommand command =
                (OperationMovementEventRecordCommand) fromMapped.invoke(
                        service,
                        mapped,
                        HyperliquidDirectCopyDispatchResult.ok(
                                0, 0, 1, 0, false, "FLIP_EXECUTION_BASIS_MISSING"),
                        "FLIP_EXECUTION_BASIS_MISSING"
                );

        assertEquals("hyperliquid_direct_ingest_audit_only", command.getSource());
        assertEquals("FLIP_EXECUTION_BASIS_MISSING", command.getNormalizationStatus());
        assertNull(command.getEffectiveRealizedPnlUsd());
        assertTrue(command.getLifecycleQualityFlags().contains(
                "FLIP_EXECUTION_BASIS_MISSING"));

        Method toEntity = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "toEntity",
                OperationMovementEventRecordCommand.class,
                OperationMovementEventEntity.class
        );
        toEntity.setAccessible(true);
        OperationMovementEventEntity entity = (OperationMovementEventEntity) toEntity.invoke(
                service, command, null);

        assertNull(entity.getRealizedPnlUsd());
        assertNull(entity.getEffectiveRealizedPnlUsd());
        assertEquals("hyperliquid_direct_ingest_audit_only", entity.getSource());
        assertFalse(entity.getRaw().path("metricEligible").asBoolean(true));
        assertEquals("audit_only_excluded_from_joyas",
                entity.getRaw().path("metricDecisionUse").asText());
    }

    @Test
    void sourceNotClosingCannotBeUpgradedToRecoveredClosingEconomics() throws Exception {
        OperationMovementEventServiceImpl service = service();
        OperationMovementEventRecordCommand command = OperationMovementEventRecordCommand.builder()
                .typeOperation("LONG")
                .eventType("UNKNOWN")
                .deltaType("RESIZE")
                .sizeQty(new BigDecimal("8"))
                .entryPrice(new BigDecimal("100"))
                .markPrice(new BigDecimal("110"))
                .normalizationStatus("NOT_CLOSING")
                .normalizationReason("no_close_or_reduce_quantity")
                .build();
        OperationMovementEventEntity previous = OperationMovementEventEntity.builder()
                .resultingSizeQty(new BigDecimal("10"))
                .entryPrice(new BigDecimal("100"))
                .typeOperation("LONG")
                .build();

        Method normalize = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "normalizeMovementValues",
                OperationMovementEventRecordCommand.class,
                OperationMovementEventEntity.class,
                String.class,
                BigDecimal.class,
                BigDecimal.class
        );
        normalize.setAccessible(true);
        Object result = normalize.invoke(
                service,
                command,
                previous,
                "REDUCE",
                new BigDecimal("-2"),
                new BigDecimal("20")
        );

        assertEquals("SEMANTIC_CONFLICT", accessor(result, "normalizationStatus"));
        assertEquals("source_not_closing_but_ledger_classified_reduce", accessor(result, "normalizationReason"));
        assertNull(accessor(result, "effectiveCloseQty"));
        assertNull(accessor(result, "effectiveRealizedPnlUsd"));
    }

    @Test
    void legacyProductionPositionDeltaPnlIsDroppedInsteadOfBecomingRealPnl() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OperationMovementEventServiceImpl service = service(registry);
        HyperliquidMappedDelta mapped = productionEstimatedPositionDelta();
        Method fromMapped = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "fromMappedDelta",
                HyperliquidMappedDelta.class,
                HyperliquidDirectCopyDispatchResult.class,
                String.class
        );
        fromMapped.setAccessible(true);
        OperationMovementEventRecordCommand command =
                (OperationMovementEventRecordCommand) fromMapped.invoke(
                        service,
                        mapped,
                        HyperliquidDirectCopyDispatchResult.ok(
                                0, 0, 1, 0, false, "resize_without_open_copy"),
                        "resize_without_open_copy"
                );

        assertNull(command.getEffectiveRealizedPnlUsd());
        assertEquals("PARTIAL_RECOVERY", command.getNormalizationStatus());
        assertEquals(
                "closed_notional_recovered_user_fill_pnl_unavailable",
                command.getNormalizationReason());

        Method toEntity = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "toEntity",
                OperationMovementEventRecordCommand.class,
                OperationMovementEventEntity.class
        );
        toEntity.setAccessible(true);
        OperationMovementEventEntity previous = OperationMovementEventEntity.builder()
                .resultingSizeQty(new BigDecimal("25080"))
                .entryPrice(new BigDecimal("0.073194059479669269"))
                .typeOperation("LONG")
                .build();
        OperationMovementEventEntity entity =
                (OperationMovementEventEntity) toEntity.invoke(service, command, previous);

        assertEquals("REDUCE", entity.getEventType());
        assertNull(entity.getRealizedPnlUsd());
        assertNull(entity.getEffectiveRealizedPnlUsd());
        assertEquals(1.0d, registry.find("position_delta_without_pnl_total")
                .tag("delta_type", "RESIZE").counter().count());
    }

    @Test
    void authoritativeUserFillPreservesPnlFeeAndFundingExactly() throws Exception {
        HyperliquidMappedDelta estimated = productionEstimatedPositionDelta();
        HyperliquidDeltaRequest original = estimated.request();
        HyperliquidDeltaRequest authoritative = copyWithAuthoritativeEconomics(
                original,
                new BigDecimal("-0.047167624614110727"),
                new BigDecimal("0.000001230000000000"),
                new BigDecimal("-0.000004560000000000")
        );
        HyperliquidMappedDelta mapped = new HyperliquidDeltaOperacionMapper().map(
                authoritative, authoritative.idempotencyKey());
        OperationMovementEventServiceImpl service = service();
        Method fromMapped = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "fromMappedDelta",
                HyperliquidMappedDelta.class,
                HyperliquidDirectCopyDispatchResult.class,
                String.class
        );
        fromMapped.setAccessible(true);
        OperationMovementEventRecordCommand command =
                (OperationMovementEventRecordCommand) fromMapped.invoke(
                        service,
                        mapped,
                        HyperliquidDirectCopyDispatchResult.ok(
                                0, 0, 1, 0, false, "authoritative_user_fill"),
                        "authoritative_user_fill"
                );

        assertEquals(0, new BigDecimal("-0.047167624614110727")
                .compareTo(command.getEffectiveRealizedPnlUsd()));
        assertEquals(0, new BigDecimal("0.000001230000000000")
                .compareTo(command.getSourceFeeUsd()));
        assertEquals(0, new BigDecimal("-0.000004560000000000")
                .compareTo(command.getFundingPnlUsd()));
        assertEquals("USER_FILL", command.getEconomicEventKind());
        assertFalse(command.getLifecycleQualityFlags().contains(
                "POSITION_DELTA_NOT_FILL"));
    }

    @Test
    void aggregatedPositionDeltaRemainsEstimatedPnlFreeAndObservable() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OperationMovementEventServiceImpl service = service(registry);
        HyperliquidMappedDelta mapped = productionAggregatedPositionDelta();
        Method fromMapped = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "fromMappedDelta",
                HyperliquidMappedDelta.class,
                HyperliquidDirectCopyDispatchResult.class,
                String.class
        );
        fromMapped.setAccessible(true);
        OperationMovementEventRecordCommand command =
                (OperationMovementEventRecordCommand) fromMapped.invoke(
                        service,
                        mapped,
                        HyperliquidDirectCopyDispatchResult.ok(
                                0, 0, 1, 0, false, "aggregate_replay"),
                        "aggregate_replay"
                );

        assertEquals("POSITION_DELTA", command.getEconomicEventKind());
        assertTrue(command.getSourceEstimated());
        assertNull(command.getEffectiveRealizedPnlUsd());
        assertTrue(command.getLifecycleQualityFlags().contains(
                "AGGREGATED_POSITION_DELTA"));
        assertTrue(command.getLifecycleQualityFlags().contains(
                "FILL_HISTORY_INCOMPLETE"));
        assertEquals(1.0d, registry.find("aggregated_position_delta_total")
                .tag("delta_type", "RESIZE").counter().count());
        assertEquals(1.0d, registry.find("position_delta_without_pnl_total")
                .tag("delta_type", "RESIZE").counter().count());

        Method toEntity = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "toEntity",
                OperationMovementEventRecordCommand.class,
                OperationMovementEventEntity.class
        );
        toEntity.setAccessible(true);
        OperationMovementEventEntity previous = OperationMovementEventEntity.builder()
                .resultingSizeQty(new BigDecimal("25080"))
                .entryPrice(new BigDecimal("0.073194059479669269"))
                .typeOperation("LONG")
                .build();
        OperationMovementEventEntity entity =
                (OperationMovementEventEntity) toEntity.invoke(service, command, previous);
        assertNull(entity.getRealizedPnlUsd());
        assertNull(entity.getEffectiveRealizedPnlUsd());
    }

    private Object accessor(Object target, String methodName) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }

    private HyperliquidMappedDelta productionIncompleteFlip() throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        try (InputStream fixture = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-d-incomplete-flip.json")) {
            assertNotNull(fixture);
            JsonNode root = mapper.readTree(fixture);
            HyperliquidDeltaRequest request = mapper.treeToValue(
                    root.get("request"), HyperliquidDeltaRequest.class);
            return new HyperliquidDeltaOperacionMapper().map(
                    request, request.idempotencyKey());
        }
    }

    private HyperliquidMappedDelta productionEstimatedPositionDelta() throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        try (InputStream fixture = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-f-position-delta-pnl.json")) {
            assertNotNull(fixture);
            JsonNode root = mapper.readTree(fixture);
            HyperliquidDeltaRequest request = mapper.treeToValue(
                    root.get("request"), HyperliquidDeltaRequest.class);
            return new HyperliquidDeltaOperacionMapper().map(
                    request, request.idempotencyKey());
        }
    }

    private HyperliquidMappedDelta productionAggregatedPositionDelta() throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        try (InputStream fixture = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-f-position-delta-pnl.json")) {
            assertNotNull(fixture);
            JsonNode root = mapper.readTree(fixture);
            ObjectNode requestNode = (ObjectNode) root.get("request");
            requestNode.put("normalizationStatus", "AGGREGATED_POSITION_DELTA");
            requestNode.put(
                    "normalizationReason",
                    "net_delta_spans_suppressed_source_fills_pnl_not_attributed");
            requestNode.putNull("effectiveRealizedPnlUsd");
            requestNode.putArray("lifecycleQualityFlags")
                    .add("AGGREGATED_POSITION_DELTA")
                    .add("FILL_HISTORY_INCOMPLETE")
                    .add("SOURCE_ESTIMATED");
            HyperliquidDeltaRequest request = mapper.treeToValue(
                    requestNode, HyperliquidDeltaRequest.class);
            return new HyperliquidDeltaOperacionMapper().map(
                    request, request.idempotencyKey());
        }
    }

    private HyperliquidDeltaRequest copyWithAuthoritativeEconomics(
            HyperliquidDeltaRequest source,
            BigDecimal pnl,
            BigDecimal fee,
            BigDecimal funding
    ) {
        return new HyperliquidDeltaRequest(
                source.eventId(), source.idempotencyKey(), source.eventType(),
                source.deltaType(), source.platform(), source.wallet(),
                source.accountId(), source.symbol(), source.side(), source.status(),
                source.sizeQty(), source.signedSizeQty(), source.notionalUsd(),
                source.marginUsedUsd(), source.entryPrice(), source.markPrice(),
                source.leverage(), source.rawNotionalUsd(), source.positionNotionalUsd(),
                source.closedNotionalUsd(), source.closedMarginUsedUsd(),
                source.effectiveCloseQty(), source.effectiveEntryPrice(),
                source.effectiveExitPrice(), pnl, "RECOVERED",
                "authoritative_user_fill_closed_pnl", source.sourceTs(),
                source.detectedAt(), source.publishedAt(), source.walletVersion(),
                source.snapshotVersion(), source.externalId(), source.rawReference(),
                false, "USER_FILL", 2,
                "hyperliquid:user-fill:wallet_sanitized_f001:293030554422796",
                293030554422796L, fee, funding,
                "HYPERLIQUID_USER_FILL_PX", "EXECUTED_QTY_X_PRICE",
                java.util.List.of(), false, source.sourceAccountEquityUsd(),
                source.equityObservedAt(), source.equitySource(),
                source.equityFreshnessMs(), source.equityQuality(),
                source.sourceSnapshotVersion(), source.sourcePositionNotionalUsd(),
                source.sourcePositionQuantity(), source.sourceMarkPrice(),
                source.sourceEntryPrice(), source.sourceLeverage(),
                source.sourceSide(), source.sourcePortfolioPositions(),
                source.sourcePortfolioSnapshotVersion(),
                source.sourcePortfolioComplete()
        );
    }

    @SuppressWarnings("unchecked")
    private OperationMovementEventServiceImpl service() {
        return service(new SimpleMeterRegistry());
    }

    private OperationMovementEventServiceImpl service(SimpleMeterRegistry registry) {
        OperationMovementEventRepository repository = (OperationMovementEventRepository) Proxy.newProxyInstance(
                OperationMovementEventRepository.class.getClassLoader(),
                new Class<?>[]{OperationMovementEventRepository.class},
                (proxy, method, args) -> defaultValue(method.getReturnType())
        );
        MetricMovementOutboxService outbox = ignored -> { };
        return new OperationMovementEventServiceImpl(
                repository,
                new ObjectMapper().findAndRegisterModules(),
                outbox,
                new NoopTransactionManager(),
                registry,
                false,
                1,
                1
        );
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class || type == short.class || type == byte.class || type == long.class) {
            return 0;
        }
        if (type == float.class || type == double.class) {
            return 0.0;
        }
        return null;
    }

    private static final class NoopTransactionManager implements PlatformTransactionManager {
        @Override
        public TransactionStatus getTransaction(TransactionDefinition definition) {
            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
        }

        @Override
        public void rollback(TransactionStatus status) {
        }
    }
}
