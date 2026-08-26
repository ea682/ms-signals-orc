package com.apunto.engine.hyperliquid;

import com.apunto.engine.dto.OperationMovementEventRecordCommand;
import com.apunto.engine.entity.OperationMovementEventEntity;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.apunto.engine.outbox.dto.MetricMovementPersistedEvent;
import com.apunto.engine.outbox.service.MetricMovementOutboxService;
import com.apunto.engine.outbox.service.impl.MetricMovementOutboxServiceImpl;
import com.apunto.engine.repository.OperationMovementEventRepository;
import com.apunto.engine.service.impl.OperationMovementEventServiceImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthoritativeUserFillStateContractTest {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void authoritativeBeforeAfterWinsOverMatchingLocalLedger() throws Exception {
        OperationMovementEventEntity entity = entity(
                request("RESIZE", "LONG", "OPEN", "10", "8", "-2", "2", "LIVE_USER_FILL"),
                previous("10"));

        assertDecimal("10", entity.getPreviousSizeQty());
        assertDecimal("8", entity.getResultingSizeQty());
        assertDecimal("-2", entity.getDeltaSizeQty());
        assertEquals("REDUCE", entity.getEventType());
        assertEquals("COMPLETE", economicBasisStatus(entity));
        assertTrue(kafkaEvent(entity).metricEligible());
    }

    @Test
    void localLedgerIsOnlyAContinuityCheckAndNeverReplacesSourceBefore() throws Exception {
        OperationMovementEventEntity entity = entity(
                request("RESIZE", "LONG", "OPEN", "10", "8", "-2", "2", "LIVE_USER_FILL"),
                previous("15"));

        assertDecimal("10", entity.getPreviousSizeQty());
        assertDecimal("8", entity.getResultingSizeQty());
        assertDecimal("-2", entity.getDeltaSizeQty());
        assertEquals("SOURCE_LEDGER_DIVERGENCE", economicBasisStatus(entity));
        assertFalse(kafkaEvent(entity).metricEligible());
    }

    @Test
    void productionZecReductionEstablishesBoundaryOverAuditOnlySnapshot()
            throws Exception {
        HyperliquidDeltaRequest request = productionRequest(
                "hyperliquid:trade:0x29998ebd5be758fdaa06f0ef48d6c890978b65de:970505954175203",
                970505954175203L,
                "ZECUSDT",
                "LONG",
                "259.57",
                "209.57",
                "-50",
                "50",
                "745",
                "2312.385",
                "15.924375");
        OperationMovementEventEntity entity = entity(
                request,
                auditPrevious("388.5", "POSITION_DELTA"));

        assertEquals("USER_FILL", entity.getEconomicEventKind());
        assertEquals("REDUCE", entity.getEventType());
        assertDecimal("259.57", entity.getPreviousSizeQty());
        assertDecimal("209.57", entity.getResultingSizeQty());
        assertDecimal("-50", entity.getDeltaSizeQty());
        assertEquals("COMPLETE", economicBasisStatus(entity));
        assertEquals("AUTHORITATIVE_PREEXISTING_POSITION_BOUNDARY",
                entity.getRaw().path("economicBasisReason").asText());
        assertTrue(kafkaEvent(entity).metricEligible());
    }

    @Test
    void productionShortReductionStoresMagnitudesAndKeepsSignedSourceState()
            throws Exception {
        HyperliquidDeltaRequest request = productionRequest(
                "hyperliquid:trade:0xa9b95f2a2e7ef219021efc5c04c32761b8553bbd:1003441918511961",
                1003441918511961L,
                "BTCUSDT",
                "SHORT",
                "-171.572390",
                "-171.572190",
                "0.000200",
                "0.000200",
                "70018",
                "-0.095580",
                "0.006301");
        OperationMovementEventEntity entity = entity(
                request,
                auditPrevious("171.6", "POSITION_DELTA"));

        assertDecimal("171.572390", entity.getPreviousSizeQty());
        assertDecimal("171.572190", entity.getResultingSizeQty());
        assertDecimal("0.000200", entity.getDeltaSizeQty());
        assertDecimal("-171.572390",
                entity.getSourcePreviousPositionQuantity());
        assertDecimal("-171.572190",
                entity.getSourceResultingPositionQuantity());
        assertEquals("REDUCE", entity.getEventType());
        assertEquals("COMPLETE", economicBasisStatus(entity));
        assertTrue(kafkaEvent(entity).metricEligible());
    }

    @Test
    void increaseCarriesNoPositiveCloseQuantity() throws Exception {
        OperationMovementEventEntity entity = entity(
                request("RESIZE", "LONG", "OPEN", "10", "12", "2", "0", "LIVE_USER_FILL"),
                previous("10"));

        assertEquals("INCREASE", entity.getEventType());
        assertDecimal("2", entity.getDeltaSizeQty());
        assertDecimal("0", entity.getEffectiveCloseQty());
        assertEquals("COMPLETE", economicBasisStatus(entity));
    }

    @Test
    void closeEndsAtZeroAndClosesTheWholePreviousPosition() throws Exception {
        OperationMovementEventEntity entity = entity(
                request("CLOSE", "LONG", "CLOSED", "2", "0", "-2", "2", "LIVE_USER_FILL"),
                previous("2"));

        assertEquals("CLOSE", entity.getEventType());
        assertDecimal("0", entity.getResultingSizeQty());
        assertDecimal("2", entity.getEffectiveCloseQty());
        assertEquals("COMPLETE", economicBasisStatus(entity));
    }

    @Test
    void flipClosesPreviousAndKeepsOppositeRemainder() throws Exception {
        OperationMovementEventEntity entity = entity(
                request("FLIP", "SHORT", "OPEN", "10", "-5", "-15", "10", "LIVE_USER_FILL"),
                previous("10"));

        assertEquals("FLIP", entity.getEventType());
        assertDecimal("-15", entity.getDeltaSizeQty());
        assertDecimal("10", entity.getEffectiveCloseQty());
        assertDecimal("5", entity.getResultingSizeQty());
        assertDecimal("-5", entity.getSourceResultingPositionQuantity());
        assertEquals("COMPLETE", economicBasisStatus(entity));
    }

    @Test
    void contradictoryArithmeticIsNeverCompleteOrMetricEligible() throws Exception {
        OperationMovementEventEntity entity = entity(
                request("RESIZE", "LONG", "OPEN", "10", "7", "-2", "2", "LIVE_USER_FILL"),
                previous("10"));

        assertEquals("CONTRACT_INCONSISTENT", economicBasisStatus(entity));
        assertFalse(kafkaEvent(entity).metricEligible());
    }

    @Test
    void legacyUserFillRemainsReadableButIsNotComplete() throws Exception {
        HyperliquidDeltaRequest request = mapper.readValue(baseJson(
                "RESIZE", "LONG", "OPEN", "8", "2", "LIVE_USER_FILL", false),
                HyperliquidDeltaRequest.class);
        OperationMovementEventEntity entity = entity(request, previous("10"));

        assertEquals("LEGACY_CONTRACT", economicBasisStatus(entity));
        assertFalse(kafkaEvent(entity).metricEligible());
    }

    @Test
    void historicalReplayIsPersistedAsAuditOnly() throws Exception {
        OperationMovementEventEntity entity = entity(
                request("RESIZE", "LONG", "OPEN", "10", "8", "-2", "2", "HISTORICAL_REPLAY"),
                previous("10"));

        assertEquals("COMPLETE", economicBasisStatus(entity));
        assertFalse(kafkaEvent(entity).metricEligible());
        assertEquals("audit_only_excluded_from_joyas", kafkaEvent(entity).metricDecisionUse());
    }

    @Test
    void orderedGapRecoveryBecomesEligibleOnlyAfterContinuityIsReconciled() throws Exception {
        OperationMovementEventEntity entity = entity(
                request("RESIZE", "LONG", "OPEN", "10", "8", "-2", "2", "GAP_RECOVERY"),
                previous("10"));

        assertEquals("COMPLETE", economicBasisStatus(entity));
        assertTrue(kafkaEvent(entity).metricEligible());
        assertEquals("GAP_RECOVERY", kafkaEvent(entity).sourceDeliveryMode());
    }

    private HyperliquidDeltaRequest request(
            String deltaType,
            String side,
            String status,
            String before,
            String after,
            String signedExecution,
            String closeQty,
            String deliveryMode
    ) throws Exception {
        return mapper.readValue(baseJson(
                        deltaType, side, status, after, closeQty, deliveryMode, true)
                        .replace("\"__BEFORE__\"", before)
                        .replace("\"__AFTER__\"", after)
                        .replace("\"__SIGNED__\"", signedExecution)
                        .replace("\"__EXECUTION__\"", new BigDecimal(signedExecution).abs().toPlainString()),
                HyperliquidDeltaRequest.class);
    }

    private String baseJson(
            String deltaType,
            String side,
            String status,
            String sizeQty,
            String closeQty,
            String deliveryMode,
            boolean includeStateContract
    ) {
        String stateContract = includeStateContract ? """
                ,"sourcePreviousPositionQuantity":"__BEFORE__"
                ,"sourceResultingPositionQuantity":"__AFTER__"
                ,"sourceExecutionQuantity":"__EXECUTION__"
                ,"sourceSignedExecutionQuantity":"__SIGNED__"
                """ : "";
        return """
                {
                  "eventId":"fill-event-101",
                  "idempotencyKey":"fill-idempotency-101",
                  "eventType":"USER_FILL",
                  "deltaType":"%s",
                  "wallet":"0xabc",
                  "symbol":"BTCUSDT",
                  "side":"%s",
                  "status":"%s",
                  "sizeQty":%s,
                  "effectiveCloseQty":%s,
                  "effectiveEntryPrice":100,
                  "effectiveExitPrice":101,
                  "effectiveRealizedPnlUsd":2,
                  "normalizationStatus":"RECOVERED",
                  "sourceTs":1785585600000,
                  "economicEventKind":"USER_FILL",
                  "economicEventVersion":3,
                  "sourceEventId":"hyperliquid:user-fill:0xabc:101",
                  "sourceSequence":101,
                  "sourceFeeUsd":0.1,
                  "executionPriceBasis":"HYPERLIQUID_USER_FILL_PX",
                  "notionalBasis":"EXECUTED_QTY_X_PRICE",
                  "sourceEstimated":false,
                  "economicFingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                  "sourceDeliveryMode":"%s",
                  "sourceRecoveredAt":"2026-08-01T12:00:01Z"
                  %s
                }
                """.formatted(deltaType, side, status, sizeQty, closeQty, deliveryMode, stateContract);
    }

    private HyperliquidDeltaRequest productionRequest(
            String sourceEventId,
            long sourceSequence,
            String symbol,
            String side,
            String before,
            String after,
            String signedExecution,
            String closeQty,
            String executionPrice,
            String realizedPnl,
            String fee
    ) throws Exception {
        String json = baseJson(
                "RESIZE", side, "OPEN", after, closeQty,
                "LIVE_USER_FILL", true)
                .replace("\"__BEFORE__\"", before)
                .replace("\"__AFTER__\"", after)
                .replace("\"__SIGNED__\"", signedExecution)
                .replace("\"__EXECUTION__\"",
                        new BigDecimal(signedExecution).abs().toPlainString())
                .replace("fill-event-101", sourceEventId)
                .replace("fill-idempotency-101", sourceEventId)
                .replace("hyperliquid:user-fill:0xabc:101", sourceEventId)
                .replace("\"sourceSequence\":101",
                        "\"sourceSequence\":" + sourceSequence)
                .replace("\"symbol\":\"BTCUSDT\"",
                        "\"symbol\":\"" + symbol + "\"")
                .replace("\"effectiveEntryPrice\":100",
                        "\"effectiveEntryPrice\":" + executionPrice)
                .replace("\"effectiveExitPrice\":101",
                        "\"effectiveExitPrice\":" + executionPrice)
                .replace("\"effectiveRealizedPnlUsd\":2",
                        "\"effectiveRealizedPnlUsd\":" + realizedPnl)
                .replace("\"sourceFeeUsd\":0.1",
                        "\"sourceFeeUsd\":" + fee);
        return mapper.readValue(json, HyperliquidDeltaRequest.class);
    }

    private OperationMovementEventEntity entity(
            HyperliquidDeltaRequest request,
            OperationMovementEventEntity previous
    ) throws Exception {
        HyperliquidMappedDelta mapped = new HyperliquidDeltaOperacionMapper()
                .map(request, request.idempotencyKey());
        OperationMovementEventServiceImpl service = service();
        Method fromMapped = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "fromMappedDelta",
                HyperliquidMappedDelta.class,
                HyperliquidDirectCopyDispatchResult.class,
                String.class);
        fromMapped.setAccessible(true);
        OperationMovementEventRecordCommand command =
                (OperationMovementEventRecordCommand) fromMapped.invoke(
                        service,
                        mapped,
                        HyperliquidDirectCopyDispatchResult.ok(
                                0, 0, 0, 0, false, "authoritative_user_fill"),
                        "authoritative_user_fill");
        Method toEntity = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "toEntity",
                OperationMovementEventRecordCommand.class,
                OperationMovementEventEntity.class);
        toEntity.setAccessible(true);
        return (OperationMovementEventEntity) toEntity.invoke(service, command, previous);
    }

    private OperationMovementEventEntity previous(String resulting) {
        return OperationMovementEventEntity.builder()
                .resultingSizeQty(new BigDecimal(resulting))
                .sourceResultingPositionQuantity(new BigDecimal(resulting))
                .economicEventKind("USER_FILL")
                .economicBasisStatus("COMPLETE")
                .metricEligible(true)
                .sourceEstimated(false)
                .entryPrice(new BigDecimal("100"))
                .typeOperation("LONG")
                .eventTime(java.time.OffsetDateTime.parse("2026-08-01T11:59:59Z"))
                .sourceSequence(100L)
                .movementKey("movement|sha256:" + "0".repeat(64))
                .build();
    }

    private OperationMovementEventEntity auditPrevious(
            String resulting,
            String economicEventKind
    ) {
        return OperationMovementEventEntity.builder()
                .resultingSizeQty(new BigDecimal(resulting))
                .economicEventKind(economicEventKind)
                .economicBasisStatus("NOT_APPLICABLE")
                .metricEligible(false)
                .sourceEstimated(true)
                .entryPrice(new BigDecimal("700"))
                .typeOperation("LONG")
                .eventTime(java.time.OffsetDateTime.parse(
                        "2026-08-22T05:02:00.035Z"))
                .sourceSequence(970505954175000L)
                .movementKey("movement|sha256:" + "1".repeat(64))
                .build();
    }

    private String economicBasisStatus(OperationMovementEventEntity entity) throws Exception {
        Method method = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "economicBasisStatus", OperationMovementEventEntity.class);
        method.setAccessible(true);
        return (String) method.invoke(service(), entity);
    }

    private MetricMovementPersistedEvent kafkaEvent(OperationMovementEventEntity entity)
            throws Exception {
        MetricMovementOutboxServiceImpl outbox =
                new MetricMovementOutboxServiceImpl(null, mapper);
        Method method = MetricMovementOutboxServiceImpl.class.getDeclaredMethod(
                "toEvent", OperationMovementEventEntity.class);
        method.setAccessible(true);
        return (MetricMovementPersistedEvent) method.invoke(outbox, entity);
    }

    private OperationMovementEventServiceImpl service() {
        OperationMovementEventRepository repository =
                (OperationMovementEventRepository) Proxy.newProxyInstance(
                        OperationMovementEventRepository.class.getClassLoader(),
                        new Class<?>[]{OperationMovementEventRepository.class},
                        (proxy, method, args) -> defaultValue(method.getReturnType()));
        MetricMovementOutboxService outbox = ignored -> { };
        return new OperationMovementEventServiceImpl(
                repository,
                mapper,
                outbox,
                new NoopTransactionManager(),
                new SimpleMeterRegistry(),
                false,
                1,
                1);
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) return null;
        if (type == boolean.class) return false;
        if (type == int.class || type == short.class || type == byte.class || type == long.class) return 0;
        if (type == float.class || type == double.class) return 0.0;
        return null;
    }

    private void assertDecimal(String expected, BigDecimal actual) {
        assertEquals(0, new BigDecimal(expected).compareTo(actual));
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
