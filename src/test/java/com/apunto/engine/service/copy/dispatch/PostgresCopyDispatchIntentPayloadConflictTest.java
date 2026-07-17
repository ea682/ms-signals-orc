package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import com.apunto.engine.repository.CopyDispatchIntentRepository;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgresCopyDispatchIntentPayloadConflictTest {

    @Test
    void differentHashIsAuditedAndNeverAuthorizedForSend() {
        CopyDispatchIntentEntity existing = existing();
        CopyDispatchIntentRepository repository = repository(existing);
        EntityManager entityManager = proxy(EntityManager.class, (method, args) -> null);
        CapturingConflictStore conflicts = new CapturingConflictStore();
        CopyDispatchPayloadConflictRecorder recorder = new CopyDispatchPayloadConflictRecorder(
                conflicts, new CopyDispatchPayloadSnapshotFactory(),
                Clock.fixed(Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC));

        PostgresCopyDispatchIntentStore store = new PostgresCopyDispatchIntentStore(
                repository, entityManager, new ObjectMapper(), new SimpleMeterRegistry(), recorder);

        CopyDispatchPermit result = store.acquire(incoming(existing.getIdempotencyKey()));

        assertEquals(CopyDispatchPermit.Decision.CONFLICT, result.decision());
        assertEquals(1, conflicts.records.size());
        assertEquals(existing.getId(), conflicts.manualReviewIntentId);
    }

    @Test
    void legacyHashWithOnlyDerivedEconomicsDriftKeepsTerminalRejectionWithoutConflict() {
        CopyDispatchIntentEntity existing = existing();
        OperationDto operation = executableOperation("1");
        existing.setStatus("REJECTED");
        existing.setLastErrorCode("BINANCE_ORDER_REJECTED");
        existing.setClientOrderId(operation.getClientOrderId());
        existing.setRequestHash(legacyHash(operation, existing));
        CapturingConflictStore conflicts = new CapturingConflictStore();
        PostgresCopyDispatchIntentStore store = new PostgresCopyDispatchIntentStore(
                repository(existing), proxy(EntityManager.class, (method, args) -> null),
                new ObjectMapper(), new SimpleMeterRegistry(),
                new CopyDispatchPayloadConflictRecorder(conflicts, new CopyDispatchPayloadSnapshotFactory(),
                        Clock.fixed(Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC)));

        CopyDispatchRequest replay = equivalentReplay(existing.getIdempotencyKey(), operation);
        CopyDispatchPermit result = store.acquire(replay);

        assertEquals(CopyDispatchPermit.Decision.REJECTED, result.decision());
        assertEquals(0, conflicts.records.size());
    }

    @Test
    void legacyHashCompatibilityDoesNotHideQuantityChange() {
        CopyDispatchIntentEntity existing = existing();
        OperationDto original = executableOperation("1");
        existing.setClientOrderId(original.getClientOrderId());
        existing.setRequestHash(legacyHash(original, existing));
        CapturingConflictStore conflicts = new CapturingConflictStore();
        PostgresCopyDispatchIntentStore store = new PostgresCopyDispatchIntentStore(
                repository(existing), proxy(EntityManager.class, (method, args) -> null),
                new ObjectMapper(), new SimpleMeterRegistry(),
                new CopyDispatchPayloadConflictRecorder(conflicts, new CopyDispatchPayloadSnapshotFactory(),
                        Clock.fixed(Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC)));
        OperationDto changed = executableOperation("2");
        String changedHash = new CopyIdempotencyKeyFactory().hashPayload(
                CopyDispatchRequestFingerprint.canonical(changed, new BigDecimal("2"), null, false));
        CopyDispatchRequest replay = new CopyDispatchRequest(existing.getIdempotencyKey(),
                new CopyDispatchIdentity("user-1", 55L, "LIVE", "MOVEMENT_ALL",
                        "ALL", "ALL", "event-1", "OPEN"),
                changed, "0xabc", "BTCUSDC", "BUY", "LONG", false,
                new BigDecimal("2"), new BigDecimal("40"), new BigDecimal("200"),
                new BigDecimal("100"), 5, null, false, "SOURCE_OPEN", changedHash, "trace-1");

        CopyDispatchPermit result = store.acquire(replay);

        assertEquals(CopyDispatchPermit.Decision.CONFLICT, result.decision());
        assertEquals(1, conflicts.records.size());
    }

    private CopyDispatchIntentEntity existing() {
        return CopyDispatchIntentEntity.builder()
                .id(UUID.randomUUID()).idempotencyKey("a".repeat(64)).idUser("user-1")
                .userCopyAllocationId(55L).executionMode("LIVE").walletId("0xabc")
                .strategyCode("MOVEMENT_ALL").scopeType("ALL").scopeValue("ALL")
                .sourceEventId("event-1").sourceEventType("SOURCE_OPEN").copyIntent("OPEN")
                .idOrderOrigin("origin-1").symbol("BTCUSDC").side("BUY").positionSide("LONG")
                .reduceOnly(false).requestedQty(BigDecimal.ONE).requestedMarginUsd(new BigDecimal("20"))
                .requestedNotionalUsd(new BigDecimal("100")).referencePrice(new BigDecimal("100"))
                .requestedLeverage(5).clientOrderId("ct_existing").requestHash("old-hash")
                .status("NEW").createdAt(OffsetDateTime.now()).updatedAt(OffsetDateTime.now()).build();
    }

    private CopyDispatchRequest incoming(String key) {
        OperationDto operation = OperationDto.builder().originId("origin-1").symbol("BTCUSDC")
                .quantity("2").clientOrderId("ct_incoming").userId("user-1").walletId("0xabc").build();
        return new CopyDispatchRequest(key,
                new CopyDispatchIdentity("user-1", 55L, "LIVE", "MOVEMENT_ALL",
                        "ALL", "ALL", "event-1", "OPEN"),
                operation, "0xabc", "BTCUSDC", "BUY", "LONG", false,
                new BigDecimal("2"), new BigDecimal("20"), new BigDecimal("200"),
                new BigDecimal("100"), 5, null, true, "SOURCE_OPEN", "new-hash", "trace-1");
    }

    private CopyDispatchRequest equivalentReplay(String key, OperationDto operation) {
        String requestHash = new CopyIdempotencyKeyFactory().hashPayload(
                CopyDispatchRequestFingerprint.canonical(operation, BigDecimal.ONE, null, false));
        return new CopyDispatchRequest(key,
                new CopyDispatchIdentity("user-1", 55L, "LIVE", "MOVEMENT_ALL",
                        "ALL", "ALL", "event-1", "OPEN"),
                operation, "0xabc", "BTCUSDC", "BUY", "LONG", false,
                BigDecimal.ONE, new BigDecimal("21"), new BigDecimal("105"),
                new BigDecimal("105"), 5, null, false, "SOURCE_OPEN", requestHash, "trace-1");
    }

    private OperationDto executableOperation(String qty) {
        return OperationDto.builder().originId("origin-1").symbol("BTCUSDC")
                .side(Side.BUY).positionSide(PositionSide.LONG).type(OrderType.MARKET)
                .quantity(qty).leverage(5).reduceOnly(false).configureAccountSettings(false)
                .clientOrderId("ct_existing").userId("user-1").walletId("0xabc").build();
    }

    private String legacyHash(OperationDto operation, CopyDispatchIntentEntity existing) {
        String payload = String.join("|",
                operation.getSymbol(), operation.getSide().name(), operation.getPositionSide().name(),
                operation.getType().name(), "1", "20", "100", "100", "5", "",
                "false", "false", operation.getClientOrderId());
        return new CopyIdempotencyKeyFactory().hashPayload(payload);
    }

    private CopyDispatchIntentRepository repository(CopyDispatchIntentEntity existing) {
        return proxy(CopyDispatchIntentRepository.class, (method, args) -> switch (method) {
            case "insertIfAbsent" -> 0;
            case "findByIdempotencyKey" -> Optional.of(existing);
            default -> throw new UnsupportedOperationException("Unexpected repository call: " + method);
        });
    }

    @SuppressWarnings("unchecked")
    private <T> T proxy(Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (instance, method, args) -> invocation.invoke(method.getName(), args));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(String method, Object[] args);
    }

    private static final class CapturingConflictStore implements CopyDispatchPayloadConflictStore {
        private final List<CopyDispatchPayloadConflictRecord> records = new ArrayList<>();
        private UUID manualReviewIntentId;

        @Override
        public void upsert(CopyDispatchPayloadConflictRecord record) {
            records.add(record);
        }

        @Override
        public boolean markManualReview(UUID intentId, String existingHash, String incomingHash) {
            manualReviewIntentId = intentId;
            return true;
        }
    }
}
