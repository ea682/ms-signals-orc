package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDispatchPayloadConflictRecorderTest {

    @Test
    void nonTerminalConflictIsPersistedAndMovedToManualReview() {
        CapturingStore store = new CapturingStore();
        CopyDispatchPayloadConflictRecorder recorder = recorder(store);
        CopyDispatchIntentEntity intent = intent("NEW");

        CopyDispatchPayloadConflictRecord result = recorder.record(intent, request());

        assertTrue(result.manualReviewRequired());
        assertEquals(1, store.records.size());
        assertEquals(1, store.manualReviewCount);
    }

    @Test
    void persistedIntentRemainsTerminalButConflictIsStillAudited() {
        CapturingStore store = new CapturingStore();
        CopyDispatchPayloadConflictRecorder recorder = recorder(store);

        CopyDispatchPayloadConflictRecord result = recorder.record(intent("PERSISTED"), request());

        assertFalse(result.manualReviewRequired());
        assertEquals(1, store.records.size());
        assertEquals(0, store.manualReviewCount);
    }

    private CopyDispatchPayloadConflictRecorder recorder(CapturingStore store) {
        return new CopyDispatchPayloadConflictRecorder(store, new CopyDispatchPayloadSnapshotFactory(),
                Clock.fixed(Instant.parse("2026-07-13T12:00:00Z"), ZoneOffset.UTC));
    }

    private CopyDispatchIntentEntity intent(String status) {
        return CopyDispatchIntentEntity.builder()
                .id(UUID.randomUUID()).idempotencyKey("a".repeat(64)).idUser("user-1")
                .userCopyAllocationId(55L).executionMode("LIVE").walletId("0xabc")
                .strategyCode("MOVEMENT_ALL").scopeType("ALL").scopeValue("ALL")
                .sourceEventId("event-1").copyIntent("OPEN").symbol("BTCUSDC")
                .side("BUY").positionSide("LONG").reduceOnly(false)
                .requestedQty(BigDecimal.ONE).requestedMarginUsd(new BigDecimal("20"))
                .requestedNotionalUsd(new BigDecimal("100")).referencePrice(new BigDecimal("100"))
                .requestedLeverage(5).clientOrderId("ct_existing").requestHash("old-hash")
                .status(status).build();
    }

    private CopyDispatchRequest request() {
        OperationDto operation = OperationDto.builder().symbol("BTCUSDC").quantity("2")
                .clientOrderId("ct_incoming").userId("user-1").walletId("0xabc").build();
        return new CopyDispatchRequest("a".repeat(64),
                new CopyDispatchIdentity("user-1", 55L, "LIVE", "MOVEMENT_ALL",
                        "ALL", "ALL", "event-1", "OPEN"),
                operation, "0xabc", "BTCUSDC", "BUY", "LONG", false,
                new BigDecimal("2"), new BigDecimal("20"), new BigDecimal("200"),
                new BigDecimal("100"), 5, null, true, "SOURCE_OPEN", "new-hash", "trace-1");
    }

    private static final class CapturingStore implements CopyDispatchPayloadConflictStore {
        private final List<CopyDispatchPayloadConflictRecord> records = new ArrayList<>();
        private int manualReviewCount;
        @Override public void upsert(CopyDispatchPayloadConflictRecord record) { records.add(record); }
        @Override public boolean markManualReview(UUID intentId, String existingHash, String incomingHash) {
            manualReviewCount++;
            return true;
        }
    }
}
