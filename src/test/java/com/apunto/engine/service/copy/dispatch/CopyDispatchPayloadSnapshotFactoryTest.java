package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.entity.CopyDispatchIntentEntity;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDispatchPayloadSnapshotFactoryTest {

    private final CopyDispatchPayloadSnapshotFactory factory = new CopyDispatchPayloadSnapshotFactory();

    @Test
    void producesDeterministicFieldDiffWithoutCredentials() {
        CopyDispatchIntentEntity existing = CopyDispatchIntentEntity.builder()
                .id(UUID.randomUUID()).idempotencyKey("a".repeat(64)).idUser("user-1")
                .userCopyAllocationId(55L).executionMode("LIVE").walletId("0xabc")
                .strategyCode("MOVEMENT_ALL").scopeType("ALL").scopeValue("ALL")
                .sourceEventId("event-1").copyIntent("OPEN").symbol("BTCUSDC")
                .side("BUY").positionSide("LONG").reduceOnly(false)
                .requestedQty(new BigDecimal("1")).requestedMarginUsd(new BigDecimal("20"))
                .requestedNotionalUsd(new BigDecimal("100")).referencePrice(new BigDecimal("100"))
                .requestedLeverage(5).userMaxConcurrentPositions(7)
                .clientOrderId("ct_existing").requestHash("old-hash")
                .status("NEW").build();
        CopyDispatchRequest incoming = request(new BigDecimal("2"), "api-secret-value");

        CopyDispatchPayloadComparison comparison = factory.compare(existing, incoming);

        assertEquals(Map.of("existing", "1", "incoming", "2"),
                comparison.fieldDiff().get("requestedQty"));
        String serialized = comparison.existingPayload().toString()
                + comparison.incomingPayload() + comparison.fieldDiff();
        assertFalse(serialized.contains("api-secret-value"));
        assertFalse(serialized.toLowerCase().contains("apikey"));
        assertFalse(serialized.toLowerCase().contains("secret"));
        assertTrue(comparison.fieldDiff().containsKey("clientOrderId"));
        assertEquals(Map.of("existing", 7, "incoming", 9),
                comparison.fieldDiff().get("userMaxConcurrentPositions"));
    }

    private CopyDispatchRequest request(BigDecimal qty, String secret) {
        OperationDto operation = OperationDto.builder()
                .symbol("BTCUSDC").quantity(qty.toPlainString()).clientOrderId("ct_incoming")
                .userId("user-1").walletId("0xabc").apiKey("api-key-value").secret(secret)
                .build();
        CopyDispatchIdentity identity = new CopyDispatchIdentity(
                "user-1", 55L, "LIVE", "MOVEMENT_ALL", "ALL", "ALL",
                "event-1", "OPEN");
        return new CopyDispatchRequest(
                "a".repeat(64), identity, operation, "0xabc", "BTCUSDC",
                "BUY", "LONG", false, qty, new BigDecimal("20"), new BigDecimal("200"),
                new BigDecimal("100"), 5, 9, true, "SOURCE_OPEN", "new-hash", "trace-1");
    }
}
