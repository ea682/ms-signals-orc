package com.apunto.engine.service.impl;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuthoritativeMovementIdentityRegressionTest {

    @Test
    void sameTidRedeliveryHasSameIdentityAndDistinctTidDoesNotCollide() throws Exception {
        Class<?> identityType = Class.forName(
                "com.apunto.engine.service.movement.AuthoritativeMovementIdentity");
        Method userFill = identityType.getMethod(
                "userFill", String.class, String.class, long.class, String.class);

        String first = (String) userFill.invoke(
                null, "hyperliquid", "0xwallet", 8101L, "0xhash-a");
        String redelivery = (String) userFill.invoke(
                null, "HYPERLIQUID", "0xWALLET", 8101L, "0xhash-a");
        String distinct = (String) userFill.invoke(
                null, "hyperliquid", "0xwallet", 8102L, "0xhash-b");

        assertEquals(first, redelivery);
        assertNotEquals(first, distinct);
        assertTrue(first.contains("hyperliquid"));
        assertTrue(first.contains("0xwallet"));
        assertTrue(first.contains("8101"));
        assertTrue(first.contains("0xhash-a"));
    }

    @Test
    void sourceIdentitySeparatesEqualEconomicProjections() throws Exception {
        Class<?> identityType = Class.forName(
                "com.apunto.engine.service.movement.AuthoritativeMovementIdentity");
        Method sourceAware = identityType.getMethod(
                "sourceAwareMovementMaterial",
                String.class,
                String.class,
                Long.class
        );

        String first = (String) sourceAware.invoke(
                null, "same-projection",
                "hyperliquid:user-fill:wallet:8101:0xhash-a", 8101L);
        String second = (String) sourceAware.invoke(
                null, "same-projection",
                "hyperliquid:user-fill:wallet:8102:0xhash-b", 8102L);

        assertNotEquals(first, second);
        assertTrue(first.contains("sourceEventId="));
        assertTrue(first.contains("sourceSequence=8101"));
    }

    @Test
    void serviceMovementMaterialIncludesSourceEventAndSequenceWhenAvailable() throws Exception {
        HyperliquidDeltaRequest request = new ObjectMapper()
                .findAndRegisterModules()
                .readValue("""
                        {
                          "eventId": "fixture-event",
                          "idempotencyKey": "legacy-key",
                          "eventType": "HYPERLIQUID_POSITION_RESIZED",
                          "deltaType": "RESIZE",
                          "platform": "hyperliquid",
                          "wallet": "0xwallet",
                          "symbol": "ETHUSDT",
                          "side": "LONG",
                          "status": "OPEN",
                          "sizeQty": 1,
                          "sourceTs": 1784783933601,
                          "economicEventKind": "USER_FILL",
                          "sourceEventId": "hyperliquid:user-fill:0xwallet:8101:0xhash-a",
                          "sourceSequence": 8101,
                          "sourceEstimated": false
                        }
                        """, HyperliquidDeltaRequest.class);
        HyperliquidMappedDelta mapped = new HyperliquidMappedDelta(
                "legacy-key",
                "position-key",
                "0xwallet",
                "ETHUSDT",
                "LONG",
                "RESIZE",
                null,
                request
        );
        Method materialMethod = OperationMovementEventServiceImpl.class
                .getDeclaredMethod(
                        "canonicalMovementPayload",
                        HyperliquidMappedDelta.class,
                        OffsetDateTime.class
                );
        materialMethod.setAccessible(true);

        String material = (String) materialMethod.invoke(
                nullSafeService(),
                mapped,
                OffsetDateTime.parse("2026-07-23T05:18:53.601Z")
        );

        assertTrue(material.contains(
                "sourceEventId=hyperliquid:user-fill:0xwallet:8101:0xhash-a"));
        assertTrue(material.contains("sourceSequence=8101"));
    }

    private OperationMovementEventServiceImpl nullSafeService() {
        try {
            Method factory = OperationMovementEconomicNormalizationTest.class
                    .getDeclaredMethod("service");
            factory.setAccessible(true);
            return (OperationMovementEventServiceImpl) factory.invoke(
                    new OperationMovementEconomicNormalizationTest());
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }
}
