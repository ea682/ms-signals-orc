package com.apunto.engine.hyperliquid.mapper;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class HyperliquidZeroHashIdentityTest {

    @Test
    void productionZeroHashUsesExchangeWalletAndTidAndRedeliveryIsStable() throws Exception {
        JsonNode fixture;
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        try (var input = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-e-zero-hash-identity.json")) {
            assertNotNull(input);
            fixture = objectMapper.readTree(input);
        }
        HyperliquidDeltaOperacionMapper mapper = new HyperliquidDeltaOperacionMapper();
        HyperliquidMappedDelta first = map(mapper, objectMapper, fixture.path("first"));
        HyperliquidMappedDelta redelivery = map(mapper, objectMapper, fixture.path("redelivery"));
        HyperliquidMappedDelta distinctTid = map(mapper, objectMapper, fixture.path("distinctTid"));

        assertEquals(first.idempotencyKey(), redelivery.idempotencyKey());
        assertNotEquals(first.idempotencyKey(), distinctTid.idempotencyKey());
        assertEquals(
                "hyperliquid:trade:wallet_sanitized_e001:23131303191059",
                first.idempotencyKey());
        assertFalse(first.idempotencyKey().contains(
                fixture.path("evidence").path("hash").asText()));
    }

    @Test
    void zeroHashNormalizationIsObservableWithoutWalletTag() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        JsonNode fixture;
        try (var input = getClass().getResourceAsStream(
                "/fixtures/production/anomaly-e-zero-hash-identity.json")) {
            assertNotNull(input);
            fixture = objectMapper.readTree(input);
        }
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        HyperliquidDeltaOperacionMapper mapper =
                new HyperliquidDeltaOperacionMapper(registry);

        map(mapper, objectMapper, fixture.path("first"));

        assertEquals(1.0d, registry.find("zero_hash_identity_total")
                .tag("source", "direct_ingest").counter().count());
    }

    private HyperliquidMappedDelta map(
            HyperliquidDeltaOperacionMapper mapper,
            ObjectMapper objectMapper,
            JsonNode node
    ) throws Exception {
        HyperliquidDeltaRequest request =
                objectMapper.treeToValue(node, HyperliquidDeltaRequest.class);
        return mapper.map(request, request.idempotencyKey());
    }
}
