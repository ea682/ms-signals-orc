package com.apunto.engine.hyperliquid.dto;

import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HyperliquidDeltaRequestV3ContractTest {

    @Test
    void deserializesAuthoritativeEquityAndSourcePositionContract() throws Exception {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        HyperliquidDeltaRequest request = mapper.readValue("""
                {
                  "deltaType":"OPEN",
                  "wallet":"0xabc",
                  "symbol":"HYPEUSDT",
                  "side":"LONG",
                  "sourceEventId":"source-1",
                  "sourceAccountEquityUsd":500000,
                  "equityObservedAt":"2026-07-13T12:00:00Z",
                  "equitySource":"HYPERLIQUID_CLEARINGHOUSE_MARGIN_SUMMARY",
                  "equityFreshnessMs":2000,
                  "equityQuality":"KNOWN",
                  "sourceSnapshotVersion":42,
                  "sourcePositionNotionalUsd":100000,
                  "sourcePositionQuantity":50,
                  "sourceMarkPrice":2000,
                  "sourceEntryPrice":1900,
                  "sourceLeverage":10,
                  "sourceSide":"LONG",
                  "sourcePortfolioSnapshotVersion":42,
                  "sourcePortfolioComplete":true,
                  "sourcePortfolioPositions":[
                    {
                      "sourcePositionKey":"hyperliquid-position:0xabc:HYPEUSDT:LONG",
                      "sourceSymbol":"HYPEUSDT",
                      "sourceSide":"LONG",
                      "sourcePositionQuantity":50,
                      "sourcePositionNotionalUsd":100000,
                      "sourceMarkPrice":2000,
                      "sourceEntryPrice":1900,
                      "sourceLeverage":10,
                      "sourceSnapshotVersion":42,
                      "sourceTs":1783944000000,
                      "estimated":false
                    }
                  ]
                }
                """, HyperliquidDeltaRequest.class);

        assertEquals(0, new BigDecimal("500000").compareTo(request.sourceAccountEquityUsd()));
        assertEquals(Instant.parse("2026-07-13T12:00:00Z"), request.equityObservedAt());
        assertEquals("HYPERLIQUID_CLEARINGHOUSE_MARGIN_SUMMARY", request.equitySource());
        assertEquals(2000L, request.equityFreshnessMs());
        assertEquals("KNOWN", request.equityQuality());
        assertEquals(42L, request.sourceSnapshotVersion());
        assertEquals(0, new BigDecimal("100000").compareTo(request.sourcePositionNotionalUsd()));
        assertEquals(0, new BigDecimal("50").compareTo(request.sourcePositionQuantity()));
        assertEquals(0, new BigDecimal("2000").compareTo(request.sourceMarkPrice()));
        assertEquals("LONG", request.sourceSide());
        assertEquals(42L, request.sourcePortfolioSnapshotVersion());
        assertEquals(Boolean.TRUE, request.sourcePortfolioComplete());
        assertEquals(1, request.sourcePortfolioPositions().size());
        assertEquals("HYPEUSDT", request.sourcePortfolioPositions().getFirst().sourceSymbol());
        assertEquals(42L, request.sourcePortfolioPositions().getFirst().sourceSnapshotVersion());
    }

    @Test
    void durableOriginRebindingPreservesTheAuthoritativeSizingSnapshot() throws Exception {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        HyperliquidDeltaRequest request = mapper.readValue("""
                {
                  "deltaType":"OPEN",
                  "wallet":"0xabc",
                  "symbol":"HYPEUSDT",
                  "side":"LONG",
                  "idempotencyKey":"event-42",
                  "sourceEventId":"source-42",
                  "sourceTs":1783944000000,
                  "sizeQty":50,
                  "positionNotionalUsd":100000,
                  "entryPrice":1900,
                  "markPrice":2000,
                  "sourceAccountEquityUsd":500000,
                  "equityObservedAt":"2026-07-13T12:00:00Z",
                  "equitySource":"HYPERLIQUID_CLEARINGHOUSE_MARGIN_SUMMARY",
                  "equityFreshnessMs":2000,
                  "equityQuality":"KNOWN",
                  "sourceSnapshotVersion":42,
                  "sourcePositionNotionalUsd":100000
                }
                """, HyperliquidDeltaRequest.class);

        HyperliquidMappedDelta rebound = new HyperliquidDeltaOperacionMapper()
                .map(request, null)
                .withOriginId(UUID.randomUUID());

        assertEquals(0, new BigDecimal("500000").compareTo(
                rebound.event().getOperacion().getSourceAccountEquityUsd()));
        assertEquals(Instant.parse("2026-07-13T12:00:00Z"),
                rebound.event().getOperacion().getEquityObservedAt());
        assertEquals("HYPERLIQUID_CLEARINGHOUSE_MARGIN_SUMMARY",
                rebound.event().getOperacion().getEquitySource());
        assertEquals(2000L, rebound.event().getOperacion().getEquityFreshnessMs());
        assertEquals("KNOWN", rebound.event().getOperacion().getEquityQuality());
        assertEquals(42L, rebound.event().getOperacion().getSourceSnapshotVersion());
        assertEquals("source-42", rebound.event().getOperacion().getSourceEventId());
        assertEquals(0, new BigDecimal("100000").compareTo(
                rebound.event().getOperacion().getSourcePositionNotionalUsd()));
    }

    @Test
    void legacyAndNewReplicaHeadersConvergeToCanonicalSourceTradeIdentity() throws Exception {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        HyperliquidDeltaRequest request = mapper.readValue("""
                {
                  "deltaType":"RESIZE",
                  "wallet":"0xAbC",
                  "symbol":"BTCUSDT",
                  "side":"SHORT",
                  "sourceTs":1710000000000,
                  "sizeQty":2,
                  "externalId":"0xabc|BTCUSDT|RESIZE|1710000000000|BTC|trade-77|hash-77"
                }
                """, HyperliquidDeltaRequest.class);
        String legacyHeader = "hyperliquid:0xabc:BTCUSDT:SHORT:RESIZE:"
                + request.externalId();

        HyperliquidMappedDelta mapped = new HyperliquidDeltaOperacionMapper()
                .map(request, legacyHeader);

        assertEquals(
                "hyperliquid:trade:0xabc:BTCUSDT:1710000000000|BTC|trade-77|hash-77",
                mapped.idempotencyKey());
    }
}
