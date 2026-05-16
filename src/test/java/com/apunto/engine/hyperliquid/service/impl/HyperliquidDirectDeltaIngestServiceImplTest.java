package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class HyperliquidDirectDeltaIngestServiceImplTest {

    @Test
    void adjustmentDedupeKeyIgnoresNoisyIdempotencyKeyWhenSourceStateIsTheSame() {
        HyperliquidMappedDelta first = mappedAdjustment("idempotency-a", "RESIZE", "100.0000", "4210.600", 1778905103699L);
        HyperliquidMappedDelta second = mappedAdjustment("idempotency-b", "RESIZE", "100.0", "4210.6000", 1778905103699L);

        assertEquals(
                HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(first),
                HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(second)
        );
    }

    @Test
    void adjustmentDedupeKeyChangesWhenResultingPositionStateChanges() {
        HyperliquidMappedDelta first = mappedAdjustment("idempotency-a", "RESIZE", "100", "4210.60", 1778905103699L);
        HyperliquidMappedDelta second = mappedAdjustment("idempotency-b", "RESIZE", "101", "4210.60", 1778905103699L);

        assertNotEquals(
                HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(first),
                HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(second)
        );
    }

    @Test
    void openDedupeKeyKeepsPublisherIdempotencyKey() {
        HyperliquidMappedDelta open = mappedAdjustment("open-idempotency", "OPEN", "100", "4210.60", 1778905103699L);

        assertEquals("open-idempotency", HyperliquidDirectDeltaIngestServiceImpl.buildDedupeKey(open));
    }

    private HyperliquidMappedDelta mappedAdjustment(
            String idempotencyKey,
            String deltaType,
            String sizeQty,
            String notionalUsd,
            long sourceTs
    ) {
        HyperliquidDeltaRequest request = new HyperliquidDeltaRequest(
                null,
                idempotencyKey,
                null,
                deltaType,
                "hyperliquid",
                "0xabc",
                null,
                "HYPE",
                "SHORT",
                "OPEN",
                new BigDecimal(sizeQty),
                null,
                new BigDecimal(notionalUsd),
                BigDecimal.TEN,
                new BigDecimal("42.106"),
                new BigDecimal("42.1060"),
                null,
                sourceTs,
                null,
                null,
                null,
                null,
                null,
                null,
                false
        );
        return new HyperliquidMappedDelta(
                idempotencyKey,
                "hyperliquid-position:0xabc:HYPEUSDT:SHORT",
                "0xabc",
                "HYPEUSDT",
                "SHORT",
                deltaType,
                new OperacionEvent(OperacionEvent.Tipo.ABIERTA, null, deltaType),
                request
        );
    }
}
