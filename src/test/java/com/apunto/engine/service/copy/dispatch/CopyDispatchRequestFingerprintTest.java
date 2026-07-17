package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.OperationDto;
import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class CopyDispatchRequestFingerprintTest {

    @Test
    void marketEconomicsDriftDoesNotChangeExecutableFingerprint() {
        OperationDto first = market("0.006", "62000", "74.40", "372.00");
        OperationDto replay = market("0.006", "62500", "75.00", "375.00");

        assertEquals(
                CopyDispatchRequestFingerprint.canonical(first, new BigDecimal("0.006"), 7, false),
                CopyDispatchRequestFingerprint.canonical(replay, new BigDecimal("0.006"), 7, false));
    }

    @Test
    void changedQuantityChangesExecutableFingerprint() {
        OperationDto first = market("0.006", "62000", "74.40", "372.00");
        OperationDto replay = market("0.005", "62000", "62.00", "310.00");

        assertNotEquals(
                CopyDispatchRequestFingerprint.canonical(first, new BigDecimal("0.006"), 7, false),
                CopyDispatchRequestFingerprint.canonical(replay, new BigDecimal("0.005"), 7, false));
    }

    @Test
    void changedLimitPriceChangesExecutableFingerprint() {
        OperationDto first = market("0.006", "62000", "74.40", "372.00");
        first.setType(OrderType.LIMIT);
        first.setPrice("62000.00");
        first.setTimeInForce("GTC");
        OperationDto replay = market("0.006", "62000", "74.40", "372.00");
        replay.setType(OrderType.LIMIT);
        replay.setPrice("62100");
        replay.setTimeInForce("GTC");

        assertNotEquals(
                CopyDispatchRequestFingerprint.canonical(first, new BigDecimal("0.006"), 7, false),
                CopyDispatchRequestFingerprint.canonical(replay, new BigDecimal("0.006"), 7, false));
    }

    private OperationDto market(String qty, String ref, String margin, String notional) {
        return OperationDto.builder()
                .symbol("BTCUSDC")
                .side(Side.BUY)
                .positionSide(PositionSide.LONG)
                .type(OrderType.MARKET)
                .quantity(qty)
                .leverage(5)
                .reduceOnly(false)
                .configureAccountSettings(false)
                .clientOrderId("cpI_8ba5313b4659271fddfc5559cb597525")
                .referencePrice(new BigDecimal(ref))
                .requestedMarginUsd(new BigDecimal(margin))
                .requestedNotionalUsd(new BigDecimal(notional))
                .build();
    }
}
