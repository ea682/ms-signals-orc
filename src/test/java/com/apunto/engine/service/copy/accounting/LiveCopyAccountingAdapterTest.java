package com.apunto.engine.service.copy.accounting;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.shared.enums.PositionSide;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveCopyAccountingAdapterTest {

    private final LiveCopyAccountingAdapter adapter = new LiveCopyAccountingAdapter(
            new CopyPositionAccountingService(new PositionDeltaClassifier())
    );

    @Test
    void liveOpenLongAcceptedWithoutRealizedPnl() {
        CopyAccountingResult result = apply(PositionSide.LONG, "0", "1", null, "100", "1");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.OPEN, result.deltaType());
        assertBd("1", result.deltaAddedQty());
        assertBd("0", result.netRealizedPnlUsd());
        assertTrue(result.positionRemainsOpen());
    }

    @Test
    void liveOpenShortAcceptedWithoutRealizedPnl() {
        CopyAccountingResult result = apply(PositionSide.SHORT, "0", "2", null, "100", "2");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.OPEN, result.deltaType());
        assertBd("2", result.deltaAddedQty());
        assertBd("0", result.netRealizedPnlUsd());
        assertTrue(result.positionRemainsOpen());
    }

    @Test
    void liveIncreaseLongRecomputesAverageEntryWithoutPnl() {
        CopyAccountingResult result = apply(PositionSide.LONG, "1", "3", "100", "130", "2");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.INCREASE, result.deltaType());
        assertBd("2", result.deltaAddedQty());
        assertBd("120.000000000000", result.newAvgEntryPrice());
        assertBd("0", result.netRealizedPnlUsd());
    }

    @Test
    void liveReduceLongGainRealizesPositivePnl() {
        CopyAccountingResult result = apply(PositionSide.LONG, "10", "4", "100", "110", "6");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.REDUCE, result.deltaType());
        assertBd("6", result.deltaClosedQty());
        assertBd("60.000000000000", result.grossRealizedPnlUsd());
        assertTrue(result.positionRemainsOpen());
    }

    @Test
    void liveReduceLongLossRealizesNegativePnl() {
        CopyAccountingResult result = apply(PositionSide.LONG, "10", "4", "100", "90", "6");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.REDUCE, result.deltaType());
        assertBd("-60.000000000000", result.grossRealizedPnlUsd());
    }

    @Test
    void liveReduceShortGainRealizesPositivePnl() {
        CopyAccountingResult result = apply(PositionSide.SHORT, "10", "4", "100", "90", "6");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.REDUCE, result.deltaType());
        assertBd("60.000000000000", result.grossRealizedPnlUsd());
    }

    @Test
    void liveReduceShortLossRealizesNegativePnl() {
        CopyAccountingResult result = apply(PositionSide.SHORT, "10", "4", "100", "110", "6");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.REDUCE, result.deltaType());
        assertBd("-60.000000000000", result.grossRealizedPnlUsd());
    }

    @Test
    void liveCloseLongMarksClosedAndRealizesPnl() {
        CopyAccountingResult result = apply(PositionSide.LONG, "3", "0", "100", "110", "3");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.CLOSE_FULL, result.deltaType());
        assertTrue(result.positionClosed());
        assertFalse(result.positionRemainsOpen());
        assertBd("30.000000000000", result.grossRealizedPnlUsd());
    }

    @Test
    void liveCloseShortMarksClosedAndRealizesPnl() {
        CopyAccountingResult result = apply(PositionSide.SHORT, "3", "0", "100", "90", "3");

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.CLOSE_FULL, result.deltaType());
        assertTrue(result.positionClosed());
        assertBd("30.000000000000", result.grossRealizedPnlUsd());
    }

    @Test
    void livePriceZeroIsRejectedBeforePersistingSuccessAccounting() {
        CopyAccountingResult result = apply(PositionSide.LONG, "10", "4", "100", "0", "6");

        assertFalse(result.accepted());
        assertEquals("PRICE_SOURCE_UNAVAILABLE", result.reasonCode());
    }

    private CopyAccountingResult apply(PositionSide side,
                                       String previousQty,
                                       String resultingQty,
                                       String entryPrice,
                                       String fillPrice,
                                       String executedQty) {
        return adapter.apply(new LiveCopyAccountingInput(
                "BTCUSDT",
                side,
                bd(previousQty),
                bd(resultingQty),
                bd(entryPrice),
                order(fillPrice, executedQty),
                null,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                null
        ));
    }

    private BinanceFuturesOrderClientResponse order(String fillPrice, String executedQty) {
        BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
        response.setOrderId(123L);
        response.setSymbol("BTCUSDT");
        response.setStatus("FILLED");
        response.setAvgPrice(bd(fillPrice));
        response.setOrigQty(bd(executedQty));
        response.setExecutedQty(bd(executedQty));
        return response;
    }

    private BigDecimal bd(String value) {
        return value == null ? null : new BigDecimal(value);
    }

    private void assertBd(String expected, BigDecimal actual) {
        assertEquals(0, new BigDecimal(expected).compareTo(actual));
    }
}
