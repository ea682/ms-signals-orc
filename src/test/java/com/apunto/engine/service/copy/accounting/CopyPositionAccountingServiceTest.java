package com.apunto.engine.service.copy.accounting;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import com.apunto.engine.service.impl.ShadowCopyTradingServiceImpl;
import com.apunto.engine.shared.enums.PositionSide;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyPositionAccountingServiceTest {

    private final PositionDeltaClassifier classifier = new PositionDeltaClassifier();
    private final CopyPositionAccountingService accounting = new CopyPositionAccountingService(classifier);

    @Test
    void classifierOpen() {
        assertEquals(PositionDeltaType.OPEN, classifier.classify(BigDecimal.ZERO, new BigDecimal("5")));
    }

    @Test
    void classifierIncrease() {
        assertEquals(PositionDeltaType.INCREASE, classifier.classify(new BigDecimal("5"), new BigDecimal("8")));
    }

    @Test
    void classifierReduce() {
        assertEquals(PositionDeltaType.REDUCE, classifier.classify(new BigDecimal("8"), new BigDecimal("3")));
    }

    @Test
    void classifierCloseFull() {
        assertEquals(PositionDeltaType.CLOSE_FULL, classifier.classify(new BigDecimal("8"), BigDecimal.ZERO));
    }

    @Test
    void classifierNoopUsesCompareToNotEqualsScale() {
        assertEquals(PositionDeltaType.NOOP, classifier.classify(new BigDecimal("8.0"), new BigDecimal("8.00")));
    }

    @Test
    void longIncreaseUsesWeightedAverageEntry() {
        CopyAccountingResult result = accounting.apply(input(PositionSide.LONG, "10", "20", "100", "120", "0", "0"));

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.INCREASE, result.deltaType());
        assertBd("10", result.deltaAddedQty());
        assertBd("110.000000000000", result.newAvgEntryPrice());
        assertBd("0", result.netRealizedPnlUsd());
    }

    @Test
    void shortIncreaseUsesWeightedAverageEntry() {
        CopyAccountingResult result = accounting.apply(input(PositionSide.SHORT, "10", "20", "100", "80", "0", "0"));

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.INCREASE, result.deltaType());
        assertBd("10", result.deltaAddedQty());
        assertBd("90.000000000000", result.newAvgEntryPrice());
        assertBd("0", result.netRealizedPnlUsd());
    }

    @Test
    void longReduceFavorable() {
        CopyAccountingResult result = accounting.apply(input(PositionSide.LONG, "10", "4", "100", "110", "0", "0.132"));

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.REDUCE, result.deltaType());
        assertBd("6", result.deltaClosedQty());
        assertBd("60.000000000000", result.grossRealizedPnlUsd());
        assertBd("59.868000000000", result.netRealizedPnlUsd());
        assertBd("100", result.newAvgEntryPrice());
        assertTrue(result.positionRemainsOpen());
        assertFalse(result.positionClosed());
    }

    @Test
    void longReduceUnfavorable() {
        CopyAccountingResult result = accounting.apply(input(PositionSide.LONG, "10", "4", "100", "90", "0", "0"));

        assertTrue(result.accepted());
        assertBd("-60.000000000000", result.grossRealizedPnlUsd());
        assertBd("-60.000000000000", result.netRealizedPnlUsd());
    }

    @Test
    void shortReduceFavorable() {
        CopyAccountingResult result = accounting.apply(input(PositionSide.SHORT, "16.87838", "5.04902", "60072", "59969", "0", "141.878977968"));

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.REDUCE, result.deltaType());
        assertBd("11.82936", result.deltaClosedQty());
        assertBd("1218.424080000000", result.grossRealizedPnlUsd());
        assertBd("1076.545102032000", result.netRealizedPnlUsd());
        assertBd("60072", result.newAvgEntryPrice());
        assertTrue(result.positionRemainsOpen());
    }

    @Test
    void shortCloseFavorable() {
        CopyAccountingResult result = accounting.apply(input(PositionSide.SHORT, "5.04902", "0", "60072", "59970", "0", "60.55794588"));

        assertTrue(result.accepted());
        assertEquals(PositionDeltaType.CLOSE_FULL, result.deltaType());
        assertBd("5.04902", result.deltaClosedQty());
        assertBd("515.000040000000", result.grossRealizedPnlUsd());
        assertTrue(result.netRealizedPnlUsd().compareTo(BigDecimal.ZERO) > 0);
        assertTrue(result.positionClosed());
    }

    @Test
    void invalidPriceRejected() {
        CopyAccountingResult result = accounting.apply(input(PositionSide.LONG, "10", "4", "100", "0", "0", "0"));

        assertFalse(result.accepted());
        assertEquals("PRICE_SOURCE_UNAVAILABLE", result.reasonCode());
        assertEquals(null, result.netRealizedPnlUsd());
    }

    @Test
    void invalidAvgEntryOnReduceRejected() {
        CopyAccountingResult result = accounting.apply(input(PositionSide.LONG, "10", "4", null, "110", "0", "0"));

        assertFalse(result.accepted());
        assertEquals("ENTRY_PRICE_MISSING", result.reasonCode());
        assertEquals(null, result.netRealizedPnlUsd());
    }

    @Test
    void shadowAndLiveAreWiredToCommonAccounting() {
        boolean shadowHasCommonAccounting = Arrays.stream(ShadowCopyTradingServiceImpl.class.getDeclaredFields())
                .map(Field::getType)
                .anyMatch(CopyPositionAccountingService.class::equals);
        assertTrue(shadowHasCommonAccounting);

        LiveCopyAccountingAdapter adapter = new LiveCopyAccountingAdapter(accounting);
        BinanceFuturesOrderClientResponse order = new BinanceFuturesOrderClientResponse();
        order.setOrderId(123L);
        order.setSymbol("BTCUSDT");
        order.setExecutedQty(new BigDecimal("6"));
        order.setAvgPrice(new BigDecimal("110"));
        CopyAccountingResult liveResult = adapter.apply(new LiveCopyAccountingInput(
                "BTCUSDT",
                PositionSide.LONG,
                new BigDecimal("10"),
                new BigDecimal("4"),
                new BigDecimal("100"),
                order,
                null,
                BigDecimal.ZERO,
                new BigDecimal("0.132"),
                null
        ));

        assertTrue(liveResult.accepted());
        assertEquals(PositionDeltaType.REDUCE, liveResult.deltaType());
        assertBd("59.868000000000", liveResult.netRealizedPnlUsd());
    }

    private CopyAccountingInput input(PositionSide side,
                                      String previousQty,
                                      String resultingQty,
                                      String avgEntry,
                                      String executionPrice,
                                      String feeUsd,
                                      String slippageUsd) {
        return new CopyAccountingInput(
                "BTCUSDT",
                side,
                bd(previousQty),
                bd(resultingQty),
                bd(avgEntry),
                bd(executionPrice),
                bd(feeUsd),
                bd(slippageUsd),
                null,
                AccountingMode.SHADOW
        );
    }

    private BigDecimal bd(String value) {
        return value == null ? null : new BigDecimal(value);
    }

    private void assertBd(String expected, BigDecimal actual) {
        assertEquals(0, new BigDecimal(expected).compareTo(actual));
    }
}
