package com.apunto.engine.service.copy.quality;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static com.apunto.engine.service.copy.quality.RoundTripExecutionQualityCalculator.Side.LONG;
import static com.apunto.engine.service.copy.quality.RoundTripExecutionQualityCalculator.Side.SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoundTripExecutionQualityCalculatorTest {

    private final RoundTripExecutionQualityCalculator calculator = new RoundTripExecutionQualityCalculator();

    @Test
    void longAndProfitableOnBothExchanges() {
        var result = calculate(LONG, fills("100", "1"), fills("110", "1"),
                fills("200", "1"), fills("218", "1"));
        assertTrue(result.complete());
        assertDecimal("100.00000000", result.executionDragBps());
    }

    @Test
    void shortAndProfitableOnBothExchanges() {
        var result = calculate(SHORT, fills("100", "1"), fills("90", "1"),
                fills("200", "1"), fills("182", "1"));
        assertTrue(result.complete());
        assertDecimal("100.00000000", result.executionDragBps());
    }

    @Test
    void weightedMultipleOpeningFillsAreUsed() {
        var result = calculate(LONG,
                List.of(fill("90", "1"), fill("110", "1")), fills("110", "2"),
                List.of(fill("190", "1"), fill("210", "1")), fills("220", "2"));
        assertDecimal("100", result.originOpenPrice());
        assertDecimal("200", result.copyOpenPrice());
        assertDecimal("0.00000000", result.executionDragBps());
    }

    @Test
    void partialClosesAreWeightedIntoACompleteCycle() {
        var result = calculate(LONG, fills("100", "2"),
                List.of(fill("108", "1"), fill("112", "1")), fills("200", "2"),
                List.of(fill("216", "1"), fill("224", "1")));
        assertTrue(result.complete());
        assertDecimal("110", result.originClosePrice());
        assertDecimal("220", result.copyClosePrice());
    }

    @Test
    void missingFourthLegIsIncompleteAndExcluded() {
        var result = calculate(LONG, fills("100", "1"), fills("110", "1"),
                fills("200", "1"), List.of());
        assertFalse(result.complete());
        assertEquals("COPY_CLOSE_MISSING", result.incompleteReason());
        assertEquals(null, result.executionDragBps());
    }

    @Test
    void constantCrossExchangeBasisDoesNotCreateFictitiousDrag() {
        var result = calculate(LONG, fills("100", "1"), fills("110", "1"),
                fills("200", "1"), fills("220", "1"));
        assertDecimal("0.00000000", result.executionDragBps());
    }

    @Test
    void losingTradeOnBothExchangesIsComparedByReturn() {
        var result = calculate(LONG, fills("100", "1"), fills("90", "1"),
                fills("200", "1"), fills("182", "1"));
        assertDecimal("-100.00000000", result.executionDragBps());
    }

    @Test
    void betterBinanceCloseProducesNegativeDrag() {
        var result = calculate(LONG, fills("100", "1"), fills("110", "1"),
                fills("200", "1"), fills("224", "1"));
        assertTrue(result.executionDragBps().signum() < 0);
    }

    @Test
    void worseBinanceCloseProducesPositiveDrag() {
        var result = calculate(LONG, fills("100", "1"), fills("110", "1"),
                fills("200", "1"), fills("216", "1"));
        assertTrue(result.executionDragBps().signum() > 0);
    }

    @Test
    void unfinishedPartialCloseIsIncomplete() {
        var result = calculate(LONG, fills("100", "2"), fills("110", "1"),
                fills("200", "2"), fills("220", "1"));
        assertFalse(result.complete());
        assertEquals("ORIGIN_CYCLE_NOT_FULLY_CLOSED", result.incompleteReason());
    }

    private RoundTripExecutionQualityCalculator.Result calculate(
            RoundTripExecutionQualityCalculator.Side side,
            List<RoundTripExecutionQualityCalculator.Fill> originOpen,
            List<RoundTripExecutionQualityCalculator.Fill> originClose,
            List<RoundTripExecutionQualityCalculator.Fill> copyOpen,
            List<RoundTripExecutionQualityCalculator.Fill> copyClose) {
        return calculator.calculate(new RoundTripExecutionQualityCalculator.Request(
                side, originOpen, originClose, copyOpen, copyClose,
                new BigDecimal("0.10"), new BigDecimal("0.02"), 150L));
    }

    private static List<RoundTripExecutionQualityCalculator.Fill> fills(String price, String qty) {
        return List.of(fill(price, qty));
    }

    private static RoundTripExecutionQualityCalculator.Fill fill(String price, String qty) {
        return new RoundTripExecutionQualityCalculator.Fill(new BigDecimal(price), new BigDecimal(qty));
    }

    private static void assertDecimal(String expected, BigDecimal actual) {
        assertEquals(0, new BigDecimal(expected).compareTo(actual));
    }
}
