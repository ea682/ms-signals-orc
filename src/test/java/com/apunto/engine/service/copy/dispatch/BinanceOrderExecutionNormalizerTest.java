package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinanceOrderExecutionNormalizerTest {

    private final BinanceOrderExecutionNormalizer normalizer = new BinanceOrderExecutionNormalizer();

    @Test
    void filledWithNullAvgPriceMustBeAccepted() {
        NormalizedBinanceExecution result = normalizer.normalize(order(1L, "FILLED", "0.2", null, null));

        assertTrue(result.accepted());
        assertEquals(CopyExecutionState.FILLED, result.executionState());
        assertEquals(AveragePriceStatus.PENDING_RESOLUTION, result.averagePriceStatus());
        assertFalse(result.requiresReconciliation());
        assertFalse(result.safeToRetrySend());
        assertNull(result.averagePrice());
    }

    @Test
    void filledWithAveragePricePersistsNormally() {
        NormalizedBinanceExecution result = normalizer.normalize(order(2L, "FILLED", "0.2", "100", null));

        assertTrue(result.accepted());
        assertEquals(AveragePriceStatus.AVAILABLE, result.averagePriceStatus());
        assertEquals(new BigDecimal("100"), result.averagePrice());
    }

    @Test
    void filledPriceDerivedFromQuoteQty() {
        NormalizedBinanceExecution result = normalizer.normalize(order(3L, "FILLED", "2", null, "250"));

        assertEquals(new BigDecimal("125"), result.averagePrice());
        assertEquals(AveragePriceStatus.DERIVED_FROM_CUM_QUOTE, result.averagePriceStatus());
        assertFalse(result.safeToRetrySend());
    }

    @Test
    void newOrderWithOrderIdIsAcknowledged() {
        NormalizedBinanceExecution result = normalizer.normalize(order(4L, "NEW", null, null, null));

        assertTrue(result.accepted());
        assertEquals(CopyExecutionState.NEW, result.executionState());
        assertTrue(result.requiresReconciliation());
        assertFalse(result.safeToRetrySend());
    }

    @Test
    void partiallyFilledIsAcknowledgedAndReconciled() {
        NormalizedBinanceExecution result = normalizer.normalize(order(5L, "PARTIALLY_FILLED", "0.1", null, null));

        assertTrue(result.accepted());
        assertEquals(CopyExecutionState.PARTIALLY_FILLED, result.executionState());
        assertTrue(result.requiresReconciliation());
        assertFalse(result.safeToRetrySend());
    }

    @Test
    void canceledOrderWithExecutedQuantityIsTerminalExecutedNotRejected() {
        NormalizedBinanceExecution result = normalizer.normalize(order(6L, "CANCELED", "0.1", "100", null));

        assertTrue(result.accepted());
        assertEquals(CopyExecutionState.FILLED, result.executionState());
        assertFalse(result.requiresReconciliation());
        assertFalse(result.safeToRetrySend());
    }

    @Test
    void canceledOrderWithoutExecutedQuantityIsDefinitivelyRejected() {
        NormalizedBinanceExecution result = normalizer.normalize(order(7L, "CANCELED", null, null, null));

        assertFalse(result.accepted());
        assertEquals(CopyExecutionState.REJECTED, result.executionState());
        assertFalse(result.requiresReconciliation());
        assertFalse(result.safeToRetrySend());
    }

    @Test
    void canceledWithoutOrderIdRemainsAmbiguous() {
        NormalizedBinanceExecution result = normalizer.normalize(order(null, "CANCELED", null, null, null));

        assertFalse(result.accepted());
        assertEquals(CopyExecutionState.AMBIGUOUS, result.executionState());
        assertTrue(result.requiresReconciliation());
    }

    @Test
    void malformedResponseWithoutOrderIdBecomesAmbiguous() {
        NormalizedBinanceExecution result = normalizer.normalize(order(null, "FILLED", "1", "10", null));

        assertFalse(result.accepted());
        assertEquals(CopyExecutionState.AMBIGUOUS, result.executionState());
        assertTrue(result.requiresReconciliation());
        assertFalse(result.safeToRetrySend());
    }

    private BinanceFuturesOrderClientResponse order(Long id, String status, String qty, String avg, String quote) {
        BinanceFuturesOrderClientResponse response = new BinanceFuturesOrderClientResponse();
        response.setOrderId(id);
        response.setClientOrderId("cpO_test");
        response.setSymbol("BTCUSDC");
        response.setStatus(status);
        response.setExecutedQty(decimal(qty));
        response.setAvgPrice(decimal(avg));
        response.setCumQuote(decimal(quote));
        return response;
    }

    private BigDecimal decimal(String value) {
        return value == null ? null : new BigDecimal(value);
    }
}
