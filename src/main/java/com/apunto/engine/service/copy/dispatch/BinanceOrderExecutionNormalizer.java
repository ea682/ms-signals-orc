package com.apunto.engine.service.copy.dispatch;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import org.springframework.stereotype.Component;

@Component
public final class BinanceOrderExecutionNormalizer {

    private static final BigDecimal ZERO = BigDecimal.ZERO;

    public NormalizedBinanceExecution normalize(BinanceFuturesOrderClientResponse response) {
        if (response == null) {
            return ambiguous(null, null, null, null, null);
        }
        String status = normalizeStatus(response.getStatus());
        BigDecimal executedQty = firstPositive(response.getExecutedQty(), response.getCumQty());
        boolean hasOrderId = response.getOrderId() != null;
        boolean filled = hasOrderId && "FILLED".equals(status) && positive(executedQty);
        boolean partial = hasOrderId && "PARTIALLY_FILLED".equals(status) && positive(executedQty);
        boolean acknowledgedNew = hasOrderId && "NEW".equals(status);
        boolean terminalPartial = hasOrderId && terminalWithoutRemainingOrder(status) && positive(executedQty);
        boolean accepted = filled || partial || acknowledgedNew || terminalPartial;

        CopyExecutionState state;
        if (filled || terminalPartial) state = CopyExecutionState.FILLED;
        else if (partial) state = CopyExecutionState.PARTIALLY_FILLED;
        else if (acknowledgedNew) state = CopyExecutionState.NEW;
        else if (hasOrderId && ("REJECTED".equals(status) || "EXPIRED".equals(status) || "CANCELED".equals(status))) state = CopyExecutionState.REJECTED;
        else state = CopyExecutionState.AMBIGUOUS;

        BigDecimal averagePrice = firstPositive(response.getAvgPrice(), response.getPrice());
        AveragePriceStatus priceStatus = averagePrice == null
                ? (accepted ? AveragePriceStatus.PENDING_RESOLUTION : AveragePriceStatus.NOT_AVAILABLE)
                : AveragePriceStatus.AVAILABLE;
        if (averagePrice == null && positive(response.getCumQuote()) && positive(executedQty)) {
            averagePrice = response.getCumQuote().divide(executedQty, 18, RoundingMode.HALF_UP).stripTrailingZeros();
            priceStatus = AveragePriceStatus.DERIVED_FROM_CUM_QUOTE;
        }

        boolean reconcile = state == CopyExecutionState.NEW
                || state == CopyExecutionState.PARTIALLY_FILLED
                || state == CopyExecutionState.AMBIGUOUS;

        // IMPORTANT:
        // A Binance MARKET order can be accepted or FILLED even when avgPrice is null.
        // orderId/status/executedQty determine execution acknowledgement.
        // Missing average price must trigger asynchronous price resolution, never a
        // blind resend of the order.
        return new NormalizedBinanceExecution(
                accepted, state, response.getOrderId(), response.getClientOrderId(), status,
                executedQty, averagePrice, response.getCumQuote(), priceStatus, reconcile, false);
    }

    private NormalizedBinanceExecution ambiguous(Long orderId, String clientOrderId, String status,
                                                  BigDecimal qty, BigDecimal quote) {
        return new NormalizedBinanceExecution(false, CopyExecutionState.AMBIGUOUS, orderId,
                clientOrderId, status, qty, null, quote, AveragePriceStatus.NOT_AVAILABLE, true, false);
    }

    private BigDecimal firstPositive(BigDecimal first, BigDecimal second) {
        if (positive(first)) return first;
        return positive(second) ? second : null;
    }

    private boolean positive(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0;
    }

    private String normalizeStatus(String value) {
        if (value == null || value.isBlank()) return "";
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        return "PARTIAL_FILLED".equals(normalized) ? "PARTIALLY_FILLED" : normalized;
    }

    private boolean terminalWithoutRemainingOrder(String status) {
        return "CANCELED".equals(status) || "EXPIRED".equals(status);
    }
}
