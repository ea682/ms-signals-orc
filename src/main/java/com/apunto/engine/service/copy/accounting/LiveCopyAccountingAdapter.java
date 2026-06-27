package com.apunto.engine.service.copy.accounting;

import com.apunto.engine.dto.client.BinanceFuturesOrderClientResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
@Service
@RequiredArgsConstructor
public class LiveCopyAccountingAdapter {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final int SCALE = 12;

    private final CopyPositionAccountingService accountingService;

    public CopyAccountingResult apply(LiveCopyAccountingInput input) {
        BinanceFuturesOrderClientResponse order = input == null ? null : input.order();
        BigDecimal executedQty = executedQty(order);
        BigDecimal fillPrice = fillPrice(order, executedQty, input == null ? null : input.fallbackExecutionPrice());
        CopyAccountingResult result = accountingService.apply(new CopyAccountingInput(
                input == null ? null : input.symbol(),
                input == null ? null : input.side(),
                input == null ? null : input.previousQty(),
                input == null ? null : input.resultingQty(),
                input == null ? null : input.currentAvgEntryPrice(),
                fillPrice,
                input == null ? null : input.commissionUsd(),
                input == null ? null : input.slippageUsd(),
                input == null ? null : input.eventTime(),
                AccountingMode.LIVE
        ));
        if (result.accepted()) {
            log.info("event=live_copy_accounting_applied reasonCode=LIVE_EXECUTION_ACCOUNTED orderId={} symbol={} side={} executedQty={} avgFillPrice={} commissionUsd={} deltaType={}",
                    order == null ? null : order.getOrderId(),
                    input == null ? null : input.symbol(),
                    input == null ? null : input.side(),
                    executedQty,
                    fillPrice,
                    input == null ? null : input.commissionUsd(),
                    result.deltaType());
        }
        return result;
    }

    private BigDecimal executedQty(BinanceFuturesOrderClientResponse order) {
        if (order == null) {
            return ZERO;
        }
        BigDecimal qty = firstPositive(order.getExecutedQty(), order.getCumQty(), order.getOrigQty());
        return qty == null ? ZERO : qty;
    }

    private BigDecimal fillPrice(BinanceFuturesOrderClientResponse order, BigDecimal executedQty, BigDecimal fallbackExecutionPrice) {
        if (order == null) {
            return firstPositive(fallbackExecutionPrice, ZERO);
        }
        BigDecimal direct = firstPositive(order.getAvgPrice(), order.getPrice());
        if (direct != null) {
            return direct;
        }
        if (isPositive(order.getCumQuote()) && isPositive(executedQty)) {
            return order.getCumQuote().divide(executedQty, SCALE, RoundingMode.HALF_UP);
        }
        return firstPositive(fallbackExecutionPrice, ZERO);
    }

    private BigDecimal firstPositive(BigDecimal... values) {
        if (values == null) {
            return null;
        }
        for (BigDecimal value : values) {
            if (isPositive(value)) {
                return value;
            }
        }
        return null;
    }

    private boolean isPositive(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0;
    }
}
