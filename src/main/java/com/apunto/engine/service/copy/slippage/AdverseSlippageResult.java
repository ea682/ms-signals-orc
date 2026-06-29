package com.apunto.engine.service.copy.slippage;

import com.apunto.engine.shared.enums.Side;

import java.math.BigDecimal;

public record AdverseSlippageResult(
        Side side,
        CopySlippageAction action,
        BigDecimal expectedPrice,
        BigDecimal executedPrice,
        BigDecimal executedQty,
        BigDecimal rawSlippageBps,
        BigDecimal adverseSlippageBps,
        BigDecimal rawSlippageUsd,
        BigDecimal adverseSlippageUsd,
        AdverseSlippageStatus status
) {
    public boolean available() {
        return status == AdverseSlippageStatus.OK;
    }
}
