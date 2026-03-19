package com.apunto.engine.dto;

import com.apunto.engine.shared.enums.PositionSide;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OriginBasketPositionDto {
    private String originId;
    private String walletId;
    private String symbol;
    private PositionSide side;
    private BigDecimal entryPrice;
    private BigDecimal markPrice;
    private BigDecimal marginUsedUsd;
    private BigDecimal notionalUsd;
    private BigDecimal sizeQty;
    private BigDecimal sizeLegacy;
}
