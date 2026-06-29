package com.apunto.engine.dto;

import com.apunto.engine.shared.enums.PositionSide;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

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
    private BigDecimal leverage;
    private BigDecimal sizeQty;
    private BigDecimal sizeLegacy;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private OffsetDateTime sourceTs;
}
