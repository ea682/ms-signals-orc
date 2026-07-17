package com.apunto.engine.dto.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BinanceExecutionFillClientDto {
    private String tradeId;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal quoteQuantity;
    private BigDecimal commission;
    private String commissionAsset;
    private BigDecimal realizedPnl;
    private Instant executedAt;
    private boolean buyer;
    private boolean maker;
}
