package com.apunto.engine.dto.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BinanceOrderBookLevelClientDto {
    private BigDecimal price;
    private BigDecimal quantity;
}
