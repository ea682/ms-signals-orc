package com.apunto.engine.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class CloseOperationDto {

    private String symbol;
    private BigDecimal operationQty;
    private String apiKey;
    private String secret;
}
