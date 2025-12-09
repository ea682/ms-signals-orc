package com.apunto.engine.dto.client;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class CloseOperationClientRequest {
    private String symbol;
    private BigDecimal operationQty;
}
