package com.apunto.engine.dto.client;

import com.apunto.engine.shared.enums.PositionSide;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class CloseOperationClientRequest {
    private String symbol;
    private BigDecimal operationQty;
    private PositionSide positionSide;
    private String clientOrderId;
}
