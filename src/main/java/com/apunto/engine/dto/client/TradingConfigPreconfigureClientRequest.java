package com.apunto.engine.dto.client;

import com.apunto.engine.shared.enums.PositionSide;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TradingConfigPreconfigureClientRequest {
    private String symbol;
    private PositionSide positionSide;
    private Integer leverage;
    private String marginType;
    private Boolean skipIfOpenPosition;
}
