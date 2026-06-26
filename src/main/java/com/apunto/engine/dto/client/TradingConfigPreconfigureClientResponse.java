package com.apunto.engine.dto.client;

import com.apunto.engine.shared.enums.PositionSide;
import lombok.Data;

@Data
public class TradingConfigPreconfigureClientResponse {
    private String symbol;
    private PositionSide positionSide;
    private Integer requestedLeverage;
    private String requestedMarginType;
    private boolean openPositionDetected;
    private String status;
    private String reasonCode;
    private String marginTypeAction;
    private String leverageAction;
}
