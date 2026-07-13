package com.apunto.engine.dto.client;

import lombok.Data;

@Data
public class BinanceFuturesPositionClientDto {
    private String symbol;
    private String positionAmt;
    private String entryPrice;
    private String markPrice;
    private String unRealizedProfit;
    private String leverage;
    private String marginType;
    private String isolatedMargin;
    private String positionSide;
    private String notional;
    private long updateTime;
    private boolean isolated;
}
