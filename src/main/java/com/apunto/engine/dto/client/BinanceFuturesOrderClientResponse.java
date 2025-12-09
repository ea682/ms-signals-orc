package com.apunto.engine.dto.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BinanceFuturesOrderClientResponse {

    private Long orderId;
    private String symbol;
    private String status;
    private String clientOrderId;

    private BigDecimal price;
    private BigDecimal avgPrice;
    private BigDecimal origQty;
    private BigDecimal executedQty;
    private BigDecimal cumQty;
    private BigDecimal cumQuote;

    private String timeInForce;
    private String type;

    private boolean reduceOnly;
    private boolean closePosition;

    private String side;
    private String positionSide;

    private BigDecimal stopPrice;
    private String workingType;
    private boolean priceProtect;

    private String origType;
    private String priceMatch;
    private String selfTradePreventionMode;

    private Long goodTillDate;
    private Long updateTime;
}

