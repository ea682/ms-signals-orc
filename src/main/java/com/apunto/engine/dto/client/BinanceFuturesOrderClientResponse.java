package com.apunto.engine.dto.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

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

    private BigDecimal liveRequestedLeverageX;
    private BigDecimal liveExchangeLeverageX;
    private BigDecimal liveEffectiveLeverageX;
    private String liveMarginMode;
    private String leverageStatus;
    private Instant leverageConfirmedAt;
    private BigDecimal liveNotionalUsd;
    private BigDecimal liveRequiredMarginUsd;
    private boolean accepted;
    private String executionState;
    private String averagePriceStatus;
    private boolean requiresReconciliation;
    private boolean safeToRetrySend;
    private UUID dispatchIntentId;
    private String sourceEventId;
    private BigDecimal referencePrice;
}
