package com.apunto.engine.dto;

import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class OperationDto {
    private String symbol;
    private Side side;
    private OrderType type;
    private PositionSide positionSide;
    private String quantity;
    private String price;
    private String timeInForce;
    private Integer leverage;
    private boolean reduceOnly;

    /**
     * When false, ms-binance-engine skips account-level setup calls (margin type/leverage).
     * Use false for hot ADJUST/RESIZE increases where the position already exists.
     */
    private Boolean configureAccountSettings;

    /**
     * Idempotency key propagated to ms-binance-engine and mapped to Binance Futures
     * parameter "newClientOrderId". Max 36 chars, allowed [A-Za-z0-9._-].
     */
    private String clientOrderId;

    /** Copy-trading trace metadata. Not sent to Binance itself; propagated as HTTP headers to ms-binance-engine logs. */
    private String originId;
    private String userId;
    private String walletId;

    /** Durable dispatch metadata. These fields are never sent to Binance. */
    private String sourceEventId;
    private String sourceEventType;
    private String copyIntent;
    private BigDecimal requestedMarginUsd;
    private BigDecimal requestedNotionalUsd;
    private BigDecimal referencePrice;
    private Boolean reservePosition;

    private String apiKey;
    private String secret;
}
