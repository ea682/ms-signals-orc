package com.apunto.engine.dto;

import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import lombok.Builder;
import lombok.Data;

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
     * Idempotency key propagated to ms-binance-engine and mapped to Binance Futures
     * parameter "newClientOrderId". Max 36 chars, allowed [A-Za-z0-9._-].
     */
    private String clientOrderId;

    private String apiKey;
    private String secret;
}
