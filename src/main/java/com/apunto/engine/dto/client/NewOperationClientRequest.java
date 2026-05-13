package com.apunto.engine.dto.client;

import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NewOperationClientRequest {
    private String symbol;
    private Side side;
    private OrderType type;
    private String quantity;
    private String price;
    private String timeInForce;
    private Integer leverage;
    private PositionSide positionSide;
    private Boolean reduceOnly;

    /**
     * Idempotency key for Binance Futures orders (newClientOrderId).
     * Max 36 chars, allowed [A-Za-z0-9._-].
     */
    private String clientOrderId;
}
