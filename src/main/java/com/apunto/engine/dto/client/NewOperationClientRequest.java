package com.apunto.engine.dto.client;

import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.Side;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NewOperationClientRequest {
    private String symbol;
    private Side side;
    private OrderType type;
    private String quantity;
    private String price;
    private String timeInForce;
    private Integer leverage;
}
