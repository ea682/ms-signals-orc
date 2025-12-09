package com.apunto.engine.dto;

import com.apunto.engine.shared.enums.OrderType;
import com.apunto.engine.shared.enums.Side;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NewOperationDto {
    private String symbol;
    private Side side;
    private OrderType type;
    private String quantity;
    private String price;
    private String timeInForce;
    private Integer leverage;
    private String apiKey;
    private String secret;
}
