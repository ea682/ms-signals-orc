package com.apunto.copytarget;

import java.math.BigDecimal;

public record OrderBookLevel(BigDecimal price, BigDecimal quantity) {
    public OrderBookLevel {
        price = DecimalSupport.nonNegative(price, "price");
        quantity = DecimalSupport.nonNegative(quantity, "quantity");
        if (price.signum() == 0 || quantity.signum() == 0) {
            throw new IllegalArgumentException("order book price and quantity must be positive");
        }
    }
}
