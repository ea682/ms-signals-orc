package com.apunto.engine.dto.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BinanceFuturesSymbolFilterDto {
    private String filterType;
    private String maxPrice;
    private String minPrice;
    private String tickSize;
    private String maxQty;
    private String minQty;
    private String stepSize;
    private Integer limit;
    private String notional;
    private String multiplierUp;
    private String multiplierDown;
    private String multiplierDecimal;
}
