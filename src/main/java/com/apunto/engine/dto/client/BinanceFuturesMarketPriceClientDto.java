package com.apunto.engine.dto.client;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class BinanceFuturesMarketPriceClientDto {

    private String symbol;
    private String usage;
    private BigDecimal price;
    private String source;
    private boolean available;
    private boolean fresh;
    private boolean stale;
    private String reasonCode;
    private Long eventTimeMs;
    private Long receivedAtMs;
    private Long ageMs;
    private BigDecimal markPrice;
    private Long markAgeMs;
    private BigDecimal bidPrice;
    private BigDecimal askPrice;
    private BigDecimal bookMidPrice;
    private Long bookAgeMs;
    private boolean markFeedConnected;
    private boolean bookFeedConnected;
    private Long markLastMessageAgeMs;
    private Long bookLastMessageAgeMs;
    private long markReconnects;
    private long bookReconnects;
}
