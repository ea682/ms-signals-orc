package com.apunto.engine.dto.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BinanceOrderBookSnapshotClientDto {
    private String symbol;
    private int limit;
    private Long lastUpdateId;
    private Long exchangeEventTime;
    private Long exchangeTransactionTime;
    private Instant capturedAt;
    private String source;
    private String status;
    private List<BinanceOrderBookLevelClientDto> bids;
    private List<BinanceOrderBookLevelClientDto> asks;
}
