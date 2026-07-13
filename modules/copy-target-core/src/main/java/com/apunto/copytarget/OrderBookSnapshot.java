package com.apunto.copytarget;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public record OrderBookSnapshot(
        String symbol,
        Instant capturedAt,
        String source,
        Long sequenceNumber,
        List<OrderBookLevel> bids,
        List<OrderBookLevel> asks
) {
    public OrderBookSnapshot {
        symbol = required(symbol, "symbol").toUpperCase();
        capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
        source = required(source, "source");
        bids = (bids == null ? List.<OrderBookLevel>of() : bids).stream()
                .sorted(Comparator.comparing(OrderBookLevel::price).reversed())
                .toList();
        asks = (asks == null ? List.<OrderBookLevel>of() : asks).stream()
                .sorted(Comparator.comparing(OrderBookLevel::price))
                .toList();
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        String normalized = value.trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }
}
