package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.OrderBookSnapshot;

import java.util.Optional;

@FunctionalInterface
public interface CopyOrderBookProvider {
    Optional<OrderBookSnapshot> snapshot(String symbol, int limit);
}
