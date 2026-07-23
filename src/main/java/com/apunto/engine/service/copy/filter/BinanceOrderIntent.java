package com.apunto.engine.service.copy.filter;

public enum BinanceOrderIntent {
    OPEN(true),
    INCREASE(true),
    REOPEN(true),
    FLIP_OPEN(true),
    REDUCE(false),
    CLOSE(false),
    FLIP_CLOSE(false);

    private final boolean increasesExposure;

    BinanceOrderIntent(boolean increasesExposure) {
        this.increasesExposure = increasesExposure;
    }

    public boolean increasesExposure() {
        return increasesExposure;
    }
}
