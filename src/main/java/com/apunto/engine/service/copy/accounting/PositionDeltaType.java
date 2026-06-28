package com.apunto.engine.service.copy.accounting;

public enum PositionDeltaType {
    OPEN,
    INCREASE,
    REDUCE,
    CLOSE_FULL,
    NOOP,
    FLIP,
    SNAPSHOT_NOOP,
    INVALID
}
