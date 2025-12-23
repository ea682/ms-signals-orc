package com.apunto.engine.jobs.model;

public enum CopyJobErrorCategory {
    NONE,
    SKIP,
    RATE_LIMIT,
    VALIDATION,
    NETWORK,
    TRANSIENT,
    REJECTED,
    UNKNOWN
}