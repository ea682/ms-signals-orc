package com.apunto.engine.outbox.exception;

public class MetricOutboxHashException extends RuntimeException {
    public MetricOutboxHashException(String message, Throwable cause) {
        super(message, cause);
    }
}
