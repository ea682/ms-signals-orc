package com.apunto.engine.outbox.exception;

public class MetricOutboxSerializationException extends RuntimeException {
    public MetricOutboxSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
