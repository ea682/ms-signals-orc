package com.apunto.engine.shared.exception;

public class SkipExecutionException extends RuntimeException {

    public SkipExecutionException(String message) {
        super(message);
    }
}