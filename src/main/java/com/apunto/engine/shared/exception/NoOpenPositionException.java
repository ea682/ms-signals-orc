package com.apunto.engine.shared.exception;

public class NoOpenPositionException extends EngineException {
    public NoOpenPositionException(String message) {
        super(ErrorCode.BUSINESS_ERROR, message);
    }
}
