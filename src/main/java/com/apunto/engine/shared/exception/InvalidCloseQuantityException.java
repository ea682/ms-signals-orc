package com.apunto.engine.shared.exception;

public class InvalidCloseQuantityException extends EngineException {
    public InvalidCloseQuantityException(String message) {
        super(ErrorCode.BUSINESS_ERROR, message);
    }
}
