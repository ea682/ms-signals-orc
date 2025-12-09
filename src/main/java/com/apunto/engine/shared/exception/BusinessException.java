package com.apunto.engine.shared.exception;

import java.util.Map;


public class BusinessException extends EngineException {

    public BusinessException(String message) {
        super(ErrorCode.BUSINESS_ERROR, message);
    }

    public BusinessException(String message, Map<String, Object> details) {
        super(ErrorCode.BUSINESS_ERROR, message, details);
    }
}
