package com.apunto.engine.shared.exception;

import java.util.Map;

public class FuturesCapitalMaintenanceException extends EngineException {

    public FuturesCapitalMaintenanceException(String message, Map<String, Object> details) {
        super(ErrorCode.BUSINESS_ERROR, message, details);
    }

    public FuturesCapitalMaintenanceException(String message, Throwable cause, Map<String, Object> details) {
        super(ErrorCode.BUSINESS_ERROR, message, cause, details);
    }
}
