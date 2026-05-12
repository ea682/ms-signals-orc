package com.apunto.engine.shared.exception;

import java.util.Map;

public class CopyMetricNotAvailableException extends CopyExecutionException {

    public CopyMetricNotAvailableException(String message) {
        super("copy_metric_not_available", ErrorCode.BUSINESS_ERROR, message);
    }

    public CopyMetricNotAvailableException(String message, Map<String, Object> details) {
        super("copy_metric_not_available", ErrorCode.BUSINESS_ERROR, message, details);
    }

    public CopyMetricNotAvailableException(String message, Throwable cause, Map<String, Object> details) {
        super("copy_metric_not_available", ErrorCode.BUSINESS_ERROR, message, cause, details);
    }
}
