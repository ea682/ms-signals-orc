package com.apunto.engine.shared.exception;

import org.springframework.http.HttpStatus;

public enum ErrorCode {

    VALIDATION_ERROR(HttpStatus.BAD_REQUEST, "Validation error"),
    RESOURCE_NOT_FOUND(HttpStatus.NOT_FOUND, "Resource not found"),
    BUSINESS_ERROR(HttpStatus.UNPROCESSABLE_ENTITY, "Business rule violation"),

    BINANCE_CLIENT_ERROR(HttpStatus.BAD_GATEWAY, "Error calling Binance Futures API"),
    BINANCE_RATE_LIMIT(HttpStatus.TOO_MANY_REQUESTS, "Binance rate limit exceeded"),
    EXTERNAL_SERVICE_ERROR(HttpStatus.BAD_GATEWAY, "External service error"),

    UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "Unauthorized"),
    FORBIDDEN(HttpStatus.FORBIDDEN, "Forbidden"),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected internal error");

    private final HttpStatus httpStatus;
    private final String defaultMessage;

    ErrorCode(HttpStatus httpStatus, String defaultMessage) {
        this.httpStatus = httpStatus;
        this.defaultMessage = defaultMessage;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }
}
