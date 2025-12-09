package com.apunto.engine.shared.exception;

import com.apunto.engine.shared.dto.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.lang.Nullable;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;


@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(EngineException.class)
    public ResponseEntity<ApiResponse<Object>> handleEngineException(
            EngineException ex,
            HttpServletRequest request
    ) {
        ErrorCode errorCode = ex.getErrorCode();
        HttpStatus status = errorCode.getHttpStatus();

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("errorCode", errorCode.name());
        if (ex.getDetails() != null && !ex.getDetails().isEmpty()) {
            data.put("details", ex.getDetails());
        }

        return buildErrorResponse(status, errorCode, ex.getMessage(), data, request, ex);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Object>> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> fieldErrors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .collect(Collectors.toMap(
                        err -> err.getField(),
                        err -> err.getDefaultMessage(),
                        (a, b) -> b,
                        LinkedHashMap::new
                ));

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("errorCode", ErrorCode.VALIDATION_ERROR.name());
        data.put("details", fieldErrors);

        return buildErrorResponse(
                ErrorCode.VALIDATION_ERROR.getHttpStatus(),
                ErrorCode.VALIDATION_ERROR,
                "Validation error",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ApiResponse<Object>> handleConstraintViolation(
            ConstraintViolationException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> violations = ex.getConstraintViolations()
                .stream()
                .collect(Collectors.toMap(
                        v -> v.getPropertyPath().toString(),
                        v -> v.getMessage(),
                        (a, b) -> b,
                        LinkedHashMap::new
                ));

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("errorCode", ErrorCode.VALIDATION_ERROR.name());
        data.put("details", violations);

        return buildErrorResponse(
                ErrorCode.VALIDATION_ERROR.getHttpStatus(),
                ErrorCode.VALIDATION_ERROR,
                "Validation error",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiResponse<Object>> handleMethodArgumentTypeMismatch(
            MethodArgumentTypeMismatchException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("errorCode", ErrorCode.VALIDATION_ERROR.name());
        data.put("details", Map.of(
                "parameter", ex.getName(),
                "value", ex.getValue(),
                "requiredType", ex.getRequiredType() != null ? ex.getRequiredType().getSimpleName() : null
        ));

        return buildErrorResponse(
                ErrorCode.VALIDATION_ERROR.getHttpStatus(),
                ErrorCode.VALIDATION_ERROR,
                "Invalid request parameter",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiResponse<Object>> handleHttpMessageNotReadable(
            HttpMessageNotReadableException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> data = Map.of(
                "errorCode", ErrorCode.VALIDATION_ERROR.name()
        );

        return buildErrorResponse(
                ErrorCode.VALIDATION_ERROR.getHttpStatus(),
                ErrorCode.VALIDATION_ERROR,
                "Malformed JSON request",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ApiResponse<Object>> handleMethodNotSupported(
            HttpRequestMethodNotSupportedException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("errorCode", ErrorCode.VALIDATION_ERROR.name());
        data.put("details", Map.of(
                "method", ex.getMethod(),
                "supportedMethods", ex.getSupportedHttpMethods()
        ));

        return buildErrorResponse(
                HttpStatus.METHOD_NOT_ALLOWED,
                ErrorCode.VALIDATION_ERROR,
                "HTTP method not supported",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Object>> handleGenericException(
            Exception ex,
            HttpServletRequest request
    ) {
        Map<String, Object> data = Map.of(
                "errorCode", ErrorCode.INTERNAL_ERROR.name()
        );

        return buildErrorResponse(
                ErrorCode.INTERNAL_ERROR.getHttpStatus(),
                ErrorCode.INTERNAL_ERROR,
                ErrorCode.INTERNAL_ERROR.getDefaultMessage(),
                data,
                request,
                ex
        );
    }

    private ResponseEntity<ApiResponse<Object>> buildErrorResponse(
            HttpStatus status,
            ErrorCode errorCode,
            @Nullable String message,
            @Nullable Map<String, Object> data,
            HttpServletRequest request,
            Throwable ex
    ) {
        String traceId = resolveTraceId();
        String path = request.getRequestURI();

        log.warn("Handling exception [{}] - code={} path={} message={}",
                traceId, errorCode.name(), path, ex.getMessage(), ex);

        ApiResponse<Object> body = ApiResponse.<Object>builder()
                .status("ERROR")
                .statusCode(status.value())
                .message(message != null ? message : errorCode.getDefaultMessage())
                .data(data == null || data.isEmpty() ? null : data)
                .timestamp(Instant.now())
                .path(path)
                .traceId(traceId)
                .build();

        return ResponseEntity.status(status).body(body);
    }


    private String resolveTraceId() {
        String existing = MDC.get("traceId");
        if (existing != null && !existing.isBlank()) {
            return existing;
        }
        String generated = UUID.randomUUID().toString();
        MDC.put("traceId", generated);
        return generated;
    }
}
