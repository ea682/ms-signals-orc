package com.apunto.engine.shared.exception;

import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.shared.util.LogFmt;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.lang.Nullable;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

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
        Map<String, Object> data = baseData(errorCode);
        if (ex.getDetails() != null && !ex.getDetails().isEmpty()) {
            data.put("details", ex.getDetails());
        }
        return buildErrorResponse(errorCode.getHttpStatus(), errorCode, ex.getMessage(), data, request, ex);
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
                        err -> err.getDefaultMessage() == null ? "Invalid value" : err.getDefaultMessage(),
                        (a, b) -> b,
                        LinkedHashMap::new
                ));

        Map<String, Object> data = baseData(ErrorCode.VALIDATION_ERROR);
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

        Map<String, Object> data = baseData(ErrorCode.VALIDATION_ERROR);
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
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("parameter", ex.getName());
        details.put("value", ex.getValue() == null ? null : String.valueOf(ex.getValue()));
        details.put("requiredType", ex.getRequiredType() == null ? null : ex.getRequiredType().getSimpleName());

        Map<String, Object> data = baseData(ErrorCode.VALIDATION_ERROR);
        data.put("details", details);
        return buildErrorResponse(
                ErrorCode.VALIDATION_ERROR.getHttpStatus(),
                ErrorCode.VALIDATION_ERROR,
                "Invalid request parameter",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ApiResponse<Object>> handleMissingServletRequestParameter(
            MissingServletRequestParameterException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("parameter", ex.getParameterName());
        details.put("requiredType", ex.getParameterType());

        Map<String, Object> data = baseData(ErrorCode.VALIDATION_ERROR);
        data.put("details", details);
        return buildErrorResponse(
                ErrorCode.VALIDATION_ERROR.getHttpStatus(),
                ErrorCode.VALIDATION_ERROR,
                "Missing request parameter",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiResponse<Object>> handleIllegalArgumentException(
            IllegalArgumentException ex,
            HttpServletRequest request
    ) {
        return buildErrorResponse(
                ErrorCode.VALIDATION_ERROR.getHttpStatus(),
                ErrorCode.VALIDATION_ERROR,
                ex.getMessage(),
                baseData(ErrorCode.VALIDATION_ERROR),
                request,
                ex
        );
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiResponse<Object>> handleHttpMessageNotReadable(
            HttpMessageNotReadableException ex,
            HttpServletRequest request
    ) {
        return buildErrorResponse(
                ErrorCode.VALIDATION_ERROR.getHttpStatus(),
                ErrorCode.VALIDATION_ERROR,
                "Malformed JSON request",
                baseData(ErrorCode.VALIDATION_ERROR),
                request,
                ex
        );
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ApiResponse<Object>> handleMethodNotSupported(
            HttpRequestMethodNotSupportedException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("method", ex.getMethod());
        details.put("supportedMethods", ex.getSupportedHttpMethods());

        Map<String, Object> data = baseData(ErrorCode.VALIDATION_ERROR);
        data.put("details", details);
        return buildErrorResponse(
                HttpStatus.METHOD_NOT_ALLOWED,
                ErrorCode.VALIDATION_ERROR,
                "HTTP method not supported",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ApiResponse<Object>> handleMediaTypeNotSupported(
            HttpMediaTypeNotSupportedException ex,
            HttpServletRequest request
    ) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("contentType", ex.getContentType() == null ? null : ex.getContentType().toString());
        details.put("supportedMediaTypes", ex.getSupportedMediaTypes());

        Map<String, Object> data = baseData(ErrorCode.VALIDATION_ERROR);
        data.put("details", details);
        return buildErrorResponse(
                HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                ErrorCode.VALIDATION_ERROR,
                "HTTP media type not supported",
                data,
                request,
                ex
        );
    }

    @ExceptionHandler(NoHandlerFoundException.class)
    public ResponseEntity<ApiResponse<Object>> handleNoHandlerFound(
            NoHandlerFoundException ex,
            HttpServletRequest request
    ) {
        return buildErrorResponse(
                ErrorCode.RESOURCE_NOT_FOUND.getHttpStatus(),
                ErrorCode.RESOURCE_NOT_FOUND,
                ErrorCode.RESOURCE_NOT_FOUND.getDefaultMessage(),
                baseData(ErrorCode.RESOURCE_NOT_FOUND),
                request,
                ex
        );
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiResponse<Object>> handleDataAccessException(
            DataAccessException ex,
            HttpServletRequest request
    ) {
        return buildErrorResponse(
                ErrorCode.INTERNAL_ERROR.getHttpStatus(),
                ErrorCode.INTERNAL_ERROR,
                ErrorCode.INTERNAL_ERROR.getDefaultMessage(),
                baseData(ErrorCode.INTERNAL_ERROR),
                request,
                ex
        );
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Object>> handleUnexpectedException(
            Exception ex,
            HttpServletRequest request
    ) {
        return buildErrorResponse(
                ErrorCode.INTERNAL_ERROR.getHttpStatus(),
                ErrorCode.INTERNAL_ERROR,
                ErrorCode.INTERNAL_ERROR.getDefaultMessage(),
                baseData(ErrorCode.INTERNAL_ERROR),
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
        logHttpError(status, errorCode, request, traceId, ex);

        ApiResponse<Object> body = ApiResponse.<Object>builder()
                .status("ERROR")
                .statusCode(status.value())
                .message(LogFmt.sanitize(message != null ? message : errorCode.getDefaultMessage()))
                .data(data == null || data.isEmpty() ? null : data)
                .timestamp(Instant.now())
                .path(path)
                .traceId(traceId)
                .build();

        return ResponseEntity.status(status).body(body);
    }

    private void logHttpError(
            HttpStatus status,
            ErrorCode errorCode,
            HttpServletRequest request,
            String traceId,
            Throwable ex
    ) {
        String message = "event=http.error traceId={} status={} code={} method={} path={} query=\"{}\" remoteIp={} userAgent=\"{}\" errClass={} errMsg=\"{}\"";
        Object[] args = new Object[] {
                traceId,
                status.value(),
                errorCode.name(),
                safeLog(request.getMethod()),
                safeLog(request.getRequestURI()),
                safeLog(request.getQueryString()),
                safeLog(clientIp(request)),
                safeLog(request.getHeader("User-Agent")),
                ex.getClass().getSimpleName(),
                safeLog(ex.getMessage())
        };

        if (status.is5xxServerError()) {
            Object[] argsWithThrowable = new Object[args.length + 1];
            System.arraycopy(args, 0, argsWithThrowable, 0, args.length);
            argsWithThrowable[args.length] = ex;
            log.error(message, argsWithThrowable);
            return;
        }
        log.warn(message, args);
    }

    private Map<String, Object> baseData(ErrorCode errorCode) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("errorCode", errorCode.name());
        return data;
    }

    private String clientIp(HttpServletRequest request) {
        String forwardedFor = request.getHeader("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isBlank()) {
            return forwardedFor.split(",")[0].trim();
        }
        String realIp = request.getHeader("X-Real-IP");
        if (realIp != null && !realIp.isBlank()) {
            return realIp.trim();
        }
        return request.getRemoteAddr();
    }

    private String safeLog(String value) {
        String clean = LogFmt.sanitize(value);
        if (clean == null || clean.isBlank()) {
            return "NA";
        }
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
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
