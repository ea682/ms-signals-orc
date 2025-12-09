package com.apunto.engine.shared.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {

    private String status;

    private int statusCode;

    private String message;

    private T data;

    private Instant timestamp;
    private String path;
    private String traceId;
}

