package com.apunto.engine.controller;

import com.apunto.engine.service.copy.calibration.CalibrationSegment;
import com.apunto.engine.service.copy.calibration.ExecutionCalibrationMetric;
import com.apunto.engine.service.copy.calibration.ExecutionCalibrationReadService;
import com.apunto.engine.service.copy.calibration.ExecutionCalibrationResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/v1/copy")
public class CopyExecutionCalibrationController {

    private final ExecutionCalibrationReadService service;

    @GetMapping("/execution-calibration")
    public ExecutionCalibrationResponse estimate(
            @RequestParam String strategyKey,
            @RequestParam String generationId,
            @RequestParam String metric,
            @RequestParam(defaultValue = "ALL") String symbol,
            @RequestParam(defaultValue = "ALL") String side,
            @RequestParam(defaultValue = "ALL") String action,
            @RequestParam(defaultValue = "ALL") String notionalBand) {
        try {
            return service.estimate(strategyKey, generationId,
                    ExecutionCalibrationMetric.parse(metric),
                    new CalibrationSegment(symbol, side, action, notionalBand),
                    OffsetDateTime.now(ZoneOffset.UTC));
        } catch (IllegalArgumentException error) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, error.getMessage());
        }
    }
}
