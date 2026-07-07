package com.apunto.engine.controller;

import com.apunto.engine.dto.CopyExecutionAccountsDiagnostics;
import com.apunto.engine.service.CopyExecutionAccountsDiagnosticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/v1/copy")
public class CopyExecutionDiagnosticsController {

    private final CopyExecutionAccountsDiagnosticsService diagnosticsService;

    @GetMapping("/execution-accounts/diagnostics")
    public CopyExecutionAccountsDiagnostics diagnostics() {
        return diagnosticsService.snapshot();
    }
}
