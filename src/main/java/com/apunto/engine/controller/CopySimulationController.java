package com.apunto.engine.controller;

import com.apunto.engine.service.copy.simulation.CopySimulationJobStore;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/internal/copy-simulations")
@RequiredArgsConstructor
public class CopySimulationController {

    private final CopySimulationJobStore store;

    @PostMapping("/{jobId}/pause")
    public ResponseEntity<Map<String, Object>> pause(@PathVariable UUID jobId) {
        boolean changed = store.requestPause(jobId);
        return changed
                ? ResponseEntity.accepted().body(Map.of("jobId", jobId, "status", "PAUSE_REQUESTED"))
                : ResponseEntity.notFound().build();
    }

    @PostMapping("/{jobId}/resume")
    public ResponseEntity<Map<String, Object>> resume(@PathVariable UUID jobId) {
        boolean changed = store.resume(jobId);
        return changed
                ? ResponseEntity.accepted().body(Map.of("jobId", jobId, "status", "PENDING"))
                : ResponseEntity.notFound().build();
    }
}
