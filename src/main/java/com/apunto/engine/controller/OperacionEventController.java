package com.apunto.engine.controller;

import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.service.OperacionEventIngestService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/operacion-event")
@RequiredArgsConstructor
public class OperacionEventController {

    private final OperacionEventIngestService operacionEventIngestService;

    @PostMapping
    public ResponseEntity<String> receiveEvent(@RequestBody OperacionEvent event) {
        int jobs = operacionEventIngestService.ingest(event);
        return ResponseEntity.accepted().body("Enqueued jobs: " + jobs);
    }
}
