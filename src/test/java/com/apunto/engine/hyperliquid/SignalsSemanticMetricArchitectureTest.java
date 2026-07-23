package com.apunto.engine.hyperliquid;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SignalsSemanticMetricArchitectureTest {

    private static final List<String> REQUIRED = List.of(
            "replica_payload_conflict_total",
            "duplicate_noop_total",
            "semantic_classification_corrected_total",
            "semantic_classification_conflict_total",
            "baseline_created_total",
            "late_adjustment_resolved_total",
            "late_adjustment_unresolved_total",
            "estimated_flip_blocked_total",
            "legacy_identity_matched_total",
            "authoritative_identity_conflict_total",
            "position_lock_wait_duration",
            "worker_queue_depth",
            "worker_failures_total"
    );

    @Test
    void everyRequiredProductionMetricIsImplemented() throws IOException {
        String sources;
        try (var files = Files.walk(Path.of("src", "main", "java"))) {
            StringBuilder joined = new StringBuilder();
            for (Path file : files
                    .filter(path -> path.toString().endsWith(".java"))
                    .toList()) {
                joined.append(Files.readString(file));
            }
            sources = joined.toString();
        }

        List<String> missing = REQUIRED.stream()
                .filter(metric -> !sources.contains('"' + metric + '"'))
                .toList();
        assertEquals(List.of(), missing);
    }
}
