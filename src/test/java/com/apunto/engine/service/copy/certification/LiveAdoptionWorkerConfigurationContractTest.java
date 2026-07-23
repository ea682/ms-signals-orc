package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveAdoptionWorkerConfigurationContractTest {

    @Test
    void criticalWorkerRequiresAnExplicitEnableFlag() throws IOException {
        String worker = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/jobs/LiveAdoptionReconciliationWorker.java"));
        String application = Files.readString(Path.of("src/main/resources/application.yml"));
        String production = Files.readString(Path.of(".env.prod.example"));

        assertFalse(worker.contains("matchIfMissing = true"));
        assertTrue(application.contains("${COPY_LIVE_ADOPTION_RECONCILIATION_ENABLED:false}"));
        assertTrue(production.contains("COPY_LIVE_ADOPTION_RECONCILIATION_ENABLED=true"));
    }
}
