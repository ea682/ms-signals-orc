package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LivePromotionDefaultsContractTest {

    @Test
    void livePromotionAndScheduledWorkerDefaultToDisabled() throws IOException {
        String yaml = Files.readString(Path.of("src/main/resources/application.yml"));
        String shadowProperties = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/service/copy/promotion/ShadowPromotionProperties.java"));
        String shadowWorker = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/jobs/ShadowPromotionJobWorker.java"));
        String worker = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/jobs/MicroLivePromotionJobWorker.java"));

        assertTrue(yaml.contains("enabled: ${COPY_PROMOTION_ENABLED:false}"));
        assertTrue(yaml.contains("enabled: ${COPY_PROMOTION_JOB_ENABLED:false}"));
        assertTrue(shadowProperties.contains("private boolean enabled = false;"));
        assertTrue(shadowWorker.contains("${copy.promotion.job.enabled:false}"));
        assertFalse(shadowWorker.contains("${copy.promotion.job.enabled:true}"));
        assertTrue(yaml.contains("enabled: ${COPY_LIVE_PROMOTION_ENABLED:false}"));
        assertTrue(yaml.contains("enabled: ${COPY_LIVE_PROMOTION_JOB_ENABLED:false}"));
        assertTrue(yaml.contains("manual-certification-required: ${COPY_LIVE_PROMOTION_MANUAL_CERTIFICATION_REQUIRED:true}"));
        assertTrue(worker.contains("${copy.live-promotion.job.enabled:false}"));
        assertFalse(worker.contains("${copy.live-promotion.job.enabled:true}"));
    }
}
