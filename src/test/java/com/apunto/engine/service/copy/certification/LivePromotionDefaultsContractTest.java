package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LivePromotionDefaultsContractTest {

    @Test
    void promotionWorkersDefaultToDisabledWhileAutomaticCertificationNeedsNoManualBypass() throws IOException {
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
        assertTrue(yaml.contains("manual-certification-required: ${COPY_LIVE_PROMOTION_MANUAL_CERTIFICATION_REQUIRED:false}"));
        assertTrue(worker.contains("${copy.live-promotion.job.enabled:false}"));
        assertFalse(worker.contains("${copy.live-promotion.job.enabled:true}"));
    }

    @Test
    void directCrossExchangeSlippageIsNotExposedAsAPromotionGate() throws IOException {
        String yaml = Files.readString(Path.of("src/main/resources/application.yml"));
        String properties = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/service/copy/promotion/LivePromotionProperties.java"));

        assertFalse(yaml.contains("COPY_LIVE_PROMOTION_MIN_SLIPPAGE_SAMPLES"));
        assertFalse(yaml.contains("COPY_LIVE_PROMOTION_MAX_ADVERSE_SLIPPAGE_P95_BPS"));
        assertFalse(properties.contains("minSlippageSamples"));
        assertFalse(properties.contains("maxAdverseSlippageP95Bps"));
    }

    @Test
    void environmentExamplesNeverDeclareTheSameVariableTwice() throws IOException {
        for (String filename : List.of(".env.prod.example", ".env.b2b.example")) {
            List<String> keys = Files.readAllLines(Path.of(filename)).stream()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#") && line.contains("="))
                    .map(line -> line.substring(0, line.indexOf('=')))
                    .toList();

            assertEquals(keys.stream().distinct().count(), keys.size(),
                    () -> filename + " contains duplicate environment variables");
        }
    }

    @Test
    void productionDatasourceSetsTheSchemaUsedByNativeCopyQueries() throws IOException {
        String productionYaml = Files.readString(Path.of("src/main/resources/application-prod.yml"));
        String productionEnvironment = Files.readString(Path.of(".env.prod.example"));

        assertTrue(productionYaml.contains("schema: ${DB_SCHEMA:futuros_operaciones}"));
        assertTrue(productionEnvironment.contains("?currentSchema=futuros_operaciones"));
    }

    @Test
    void localProfileNeverProvidesAVersionedDatabasePasswordDefault() throws IOException {
        String localYaml = Files.readString(Path.of("src/main/resources/application-local.yml"));

        assertTrue(localYaml.contains("password: ${DB_PASSWORD}"));
        assertFalse(localYaml.matches("(?s).*password:\\s*\\$\\{DB_PASSWORD:[^}]+}.*"));
    }

    @Test
    void b2bEnvironmentComposesProductionInfrastructureWithTheSafetyProfile() throws IOException {
        String b2bEnvironment = Files.readString(Path.of(".env.b2b.example"));

        assertTrue(b2bEnvironment.contains("SPRING_PROFILES_ACTIVE=prod,b2b"));
    }
}
