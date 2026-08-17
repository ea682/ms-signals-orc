package com.apunto.engine.hyperliquid.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperliquidKafkaIngestConfigurationContractTest {

    @Test
    void productionEnablesOneStableTopicGroupAndSafeNewGroupOffsetPolicy() throws IOException {
        String base = Files.readString(Path.of("src/main/resources/application.yml"));
        String production = Files.readString(Path.of("src/main/resources/application-prod.yml"));
        String environmentExample = Files.readString(Path.of(".env.prod.example"));

        assertTrue(base.contains(
                "HYPERLIQUID_DELTA_KAFKA_TOPIC:hyperliquid-position-deltas"));
        assertTrue(base.contains(
                "HYPERLIQUID_DELTA_KAFKA_GROUP_ID:ms-signals-orc-hyperliquid-position-deltas-v1"));
        assertTrue(base.contains(
                "HYPERLIQUID_KAFKA_INGEST_AUTO_OFFSET_RESET:latest"));
        assertTrue(production.contains(
                "HYPERLIQUID_KAFKA_INGEST_ENABLED:true"));
        assertTrue(environmentExample.contains(
                "HYPERLIQUID_KAFKA_INGEST_ENABLED=true"));
        assertTrue(environmentExample.contains(
                "HYPERLIQUID_DELTA_KAFKA_TOPIC=hyperliquid-position-deltas"));
        assertTrue(environmentExample.contains(
                "HYPERLIQUID_DELTA_KAFKA_GROUP_ID=ms-signals-orc-hyperliquid-position-deltas-v1"));
        assertTrue(environmentExample.contains(
                "HYPERLIQUID_KAFKA_INGEST_AUTO_OFFSET_RESET=latest"));
    }
}
