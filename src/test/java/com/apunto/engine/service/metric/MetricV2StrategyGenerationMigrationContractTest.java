package com.apunto.engine.service.metric;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricV2StrategyGenerationMigrationContractTest {

    @Test
    void acquiresWriterLocksBeforeBackfillingRows() throws Exception {
        String migration = new ClassPathResource(
                "db/migration/V202607130007__metric_v2_strategy_generation_identity.sql"
        ).getContentAsString(StandardCharsets.UTF_8);

        int lock = migration.indexOf("LOCK TABLE");
        int firstBackfill = migration.indexOf("UPDATE futuros_operaciones.user_copy_allocation");

        assertTrue(migration.contains("SET lock_timeout = '60s'"));
        assertTrue(migration.contains("SET statement_timeout = '15min'"));
        assertTrue(migration.contains("IN ACCESS EXCLUSIVE MODE;"));
        assertTrue(lock >= 0 && lock < firstBackfill,
                "Writer locks must be acquired before any backfill UPDATE");
    }
}
