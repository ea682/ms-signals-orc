package com.apunto.engine.service.copy.coverage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowCoverageMigrationContractTest {

    private static final String MIGRATION = "/db/migration/V202607100001__shadow_promotion_rolling_coverage_index.sql";

    @Test
    void migrationCreatesAllocationTimeIndexWithDeterministicTieBreaker() throws IOException {
        String sql = migrationSql();

        assertTrue(sql.contains("shadow_allocation_id, event_time desc, id_event desc"));
        assertTrue(sql.contains("include (decision)"));
    }

    @Test
    void migrationIndexIsPartialForExactlyTheEvaluableDecisions() throws IOException {
        String sql = migrationSql();

        assertTrue(sql.contains("decision in ('simulated', 'recorded', 'skipped', 'error')"));
        assertTrue(sql.contains("event_time is not null"));
    }

    @Test
    void migrationNeverMutatesOrDeletesHistoricalEvents() throws IOException {
        String sql = migrationSql();

        assertTrue(sql.contains("create index"));
        assertTrue(!sql.contains("delete from"));
        assertTrue(!sql.contains("update futuros_operaciones.shadow_copy_operation_event"));
        assertTrue(!sql.contains("truncate"));
    }

    private static String migrationSql() throws IOException {
        try (InputStream stream = ShadowCoverageMigrationContractTest.class.getResourceAsStream(MIGRATION)) {
            assertNotNull(stream, "rolling coverage migration must exist");
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8)
                    .toLowerCase(Locale.ROOT)
                    .replaceAll("\\s+", " ")
                    .trim();
        }
    }
}
