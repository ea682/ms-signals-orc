package com.apunto.engine.service.copy.simulation;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopySimulationMigrationContractTest {

    @Test
    void migrationMakesMatrixDurableIdempotentPausableAndResumable() throws Exception {
        String sql = Files.readString(Path.of(
                "src/main/resources/db/migration/V202607130003__copy_simulation_certification_v3.sql"));

        assertTrue(sql.contains("copy_simulation_job_v3"));
        assertTrue(sql.contains("copy_capital_leverage_simulation_v3"));
        assertTrue(sql.contains("copy_liquidity_job_v3"));
        assertTrue(sql.contains("copy_order_book_snapshot_v3"));
        assertTrue(sql.contains("copy_liquidity_simulation_v3"));
        assertTrue(sql.contains("resume_cursor"));
        assertTrue(sql.contains("pause_requested"));
        assertTrue(sql.contains("input_hash"));
        assertTrue(sql.contains("UNIQUE (job_id, capital_usd, target_leverage)"));
        assertTrue(sql.contains("simulation_only BOOLEAN NOT NULL DEFAULT TRUE"));
        assertTrue(sql.contains("evidence_level <> 'REAL_VALIDATED'"));
        assertFalse(sql.contains("copy_dispatch_intent_id"));
        assertFalse(sql.contains("binance_order_id"));
    }

    @Test
    void additiveMigrationExpandsMatrixAndPreservesUnknownEvidence() throws Exception {
        String sql = Files.readString(Path.of(
                "src/main/resources/db/migration/V202607140001__institutional_financial_simulation_v3.sql"));

        assertTrue(sql.contains("resume_cursor BETWEEN 0 AND 44"));
        assertTrue(sql.contains("scenario_index BETWEEN 0 AND 43"));
        assertTrue(sql.contains("strategy_key"));
        assertTrue(sql.contains("generation_id"));
        assertTrue(sql.contains("economic_evidence"));
        assertTrue(sql.contains("UNKNOWN_HISTORICAL_EXECUTION_EVIDENCE"));
        assertTrue(sql.contains("modeled_economics_status TYPE VARCHAR(64)"));
        assertTrue(sql.contains("execution_mode IN ('SHADOW', 'MICRO_LIVE', 'LIVE')"));
        assertFalse(sql.contains("DELETE FROM"));
        assertFalse(sql.contains("TRUNCATE"));
    }
}
