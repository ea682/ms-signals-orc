package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveCertificationMigrationContractTest {

    @Test
    void migrationPersistsExactManualCertificationAdoptionAndActivationAudit() throws Exception {
        String sql = Files.readString(Path.of(
                "src/main/resources/db/migration/V202607130003__copy_simulation_certification_v3.sql"));

        assertTrue(sql.contains("strategy_live_certification"));
        assertTrue(sql.contains("creation_key VARCHAR(240) NOT NULL UNIQUE"));
        assertTrue(sql.contains("automatic_promotion_enabled BOOLEAN NOT NULL DEFAULT FALSE"));
        assertTrue(sql.contains("strategy_live_certification_audit"));
        assertTrue(sql.contains("prior_version BIGINT NOT NULL"));
        assertTrue(sql.contains("next_version BIGINT NOT NULL"));
        assertTrue(sql.contains("user_live_certification_adoption"));
        assertTrue(sql.contains("assigned_capital_usd"));
        assertTrue(sql.contains("observed_at TIMESTAMPTZ NOT NULL"));
        assertTrue(sql.contains("live_allocation_activation_audit"));
        assertTrue(sql.contains("prior_mode = 'MICRO_LIVE' AND next_mode = 'LIVE'"));
        assertTrue(sql.contains("ALTER TABLE futuros_operaciones.user_copy_allocation"));
        assertFalse(sql.contains("ALTER TABLE user_copy_allocation"));
        assertFalse(sql.contains("REFERENCES strategy_live_certification(id) ON DELETE CASCADE"));
    }
}
