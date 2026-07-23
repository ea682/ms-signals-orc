package com.apunto.engine.service.copy.account;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionAccountMigrationContractTest {

    @Test
    void migrationPersistsImmutableAccountIdentityCapacityOwnershipAndCycleLineage() throws Exception {
        String migration;
        try (var stream = getClass().getResourceAsStream(
                "/db/migration/V202607180002__copy_execution_account_identity_and_micro_capacity.sql")) {
            assertTrue(stream != null, "execution account migration must exist");
            migration = new String(stream.readAllBytes(), StandardCharsets.UTF_8).toLowerCase();
        }

        assertTrue(migration.contains("account_purpose"));
        assertTrue(migration.contains("exchange_account_id"));
        assertTrue(migration.contains("source_position_cycle_id"));
        assertTrue(migration.contains("micro_live_capacity_exhausted"));
        assertTrue(migration.contains("micro_live_release_requires_flat"));
        assertTrue(migration.contains("copy_position_ownership"));
        assertTrue(migration.contains("unique") && migration.contains("upper(symbol)"));
        assertTrue(migration.contains("copy_flip_saga"));
        assertTrue(migration.contains("prevent_exchange_account_rebind"));
    }
}
