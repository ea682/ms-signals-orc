package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDispatchPayloadConflictMigrationContractTest {

    @Test
    void persistsOneSanitizedAuditableConflictPerIntentAndIncomingHash() throws IOException {
        String sql = Files.readString(Path.of(
                "src/main/resources/db/migration/V202607130004__copy_dispatch_payload_conflict_v3.sql"));
        String normalized = sql.toLowerCase(Locale.ROOT);

        assertTrue(normalized.contains("create table if not exists futuros_operaciones.copy_dispatch_payload_conflict"));
        assertTrue(normalized.contains("existing_payload jsonb"));
        assertTrue(normalized.contains("incoming_payload jsonb"));
        assertTrue(normalized.contains("field_diff jsonb"));
        assertTrue(normalized.contains("unique (dispatch_intent_id, incoming_hash)"));
        assertTrue(normalized.contains("alert_status varchar"));
        assertTrue(normalized.contains("conflict_count integer"));
        assertFalse(normalized.contains("api_key"));
        assertFalse(normalized.contains("api_secret"));
    }
}
