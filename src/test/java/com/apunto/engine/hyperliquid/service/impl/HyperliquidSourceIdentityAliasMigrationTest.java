package com.apunto.engine.hyperliquid.service.impl;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HyperliquidSourceIdentityAliasMigrationTest {

    @Test
    void migrationSeedsCanonicalAliasesWithoutReplacingExistingClaims() throws Exception {
        String resource = "/db/migration/V202607130006__hyperliquid_source_identity_alias.sql";
        try (InputStream stream = getClass().getResourceAsStream(resource)) {
            assertNotNull(stream, "canonical alias migration must exist");
            String sql = new String(stream.readAllBytes(), StandardCharsets.UTF_8).toLowerCase();
            assertTrue(sql.contains("hyperliquid:trade:"));
            assertTrue(sql.contains("hyperliquid:recovery:"));
            assertTrue(sql.contains("insert into futuros_operaciones.hyperliquid_direct_ingest_dedupe"));
            assertTrue(sql.contains("on conflict (idempotency_key) do nothing"));
        }
    }
}
