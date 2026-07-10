package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyIncidentDryRunContractTest {

    private static final Path SCRIPT = Path.of("scripts/reconcile-copy-dispatch-incident.sql");

    @Test
    void orphanFilledOrderIsDetected() throws IOException {
        String sql = script().toLowerCase(Locale.ROOT);

        assertTrue(sql.contains("left join futuros_operaciones.copy_operation"));
        assertTrue(sql.contains("o.id_operation is null"));
        assertTrue(sql.contains("legacy orphan orders without a durable intent"));
        assertTrue(sql.contains("array_agg(order_id"));
    }

    @Test
    void dryRunNeverModifiesBinance() throws IOException {
        String sql = script().toLowerCase(Locale.ROOT);

        assertTrue(sql.contains("\\set apply 0"));
        assertTrue(sql.contains("\\if :apply_requested"));
        assertTrue(sql.contains("i_understand_db_backfill_only"));
        assertFalse(sql.contains("api_key"));
        assertFalse(sql.contains("api_secret"));
        assertFalse(sql.contains("delete from"));
        assertFalse(sql.contains("insert into"));
    }

    @Test
    void applyOnlyAcceptsOneExactlyMappedBinanceOrder() throws IOException {
        String sql = script().toLowerCase(Locale.ROOT);

        assertTrue(sql.contains("safe_binance_order"));
        assertTrue(sql.contains("b.user_copy_allocation_id is not null"));
        assertTrue(sql.contains("b.source_event_id is not null"));
        assertTrue(sql.contains("having count(*) = 1"));
    }

    private String script() throws IOException {
        assertTrue(Files.isRegularFile(SCRIPT), "missing dry-run script: " + SCRIPT.toAbsolutePath());
        return Files.readString(SCRIPT);
    }
}
