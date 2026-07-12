package com.apunto.engine.service.copy.recovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowEventDeadLetterStoreTest {

    @Test
    void claimKeepsOneInFlightEventPerPositionInStableFailureOrder() {
        CapturingJdbcTemplate jdbc = new CapturingJdbcTemplate();
        ShadowEventDeadLetterStore store = new ShadowEventDeadLetterStore(
                jdbc,
                new ObjectMapper().findAndRegisterModules(),
                new SimpleMeterRegistry()
        );

        store.claimRecoverable(50, 60_000L);

        String sql = jdbc.querySql.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
        assertTrue(sql.contains("not exists"));
        assertTrue(sql.contains("prior.status in ('recoverable', 'replaying')"));
        assertTrue(sql.contains("prior.position_key"));
        assertTrue(sql.contains("(prior.first_failed_at, prior.idempotency_key) < (d.first_failed_at, d.idempotency_key)"));
        assertTrue(sql.contains("order by d.first_failed_at, d.idempotency_key"));
        assertTrue(sql.contains("for update of d skip locked"));
    }

    private static final class CapturingJdbcTemplate extends JdbcTemplate {
        private String querySql;

        @Override
        public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args) {
            querySql = sql;
            return List.of();
        }
    }
}
