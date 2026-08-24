package com.apunto.engine.outbox.jobs;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricHandoffObservabilityTest {

    @Test
    void exposesExpiredClaimsAndDurableOutboxBacklog() {
        JdbcTemplate jdbcTemplate = jdbcTemplate(2L, Map.of(
                "pending_count", 3L,
                "oldest_age_seconds", 75L));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricHandoffObservability observability =
                new MetricHandoffObservability(jdbcTemplate, registry);
        enable(observability);

        observability.refresh();

        assertEquals(2.0, gauge(registry,
                "signals.hyperliquid.direct_ingest.processing.stale"));
        assertEquals(3.0, gauge(registry,
                "signals.metric_outbox.pending"));
        assertEquals(75.0, gauge(registry,
                "signals.metric_outbox.oldest.pending.age.seconds"));
        assertEquals(1.0, registry.counter(
                "signals.metric_outbox.observation.refresh",
                "result", "success").count());
    }

    @Test
    void databaseFailureIsVisibleAndDoesNotInventHealthyValues() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate() {
            @Override
            public <T> T queryForObject(String sql, Class<T> requiredType) {
                throw new DataAccessResourceFailureException("offline");
            }
        };
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricHandoffObservability observability =
                new MetricHandoffObservability(jdbcTemplate, registry);
        enable(observability);

        observability.refresh();

        assertEquals(1.0, registry.counter(
                "signals.metric_outbox.observation.refresh",
                "result", "failure").count());
        assertEquals(0.0, gauge(registry,
                "signals.metric_outbox.pending"));
    }

    private double gauge(SimpleMeterRegistry registry, String name) {
        return registry.get(name).gauge().value();
    }

    private JdbcTemplate jdbcTemplate(
            long stale,
            Map<String, Object> backlog
    ) {
        return new JdbcTemplate() {
            @Override
            public <T> T queryForObject(String sql, Class<T> requiredType) {
                return requiredType.cast(stale);
            }

            @Override
            public Map<String, Object> queryForMap(String sql) {
                return backlog;
            }
        };
    }

    private void enable(MetricHandoffObservability observability) {
        try {
            Field enabled = MetricHandoffObservability.class
                    .getDeclaredField("enabled");
            enabled.setAccessible(true);
            enabled.setBoolean(observability, true);
        } catch (ReflectiveOperationException ex) {
            throw new AssertionError(ex);
        }
    }
}
