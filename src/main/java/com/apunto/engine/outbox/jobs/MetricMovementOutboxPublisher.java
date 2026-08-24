package com.apunto.engine.outbox.jobs;

import com.apunto.engine.outbox.dto.MetricOutboxRecord;
import com.apunto.engine.shared.util.LogFmt;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class MetricMovementOutboxPublisher {

    private static final TypeReference<Map<String, Object>> JSON_MAP = new TypeReference<>() { };

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final AtomicLong lastSuccessEpochSeconds = new AtomicLong();
    private final AtomicLong lastFailureEpochSeconds = new AtomicLong();

    @Value("${metric.outbox.publisher.enabled:false}")
    private boolean enabled;

    @Value("${metric.outbox.topics.movement:${metric.outbox.topic:operation-movement-persisted-v1}}")
    private String movementTopic;

    @Value("${metric.outbox.topics.copy-operation-event:copy-operation-event-persisted-v1}")
    private String copyOperationEventTopic;

    @Value("${metric.outbox.publisher.batch-size:100}")
    private int batchSize;

    @Value("${metric.outbox.publisher.lock-timeout-ms:60000}")
    private long lockTimeoutMs;

    @Value("${metric.outbox.publisher.publish-timeout-ms:30000}")
    private long publishTimeoutMs;

    @Value("${metric.outbox.publisher.instance-id:${spring.application.name:ms-signals-orc}}")
    private String instanceId;

    @PostConstruct
    void registerMetrics() {
        io.micrometer.core.instrument.Gauge.builder(
                        "signals.metric_outbox.publisher.last.success.epoch.seconds",
                        lastSuccessEpochSeconds,
                        AtomicLong::get)
                .description("Epoch second of the latest durably acknowledged Kafka publication")
                .register(meterRegistry);
        io.micrometer.core.instrument.Gauge.builder(
                        "signals.metric_outbox.publisher.last.failure.epoch.seconds",
                        lastFailureEpochSeconds,
                        AtomicLong::get)
                .description("Epoch second of the latest Kafka publication failure")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${metric.outbox.publisher.poll-ms:2000}")
    public void publishPending() {
        if (!enabled) {
            return;
        }
        List<MetricOutboxRecord> records = claimPending();
        if (records.isEmpty()) {
            return;
        }
        int published = 0;
        for (MetricOutboxRecord record : records) {
            if (publish(record)) {
                published++;
            }
        }
        log.info("event=metric_outbox.batch published={} claimed={} instanceId={}", published, records.size(), safe(instanceId));
    }

    private List<MetricOutboxRecord> claimPending() {
        int safeBatchSize = Math.max(1, Math.min(batchSize, 1000));
        return jdbcTemplate.query(
                """
                WITH picked AS (
                    SELECT id
                    FROM futuros_operaciones.metric_event_outbox
                    WHERE published_at IS NULL
                      AND (locked_at IS NULL OR locked_at < now() - CAST(? AS bigint) * interval '1 millisecond')
                    ORDER BY created_at ASC, id ASC
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE futuros_operaciones.metric_event_outbox o
                SET locked_at = now(), locked_by = ?, attempts = attempts + 1
                FROM picked
                WHERE o.id = picked.id
                RETURNING o.id, o.event_type, o.kafka_key, o.payload::text, o.created_at, o.attempts
                """,
                (rs, rowNum) -> new MetricOutboxRecord(
                        rs.getLong("id"),
                        rs.getString("event_type"),
                        rs.getString("kafka_key"),
                        rs.getString("payload"),
                        toOffsetDateTime(rs.getTimestamp("created_at")),
                        rs.getInt("attempts")
                ),
                lockTimeoutMs,
                safeBatchSize,
                instanceId
        );
    }

    private boolean publish(MetricOutboxRecord record) {
        meterRegistry.counter(
                "signals.metric_outbox.publish",
                "result", "attempt").increment();
        try {
            Map<String, Object> payload = objectMapper.readValue(record.payload(), JSON_MAP);
            String topic = topicFor(record.eventType());
            kafkaTemplate.send(topic, record.kafkaKey(), payload).get(Math.max(1000, publishTimeoutMs), TimeUnit.MILLISECONDS);
            markPublished(record.id());
            lastSuccessEpochSeconds.set(
                    java.time.Instant.now().getEpochSecond());
            meterRegistry.counter(
                    "signals.metric_outbox.publish",
                    "result", "success").increment();
            if ("operation-movement-persisted-v1".equals(record.eventType())) {
                meterRegistry.counter(
                        "signals_movement_event_published_total").increment();
            }
            return true;
        } catch (JsonProcessingException ex) {
            recordPublishFailure("bad_payload");
            markFailed(record.id(), "json_processing:" + ex.getOriginalMessage());
            log.error("event=metric_outbox.bad_payload outboxId={} errMsg=\"{}\"", record.id(), safe(ex.getOriginalMessage()), ex);
            return false;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            recordPublishFailure("interrupted");
            markFailed(record.id(), "interrupted");
            log.warn("event=metric_outbox.interrupted outboxId={} topic={}", record.id(), topicFor(record.eventType()));
            return false;
        } catch (KafkaException | DataAccessException | ExecutionException | TimeoutException ex) {
            recordPublishFailure(ex.getClass().getSimpleName());
            markFailed(record.id(), ex.getClass().getSimpleName() + ":" + safe(ex.getMessage()));
            log.error("event=metric_outbox.publish_failed outboxId={} topic={} key={} errClass={} errMsg=\"{}\" {}",
                    record.id(), topicFor(record.eventType()), safe(record.kafkaKey()), ex.getClass().getSimpleName(), safe(ex.getMessage()),
                    LogFmt.kv("component", "metric_outbox_publisher"), ex);
            return false;
        }
    }

    private void recordPublishFailure(String reason) {
        lastFailureEpochSeconds.set(
                java.time.Instant.now().getEpochSecond());
        meterRegistry.counter(
                "signals.metric_outbox.publish",
                "result", "failure",
                "reason", safeTag(reason)).increment();
    }


    private String topicFor(String eventType) {
        if ("copy-operation-event-persisted-v1".equals(eventType)) {
            return copyOperationEventTopic;
        }
        return movementTopic;
    }

    private void markPublished(long id) {
        jdbcTemplate.update(
                "UPDATE futuros_operaciones.metric_event_outbox SET published_at = now(), locked_at = NULL, locked_by = NULL WHERE id = ?",
                id
        );
    }

    private void markFailed(long id, String error) {
        jdbcTemplate.update(
                "UPDATE futuros_operaciones.metric_event_outbox SET locked_at = NULL, locked_by = NULL, last_error = ? WHERE id = ?",
                truncate(error, 1000),
                id
        );
    }

    private OffsetDateTime toOffsetDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant().atOffset(ZoneOffset.UTC);
    }

    private String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    private String safe(Object value) {
        return value == null ? "null" : String.valueOf(value).replace('\n', '_').replace('\r', '_');
    }

    private String safeTag(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT)
                .replaceAll("[^a-z0-9_.-]", "_");
        return normalized.length() <= 80
                ? normalized
                : normalized.substring(0, 80);
    }
}
