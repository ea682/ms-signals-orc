package com.apunto.engine.outbox.jobs;

import com.apunto.engine.outbox.dto.MetricOutboxRecord;
import com.apunto.engine.shared.util.LogFmt;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@Slf4j
@Component
@RequiredArgsConstructor
public class MetricMovementOutboxPublisher {

    private static final TypeReference<Map<String, Object>> JSON_MAP = new TypeReference<>() { };

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

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
        try {
            Map<String, Object> payload = objectMapper.readValue(record.payload(), JSON_MAP);
            String topic = topicFor(record.eventType());
            kafkaTemplate.send(topic, record.kafkaKey(), payload).get(Math.max(1000, publishTimeoutMs), TimeUnit.MILLISECONDS);
            markPublished(record.id());
            return true;
        } catch (JsonProcessingException ex) {
            markFailed(record.id(), "json_processing:" + ex.getOriginalMessage());
            log.error("event=metric_outbox.bad_payload outboxId={} errMsg=\"{}\"", record.id(), safe(ex.getOriginalMessage()), ex);
            return false;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            markFailed(record.id(), "interrupted");
            log.warn("event=metric_outbox.interrupted outboxId={} topic={}", record.id(), topicFor(record.eventType()));
            return false;
        } catch (KafkaException | DataAccessException | ExecutionException | TimeoutException ex) {
            markFailed(record.id(), ex.getClass().getSimpleName() + ":" + safe(ex.getMessage()));
            log.error("event=metric_outbox.publish_failed outboxId={} topic={} key={} errClass={} errMsg=\"{}\" {}",
                    record.id(), topicFor(record.eventType()), safe(record.kafkaKey()), ex.getClass().getSimpleName(), safe(ex.getMessage()),
                    LogFmt.kv("component", "metric_outbox_publisher"), ex);
            return false;
        }
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
}
