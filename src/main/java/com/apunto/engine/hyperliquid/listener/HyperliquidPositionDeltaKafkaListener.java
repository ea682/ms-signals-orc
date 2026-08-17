package com.apunto.engine.hyperliquid.listener;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaAcceptedResponse;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.apunto.engine.hyperliquid.metric.HyperliquidIngestTransportMetrics;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectDeltaIngestService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
@ConditionalOnProperty(
        name = "hyperliquid.kafka-ingest.enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
public class HyperliquidPositionDeltaKafkaListener {

    public static final String LISTENER_ID = "hyperliquidPositionDeltaKafkaListener";

    private final HyperliquidDeltaOperacionMapper mapper;
    private final HyperliquidDirectDeltaIngestService ingestService;
    private final Validator validator;
    private final HyperliquidIngestTransportMetrics metrics;

    @KafkaListener(
            id = LISTENER_ID,
            topics = "${hyperliquid.kafka-ingest.topic}",
            groupId = "${hyperliquid.kafka-ingest.group-id}",
            concurrency = "${hyperliquid.kafka-ingest.concurrency:1}",
            containerFactory = "kafkaListenerContainerFactoryHyperliquidPositionDeltas"
    )
    public void listen(
            ConsumerRecord<String, HyperliquidDeltaRequest> record,
            Acknowledgment acknowledgment
    ) {
        metrics.kafkaReceived();
        MDC.put("correlationId", correlationId(record));
        MDC.put("kafka.topic", record.topic());
        MDC.put("kafka.partition", Integer.toString(record.partition()));
        MDC.put("kafka.offset", Long.toString(record.offset()));
        try {
            HyperliquidDeltaRequest request = requiredValue(record);
            Set<ConstraintViolation<HyperliquidDeltaRequest>> violations = validator.validate(request);
            if (!violations.isEmpty()) {
                throw new ConstraintViolationException(violations);
            }

            HyperliquidMappedDelta mapped = mapper.map(request, record.key());
            HyperliquidDeltaAcceptedResponse response = ingestService.accept(mapped);
            metrics.kafkaAccepted(response.duplicate());
            acknowledgment.acknowledge();
            log.info("event=hyperliquid.kafka_ingest.accepted topic={} partition={} offset={} idempotencyKey={} positionKey={} deltaType={} duplicate={} queueDepth={} decision=CANONICAL_INGEST_ACK",
                    record.topic(), record.partition(), record.offset(), response.idempotencyKey(),
                    response.positionKey(), response.deltaType(), response.duplicate(), response.queueDepth());
        } catch (ConstraintViolationException | IllegalArgumentException invalid) {
            metrics.kafkaFailed("validation_error");
            log.error("event=hyperliquid.kafka_ingest.failed reasonCode=validation_error topic={} partition={} offset={} errClass={} violationCount={} decision=NO_ACK",
                    record.topic(), record.partition(), record.offset(),
                    invalid.getClass().getSimpleName(), violationCount(invalid));
            throw invalid;
        } catch (RuntimeException failure) {
            metrics.kafkaFailed("ingest_error");
            log.error("event=hyperliquid.kafka_ingest.failed reasonCode=ingest_error topic={} partition={} offset={} errClass={} errMsg=\"{}\" decision=NO_ACK",
                    record.topic(), record.partition(), record.offset(),
                    failure.getClass().getSimpleName(), safeLog(failure.getMessage()));
            throw failure;
        } finally {
            MDC.clear();
        }
    }

    private HyperliquidDeltaRequest requiredValue(ConsumerRecord<String, HyperliquidDeltaRequest> record) {
        if (record.value() == null) {
            throw new IllegalArgumentException("Hyperliquid Kafka record value is required");
        }
        return record.value();
    }

    private int violationCount(RuntimeException invalid) {
        if (invalid instanceof ConstraintViolationException constraintViolation) {
            return constraintViolation.getConstraintViolations().size();
        }
        return 1;
    }

    private String correlationId(ConsumerRecord<String, HyperliquidDeltaRequest> record) {
        if (record.key() != null && !record.key().isBlank()) {
            return record.key();
        }
        return "%s:%d:%d".formatted(record.topic(), record.partition(), record.offset());
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 300 ? clean.substring(0, 300) : clean;
    }
}
