package com.apunto.engine.listener;

import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.metric.TradingMetrics;
import com.apunto.engine.service.OperacionEventIngestService;
import com.apunto.engine.shared.exception.EngineException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

@Slf4j
@Component
@ConditionalOnProperty(
        name = "engine.copy.legacy-kafka-listener.enabled",
        havingValue = "true",
        matchIfMissing = false
)
@RequiredArgsConstructor
public class OperacionEventListener {

    private final OperacionEventIngestService operacionEventIngestService;
    private final TradingMetrics tradingMetrics;

    @KafkaListener(
            topics = "${app.kafka.operaciones.topic:${KAFKA_TOPIC_OPERACIONES:operaciones-eventos}}",
            groupId = "${spring.kafka.consumer.group-id:operaciones-consumer}",
            containerFactory = "kafkaListenerContainerFactoryOperacionEvents"
    )
    public void listenOperacionEvent(
            @Payload OperacionEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = KafkaHeaders.RECEIVED_TIMESTAMP, required = false) Long timestamp,
            @Header(name = "correlationId", required = false) String correlationId,
            Acknowledgment acknowledgment
    ) {
        final String corr = (correlationId != null && !correlationId.isBlank())
                ? correlationId
                : "%s:%d:%d".formatted(topic, partition, offset);

        MDC.put("correlationId", corr);
        MDC.put("kafka.topic", topic);
        MDC.put("kafka.partition", String.valueOf(partition));
        MDC.put("kafka.offset", String.valueOf(offset));

        try {
            tradingMetrics.kafkaReceived(topic);
            log.info("event=kafka.received key={} ts={}", key, timestamp);

            operacionEventIngestService.ingest(event);

            acknowledgment.acknowledge();
            log.info("event=kafka.ack key={}", key);

        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException e) {
            log.error("event=kafka.handler.error key={} errClass={} errMsg=\"{}\"",
                    key, e.getClass().getSimpleName(), safeLog(e.getMessage()), e);
            throw e;
        } finally {
            MDC.clear();
        }
    }

    private String safeLog(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }
}
