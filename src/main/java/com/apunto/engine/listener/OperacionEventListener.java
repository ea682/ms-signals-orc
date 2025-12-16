package com.apunto.engine.listener;


import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.service.OperationCoreService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@AllArgsConstructor
public class OperacionEventListener {
    private static final long SLOW_MS = 300;

    private final OperationCoreService operationCoreService;

    @KafkaListener(
            topics = "${app.kafka.operaciones.topic:operaciones-eventos}",
            groupId = "${spring.kafka.consumer.group-id:operaciones-consumer}",
            containerFactory = "kafkaListenerContainerFactoryOperacionEvents"
    )

    public void listenOperacionEvent(
            @Payload OperacionEvent event,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic
    ) {
        long startNs = System.nanoTime();

        Instant created = event != null && event.getOperacion() != null ? event.getOperacion().getFechaCreacion() : null;
        long lagMs = created != null ? (System.currentTimeMillis() - created.toEpochMilli()) : -1;

        String opId = event != null && event.getOperacion() != null && event.getOperacion().getIdOperacion() != null
                ? event.getOperacion().getIdOperacion().toString()
                : "null";

        String tipo = event != null && event.getTipo() != null ? event.getTipo().name() : "null";

        log.info("event=kafka.received topic={} partition={} offset={} key={} tipo={} opId={} lagMs={}",
                topic, partition, offset, key, tipo, opId, lagMs);

        operationCoreService.procesarEventoOperacion(event);

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        if (durationMs >= SLOW_MS) {
            log.warn("event=kafka.process_slow opId={} tipo={} durationMs={} lagMs={}", opId, tipo, durationMs, lagMs);
        } else {
            log.debug("event=kafka.process_ok opId={} tipo={} durationMs={}", opId, tipo, durationMs);
        }
    }
}