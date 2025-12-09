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

import java.util.Map;

@Slf4j
@Component
@AllArgsConstructor
public class OperacionEventListener {

    private final OperationCoreService operationCoreService;

    @KafkaListener(
            topics = "${app.kafka.operaciones.topic:operaciones-eventos}",
            groupId = "${spring.kafka.consumer.group-id:operaciones-consumer}",
            containerFactory = "kafkaListenerContainerFactoryOperacionEvents"
    )

    public void onOperacionEvent(
            @Payload OperacionEvent event,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Headers Map<String, Object> headers
    ) {
        log.info("OperacionEvent recibido: key={}, partition={}, offset={}, topic={}",
                key, partition, offset, topic);
        log.debug("Headers: {}", headers);
        log.info("Evento: {}", event);

        operationCoreService.procesarEventoOperacion(event);
    }
}