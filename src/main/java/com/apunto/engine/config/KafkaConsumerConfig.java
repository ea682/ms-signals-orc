package com.apunto.engine.config;

import com.apunto.engine.events.OperacionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final CommonErrorHandler kafkaErrorHandler;

    @Value("${spring.kafka.consumer.properties.fetch.max.wait.ms:25}")
    private int fetchMaxWaitMs;

    @Value("${spring.kafka.consumer.properties.fetch.min.bytes:1}")
    private int fetchMinBytes;

    @Value("${spring.kafka.consumer.properties.max.poll.records:100}")
    private int maxPollRecords;

    @Bean
    public ConsumerFactory<String, OperacionEvent> consumerFactoryOperacionEvents() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OperacionEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.apunto.engine.events,com.apunto.engine.dto,com.apunto.engine.shared");

        log.info("event=kafka.consumer.config.applied fetchMaxWaitMs={} fetchMinBytes={} maxPollRecords={} enableAutoCommit={}",
                fetchMaxWaitMs, fetchMinBytes, maxPollRecords, props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "kafkaListenerContainerFactoryOperacionEvents")
    public ConcurrentKafkaListenerContainerFactory<String, OperacionEvent> kafkaListenerContainerFactoryOperacionEvents(
            ConsumerFactory<String, OperacionEvent> consumerFactoryOperacionEvents
    ) {
        ConcurrentKafkaListenerContainerFactory<String, OperacionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactoryOperacionEvents);
        factory.setCommonErrorHandler(kafkaErrorHandler);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setSyncCommits(true);

        return factory;
    }
}
