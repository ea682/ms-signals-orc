package com.apunto.engine.config;

import com.apunto.engine.events.OperacionEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public ConsumerFactory<String, OperacionEvent> consumerFactoryOperacionEvents() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        JsonDeserializer<OperacionEvent> valueDeserializer = new JsonDeserializer<>(OperacionEvent.class, false);
        valueDeserializer.addTrustedPackages("*");
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                valueDeserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OperacionEvent> kafkaListenerContainerFactoryOperacionEvents(
            ConsumerFactory<String, OperacionEvent> consumerFactoryOperacionEvents,
            CommonErrorHandler operacionesErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, OperacionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactoryOperacionEvents);
        factory.setCommonErrorHandler(operacionesErrorHandler);
        return factory;
    }
}
