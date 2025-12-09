package com.apunto.engine.config;

import com.apunto.engine.events.OperacionEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String operacionesConsumerGroupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:latest}")
    private String autoOffsetReset;

    @Bean
    public Map<String, Object> operacionesConsumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, operacionesConsumerGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        // Decirle cuál es el tipo por defecto del value
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OperacionEvent.class);

        return props;
    }

    @Bean
    public ConsumerFactory<String, OperacionEvent> consumerFactoryOperacionEvents() {
        return new DefaultKafkaConsumerFactory<>(operacionesConsumerConfigs());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OperacionEvent>
    kafkaListenerContainerFactoryOperacionEvents() {

        ConcurrentKafkaListenerContainerFactory<String, OperacionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactoryOperacionEvents());
        return factory;
    }
}