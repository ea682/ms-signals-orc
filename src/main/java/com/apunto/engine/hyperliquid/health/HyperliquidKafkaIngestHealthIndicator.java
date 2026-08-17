package com.apunto.engine.hyperliquid.health;

import com.apunto.engine.hyperliquid.listener.HyperliquidPositionDeltaKafkaListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component("hyperliquidKafkaIngest")
@ConditionalOnProperty(
        name = "hyperliquid.kafka-ingest.enabled",
        havingValue = "true"
)
public class HyperliquidKafkaIngestHealthIndicator implements HealthIndicator {

    private final KafkaListenerEndpointRegistry registry;
    private final String topic;
    private final String groupId;

    public HyperliquidKafkaIngestHealthIndicator(
            KafkaListenerEndpointRegistry registry,
            @Value("${hyperliquid.kafka-ingest.topic}") String topic,
            @Value("${hyperliquid.kafka-ingest.group-id}") String groupId
    ) {
        this.registry = registry;
        this.topic = topic;
        this.groupId = groupId;
    }

    @Override
    public Health health() {
        MessageListenerContainer container =
                registry.getListenerContainer(HyperliquidPositionDeltaKafkaListener.LISTENER_ID);
        boolean running = container != null && container.isRunning();
        boolean expectedState = container != null && container.isInExpectedState();
        Collection<TopicPartition> assignments =
                container == null || container.getAssignedPartitions() == null
                        ? java.util.List.of()
                        : container.getAssignedPartitions();
        boolean ready = running && expectedState && !assignments.isEmpty();
        Health.Builder builder = ready ? Health.up() : Health.status("DEGRADED");
        return builder
                .withDetail("listener", HyperliquidPositionDeltaKafkaListener.LISTENER_ID)
                .withDetail("state", ready ? "READY" : "DOWN")
                .withDetail("topic", topic)
                .withDetail("groupId", groupId)
                .withDetail("running", running)
                .withDetail("expectedState", expectedState)
                .withDetail("assignedPartitions", assignments.size())
                .build();
    }
}
