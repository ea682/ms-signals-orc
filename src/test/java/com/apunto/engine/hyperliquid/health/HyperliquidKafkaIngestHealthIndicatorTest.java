package com.apunto.engine.hyperliquid.health;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.lang.reflect.Proxy;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HyperliquidKafkaIngestHealthIndicatorTest {

    @Test
    void listenerIsReadyOnlyAfterKafkaPartitionAssignment() {
        HyperliquidKafkaIngestHealthIndicator indicator =
                new HyperliquidKafkaIngestHealthIndicator(
                        registry(container(true, true, 1)),
                        "hyperliquid-position-deltas",
                        "ms-signals-orc-hyperliquid-position-deltas-v1");

        var health = indicator.health();

        assertEquals("UP", health.getStatus().getCode());
        assertEquals("READY", health.getDetails().get("state"));
        assertEquals(1, health.getDetails().get("assignedPartitions"));
    }

    @Test
    void runningContainerWithoutAssignmentIsDegradedNotReady() {
        HyperliquidKafkaIngestHealthIndicator indicator =
                new HyperliquidKafkaIngestHealthIndicator(
                        registry(container(true, true, 0)),
                        "hyperliquid-position-deltas",
                        "ms-signals-orc-hyperliquid-position-deltas-v1");

        var health = indicator.health();

        assertEquals("DEGRADED", health.getStatus().getCode());
        assertEquals("DOWN", health.getDetails().get("state"));
        assertEquals(0, health.getDetails().get("assignedPartitions"));
    }

    private KafkaListenerEndpointRegistry registry(MessageListenerContainer container) {
        return new KafkaListenerEndpointRegistry() {
            @Override
            public MessageListenerContainer getListenerContainer(String id) {
                return container;
            }
        };
    }

    private MessageListenerContainer container(boolean running, boolean expected, int partitions) {
        return (MessageListenerContainer) Proxy.newProxyInstance(
                MessageListenerContainer.class.getClassLoader(),
                new Class<?>[]{MessageListenerContainer.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isRunning" -> running;
                    case "isInExpectedState" -> expected;
                    case "getAssignedPartitions" -> partitions == 0
                            ? List.of()
                            : List.of(new TopicPartition("hyperliquid-position-deltas", 0));
                    case "getPhase" -> 0;
                    case "isAutoStartup" -> true;
                    default -> null;
                }
        );
    }
}
