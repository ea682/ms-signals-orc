package com.apunto.engine.outbox.service.impl;

import com.apunto.engine.entity.OperationMovementEventEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricMovementOutboxPartitionKeyTest {

    @Test
    void kafkaKeyKeepsAllSidesOfWalletAndSymbolInOnePartition() throws Exception {
        CapturingJdbcTemplate jdbc = new CapturingJdbcTemplate();
        MetricMovementOutboxServiceImpl service = new MetricMovementOutboxServiceImpl(
                jdbc,
                new ObjectMapper().findAndRegisterModules()
        );
        Field enabled = MetricMovementOutboxServiceImpl.class.getDeclaredField("enabled");
        enabled.setAccessible(true);
        enabled.setBoolean(service, true);

        service.enqueue(OperationMovementEventEntity.builder()
                .movementKey("movement|sha256:abc")
                .idWalletOrigin("0xABC")
                .parsymbol("btcusdt")
                .dateCreation(OffsetDateTime.parse("2026-07-12T00:00:00Z"))
                .build());

        assertEquals("0xabc|BTCUSDT", jdbc.arguments[2]);
    }

    private static final class CapturingJdbcTemplate extends JdbcTemplate {
        private Object[] arguments;

        @Override
        public int update(String sql, Object... args) {
            this.arguments = args;
            return 1;
        }
    }
}
