package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperationMovementEventRecordCommand;
import com.apunto.engine.entity.OperationMovementEventEntity;
import com.apunto.engine.outbox.service.MetricMovementOutboxService;
import com.apunto.engine.repository.OperationMovementEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OperationMovementEconomicNormalizationTest {

    @Test
    void sourceNotClosingCannotBeUpgradedToRecoveredClosingEconomics() throws Exception {
        OperationMovementEventServiceImpl service = service();
        OperationMovementEventRecordCommand command = OperationMovementEventRecordCommand.builder()
                .typeOperation("LONG")
                .eventType("UNKNOWN")
                .deltaType("RESIZE")
                .sizeQty(new BigDecimal("8"))
                .entryPrice(new BigDecimal("100"))
                .markPrice(new BigDecimal("110"))
                .normalizationStatus("NOT_CLOSING")
                .normalizationReason("no_close_or_reduce_quantity")
                .build();
        OperationMovementEventEntity previous = OperationMovementEventEntity.builder()
                .resultingSizeQty(new BigDecimal("10"))
                .entryPrice(new BigDecimal("100"))
                .typeOperation("LONG")
                .build();

        Method normalize = OperationMovementEventServiceImpl.class.getDeclaredMethod(
                "normalizeMovementValues",
                OperationMovementEventRecordCommand.class,
                OperationMovementEventEntity.class,
                String.class,
                BigDecimal.class,
                BigDecimal.class
        );
        normalize.setAccessible(true);
        Object result = normalize.invoke(
                service,
                command,
                previous,
                "REDUCE",
                new BigDecimal("-2"),
                new BigDecimal("20")
        );

        assertEquals("SEMANTIC_CONFLICT", accessor(result, "normalizationStatus"));
        assertEquals("source_not_closing_but_ledger_classified_reduce", accessor(result, "normalizationReason"));
        assertNull(accessor(result, "effectiveCloseQty"));
        assertNull(accessor(result, "effectiveRealizedPnlUsd"));
    }

    private Object accessor(Object target, String methodName) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }

    @SuppressWarnings("unchecked")
    private OperationMovementEventServiceImpl service() {
        OperationMovementEventRepository repository = (OperationMovementEventRepository) Proxy.newProxyInstance(
                OperationMovementEventRepository.class.getClassLoader(),
                new Class<?>[]{OperationMovementEventRepository.class},
                (proxy, method, args) -> defaultValue(method.getReturnType())
        );
        MetricMovementOutboxService outbox = ignored -> { };
        return new OperationMovementEventServiceImpl(
                repository,
                new ObjectMapper(),
                outbox,
                new NoopTransactionManager(),
                new SimpleMeterRegistry(),
                false,
                1,
                1
        );
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class || type == short.class || type == byte.class || type == long.class) {
            return 0;
        }
        if (type == float.class || type == double.class) {
            return 0.0;
        }
        return null;
    }

    private static final class NoopTransactionManager implements PlatformTransactionManager {
        @Override
        public TransactionStatus getTransaction(TransactionDefinition definition) {
            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
        }

        @Override
        public void rollback(TransactionStatus status) {
        }
    }
}
