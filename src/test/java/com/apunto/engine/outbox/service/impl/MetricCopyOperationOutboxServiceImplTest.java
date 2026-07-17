package com.apunto.engine.outbox.service.impl;

import com.apunto.engine.entity.CopyOperationEventEntity;
import com.apunto.engine.outbox.dto.MetricCopyOperationPersistedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricCopyOperationOutboxServiceImplTest {

    @Test
    void canonicalRuntimeShadowIsPublishedAsExecutableShadow() throws Exception {
        MetricCopyOperationOutboxServiceImpl service = new MetricCopyOperationOutboxServiceImpl(
                null, new ObjectMapper());
        CopyOperationEventEntity entity = CopyOperationEventEntity.builder()
                .idEvent(UUID.randomUUID())
                .executionMode("SHADOW")
                .shadow(true)
                .build();

        MetricCopyOperationPersistedEvent event = toEvent(service, entity);

        assertEquals("EXECUTABLE_SHADOW", event.executionMode());
    }

    @Test
    void realModesKeepTheirExecutionIdentity() throws Exception {
        MetricCopyOperationOutboxServiceImpl service = new MetricCopyOperationOutboxServiceImpl(
                null, new ObjectMapper());
        CopyOperationEventEntity entity = CopyOperationEventEntity.builder()
                .idEvent(UUID.randomUUID())
                .executionMode("MICRO_LIVE")
                .shadow(false)
                .build();

        MetricCopyOperationPersistedEvent event = toEvent(service, entity);

        assertEquals("MICRO_LIVE", event.executionMode());
    }

    private MetricCopyOperationPersistedEvent toEvent(MetricCopyOperationOutboxServiceImpl service,
                                                       CopyOperationEventEntity entity) throws Exception {
        Method method = MetricCopyOperationOutboxServiceImpl.class
                .getDeclaredMethod("toEvent", CopyOperationEventEntity.class);
        method.setAccessible(true);
        return (MetricCopyOperationPersistedEvent) method.invoke(service, entity);
    }
}
