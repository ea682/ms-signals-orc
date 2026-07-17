package com.apunto.engine.outbox.service;

import com.apunto.engine.entity.CopyOperationEventEntity;
import com.apunto.engine.outbox.dto.MetricCopyOperationPersistedEvent;

public interface MetricCopyOperationOutboxService {
    void enqueue(CopyOperationEventEntity entity);

    default void enqueue(MetricCopyOperationPersistedEvent event) {
        throw new UnsupportedOperationException("raw economic event enqueue is not implemented");
    }
}
