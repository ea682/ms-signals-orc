package com.apunto.engine.outbox.service;

import com.apunto.engine.entity.CopyOperationEventEntity;

public interface MetricCopyOperationOutboxService {
    void enqueue(CopyOperationEventEntity entity);
}
