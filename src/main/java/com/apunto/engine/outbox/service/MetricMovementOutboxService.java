package com.apunto.engine.outbox.service;

import com.apunto.engine.entity.OperationMovementEventEntity;

public interface MetricMovementOutboxService {
    void enqueue(OperationMovementEventEntity entity);
}
