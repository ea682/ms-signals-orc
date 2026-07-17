package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationEventRecordCommand;

import java.util.UUID;

public interface CopyOperationEventService {
    void record(CopyOperationEventRecordCommand command);

    UUID recordRequired(CopyOperationEventRecordCommand command);

    UUID recordReconciliationRequired(CopyOperationEventRecordCommand command);

    String findEventType(UUID eventId);
}
