package com.apunto.engine.service;

import com.apunto.engine.dto.CopyOperationEventRecordCommand;

public interface CopyOperationEventService {
    void record(CopyOperationEventRecordCommand command);
}
