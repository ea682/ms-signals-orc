package com.apunto.engine.service;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.entity.CopyExecutionJobEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.jobs.model.CopyJobAction;

import java.time.OffsetDateTime;
import java.util.List;

public interface CopyExecutionJobService {

    int enqueueForUsers(OperacionEvent event, List<UserDetailDto> users, CopyJobAction action);

    List<CopyExecutionJobEntity> claimBatch(String workerId, int limit);

    void markDone(CopyExecutionJobEntity job);

    void markDead(CopyExecutionJobEntity job, String category, String message);

    void reschedule(CopyExecutionJobEntity job, OffsetDateTime nextRunAt, String category, String message);

    void requeueStaleProcessing(OffsetDateTime threshold);
}
