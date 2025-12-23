package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.entity.CopyExecutionJobEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.jobs.model.CopyJobErrorCategory;
import com.apunto.engine.jobs.model.CopyJobStatus;
import com.apunto.engine.repository.CopyExecutionJobRepository;
import com.apunto.engine.service.CopyExecutionJobService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.swing.text.DefaultEditorKit;
import java.time.OffsetDateTime;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class CopyExecutionJobServiceImpl implements CopyExecutionJobService {

    private final CopyExecutionJobRepository repository;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional
    public int enqueueForUsers(OperacionEvent event, List<UserDetailDto> users, CopyJobAction action) {
        if (event == null || users == null || users.isEmpty()) {
            return 0;
        }

        final String originId = event.getOperacion().getIdOperacion().toString();
        final String payload = serializeEvent(event);

        int created = 0;
        for (UserDetailDto u : users) {
            if (u == null || u.getUser() == null || u.getUser().getId() == null) continue;

            CopyExecutionJobEntity job = new CopyExecutionJobEntity();

            job.setId(null);

            job.setOriginId(originId);
            job.setUserId(u.getUser().getId().toString());
            job.setAction(action);
            job.setStatus(CopyJobStatus.PENDING);
            job.setAttempt(0);
            job.setNextRunAt(OffsetDateTime.now());
            job.setPayload(payload);
            job.setLastErrorMessage("");
            job.setLastErrorCategory(CopyJobErrorCategory.NONE);

            try {
                repository.saveAndFlush(job);
                created++;
            } catch (DataIntegrityViolationException dup) {
                // duplicado por uq_copy_execution_job_origin_user_action -> lo ignoramos
            }
        }

        return created;
    }



    @Override
    @Transactional
    public List<CopyExecutionJobEntity> claimBatch(String workerId, int limit) {
        if (limit <= 0) return List.of();

        OffsetDateTime now = OffsetDateTime.now();
        List<UUID> ids = repository.findClaimableIdsForUpdateSkipLocked(now, limit);
        if (ids == null || ids.isEmpty()) return List.of();

        repository.markProcessing(ids, workerId, now);

        List<CopyExecutionJobEntity> out = new ArrayList<>(ids.size());
        repository.findAllById(ids).forEach(out::add);
        return out;
    }



    @Override
    @Transactional
    public void markDone(CopyExecutionJobEntity job) {
        OffsetDateTime now = OffsetDateTime.now();
        repository.finish(job.getId(), CopyJobStatus.DONE.name(), job.getAttempt(), now,
                CopyJobErrorCategory.NONE.name(), null, null, now);
    }

    @Override
    @Transactional
    public void markDead(CopyExecutionJobEntity job, String category, String message) {
        OffsetDateTime now = OffsetDateTime.now();
        repository.finish(job.getId(), CopyJobStatus.DEAD.name(), job.getAttempt(), now,
                safeCategory(category), message, now, now);
    }

    @Override
    @Transactional
    public void reschedule(CopyExecutionJobEntity job, OffsetDateTime nextRunAt, String category, String message) {
        OffsetDateTime now = OffsetDateTime.now();
        repository.finish(job.getId(), CopyJobStatus.PENDING.name(), job.getAttempt(), nextRunAt,
                safeCategory(category), message, now, now);
    }

    @Override
    @Transactional
    public void requeueStaleProcessing(OffsetDateTime threshold) {
        repository.requeueStaleProcessing(threshold, OffsetDateTime.now());
    }

    private String serializeEvent(OperacionEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar OperacionEvent a JSON", e);
        }
    }

    private String safeCategory(String category) {
        if (category == null || category.isBlank()) return CopyJobErrorCategory.UNKNOWN.name();
        return category;
    }
}
