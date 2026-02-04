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
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class CopyExecutionJobServiceImpl implements CopyExecutionJobService {

    private final CopyExecutionJobRepository repository;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional
    public int enqueueForUsers(OperacionEvent event, List<UserDetailDto> users, CopyJobAction action) {
        final String originId = String.valueOf(event.getOperacion().getIdOperacion());
        final String payload = serializeEvent(event);

        final OffsetDateTime now = OffsetDateTime.now();
        int enqueued = 0;

        for (UserDetailDto u : users) {
            final String userId = u.getUser().getId().toString();

            // INSERT idempotente: si ya existe (origin_id, user_id, action), NO lanza error.
            int inserted = repository.insertIgnore(
                    UUID.randomUUID(),
                    originId,
                    userId,
                    action.name(),
                    CopyJobStatus.PENDING.name(),
                    0,
                    now,
                    payload,
                    CopyJobErrorCategory.NONE.name(),
                    now,
                    now
            );

            if (inserted == 1) {
                enqueued++;
            }
        }

        return enqueued;
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
    public int requeueStaleProcessing(OffsetDateTime threshold) {
        return repository.requeueStaleProcessing(threshold, OffsetDateTime.now());
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
