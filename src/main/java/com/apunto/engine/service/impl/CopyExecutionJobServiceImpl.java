package com.apunto.engine.service.impl;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.entity.CopyExecutionJobEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.jobs.model.CopyJobErrorCategory;
import com.apunto.engine.jobs.model.CopyJobStatus;
import com.apunto.engine.repository.CopyExecutionJobRepository;
import com.apunto.engine.service.CopyExecutionJobService;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class CopyExecutionJobServiceImpl implements CopyExecutionJobService {

    private static final String CLAIM_SQL = """
            UPDATE copy_execution_job j
            SET status = 'PROCESSING',
                locked_at = :now,
                locked_by = :workerId,
                updated_at = :now
            WHERE j.id IN (
                SELECT id
                FROM copy_execution_job
                WHERE status = 'PENDING'
                  AND next_run_at <= :now
                ORDER BY next_run_at ASC, id ASC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """;

    private final CopyExecutionJobRepository repository;
    private final ObjectMapper objectMapper;

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    @Transactional
    public int enqueueForUsers(OperacionEvent event, List<UserDetailDto> users, CopyJobAction action) {
        final String originId = String.valueOf(event.getOperacion().getIdOperacion());
        final String payload = serializeEvent(event);

        final OffsetDateTime now = OffsetDateTime.now();
        int enqueued = 0;

        for (UserDetailDto u : users) {
            final String userId = u.getUser().getId().toString();

            // UPSERT idempotente: refresca el payload incluso cuando un job anterior quedó DONE/DEAD
            // o cuando llega un payload nuevo mientras el job actual está PROCESSING.
            int inserted = repository.upsertPending(
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

        final OffsetDateTime now = OffsetDateTime.now();
        final String safeWorkerId = workerId == null || workerId.isBlank() ? "unknown-worker" : workerId;

        @SuppressWarnings("unchecked")
        final List<CopyExecutionJobEntity> claimed = entityManager
                .createNativeQuery(CLAIM_SQL, CopyExecutionJobEntity.class)
                .setParameter("now", now)
                .setParameter("workerId", safeWorkerId)
                .setParameter("limit", limit)
                .getResultList();

        if (claimed == null || claimed.isEmpty()) {
            return List.of();
        }

        log.debug("event=copy.job.claimed_atomic workerId={} count={} limit={}", safeWorkerId, claimed.size(), limit);
        return claimed;
    }

    @Override
    @Transactional
    public void markDone(CopyExecutionJobEntity job) {
        final OffsetDateTime now = OffsetDateTime.now();
        final String none = CopyJobErrorCategory.NONE.name();

        final int markedDone = repository.markDoneIfPayloadUnchanged(
                job.getId(),
                job.getPayload(),
                job.getAttempt(),
                none,
                now
        );

        if (markedDone == 1) {
            return;
        }

        final int requeued = repository.requeueDoneWhenPayloadChanged(
                job.getId(),
                job.getPayload(),
                none,
                now
        );

        if (requeued == 1) {
            log.info("event=copy.job.requeued_payload_changed id={} originId={} userId={} action={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction());
            return;
        }

        log.warn("event=copy.job.mark_done_noop id={} originId={} userId={} action={} status={} note=row_not_found_or_already_changed",
                job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), job.getStatus());
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
            throw new EngineException(ErrorCode.INTERNAL_ERROR, "No se pudo serializar OperacionEvent a JSON", e);
        }
    }

    private String safeCategory(String category) {
        if (category == null || category.isBlank()) return CopyJobErrorCategory.UNKNOWN.name();
        return category;
    }
}
