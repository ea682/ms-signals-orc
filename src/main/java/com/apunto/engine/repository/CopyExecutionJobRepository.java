package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyExecutionJobEntity;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.jobs.model.CopyJobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface CopyExecutionJobRepository extends JpaRepository<CopyExecutionJobEntity, UUID> {

    @Query(value = """
            SELECT id
            FROM copy_execution_job
            WHERE status = 'PENDING'
              AND next_run_at <= :now
            ORDER BY next_run_at ASC, id ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<UUID> findClaimableIdsForUpdateSkipLocked(
            @Param("now") OffsetDateTime now,
            @Param("limit") int limit
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE copy_execution_job
            SET status = 'PROCESSING',
                locked_at = :now,
                locked_by = :workerId,
                updated_at = :now
            WHERE id IN (:ids)
            """, nativeQuery = true)
    int markProcessing(
            @Param("ids") List<UUID> ids,
            @Param("workerId") String workerId,
            @Param("now") OffsetDateTime now
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE copy_execution_job
            SET status = :status,
                attempt = :attempt,
                next_run_at = :nextRunAt,
                locked_at = NULL,
                locked_by = NULL,
                last_error_category = :errorCategory,
                last_error_message = :errorMessage,
                last_error_at = :errorAt,
                updated_at = :now
            WHERE id = :id
            """, nativeQuery = true)
    int finish(
            @Param("id") UUID id,
            @Param("status") String status,
            @Param("attempt") int attempt,
            @Param("nextRunAt") OffsetDateTime nextRunAt,
            @Param("errorCategory") String errorCategory,
            @Param("errorMessage") String errorMessage,
            @Param("errorAt") OffsetDateTime errorAt,
            @Param("now") OffsetDateTime now
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE copy_execution_job
            SET status = 'PENDING',
                locked_at = NULL,
                locked_by = NULL,
                updated_at = :now
            WHERE status = 'PROCESSING'
              AND locked_at IS NOT NULL
              AND locked_at < :threshold
            """, nativeQuery = true)
    int requeueStaleProcessing(
            @Param("threshold") OffsetDateTime threshold,
            @Param("now") OffsetDateTime now
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
    INSERT INTO copy_execution_job (
        id, origin_id, user_id, action, status, attempt, next_run_at,
        payload, last_error_category, created_at, updated_at
    ) VALUES (
        :id, :originId, :userId, :action, :status, :attempt, :nextRunAt,
        :payload, :lastErrorCategory, :createdAt, :updatedAt
    )
    ON CONFLICT (origin_id, user_id, action) DO UPDATE
       SET status = CASE
               WHEN copy_execution_job.status IN ('PENDING', 'DONE', 'DEAD') THEN 'PENDING'
               ELSE copy_execution_job.status
           END,
           attempt = CASE
               WHEN copy_execution_job.status IN ('DONE', 'DEAD') THEN 0
               ELSE copy_execution_job.attempt
           END,
           next_run_at = CASE
               WHEN copy_execution_job.status IN ('PENDING', 'DONE', 'DEAD') THEN EXCLUDED.next_run_at
               ELSE copy_execution_job.next_run_at
           END,
           payload = EXCLUDED.payload,
           last_error_category = EXCLUDED.last_error_category,
           last_error_message = CASE
               WHEN copy_execution_job.status IN ('PENDING', 'DONE', 'DEAD') THEN NULL
               ELSE copy_execution_job.last_error_message
           END,
           last_error_at = CASE
               WHEN copy_execution_job.status IN ('PENDING', 'DONE', 'DEAD') THEN NULL
               ELSE copy_execution_job.last_error_at
           END,
           locked_at = CASE
               WHEN copy_execution_job.status IN ('DONE', 'DEAD') THEN NULL
               ELSE copy_execution_job.locked_at
           END,
           locked_by = CASE
               WHEN copy_execution_job.status IN ('DONE', 'DEAD') THEN NULL
               ELSE copy_execution_job.locked_by
           END,
           updated_at = EXCLUDED.updated_at
    """, nativeQuery = true)
    int upsertPending(
            @Param("id") UUID id,
            @Param("originId") String originId,
            @Param("userId") String userId,
            @Param("action") String action,
            @Param("status") String status,
            @Param("attempt") int attempt,
            @Param("nextRunAt") OffsetDateTime nextRunAt,
            @Param("payload") String payload,
            @Param("lastErrorCategory") String lastErrorCategory,
            @Param("createdAt") OffsetDateTime createdAt,
            @Param("updatedAt") OffsetDateTime updatedAt
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE copy_execution_job
            SET status = 'DONE',
                attempt = :attempt,
                next_run_at = :now,
                locked_at = NULL,
                locked_by = NULL,
                last_error_category = :errorCategory,
                last_error_message = NULL,
                last_error_at = NULL,
                updated_at = :now
            WHERE id = :id
              AND status = 'PROCESSING'
              AND payload = :payload
            """, nativeQuery = true)
    int markDoneIfPayloadUnchanged(
            @Param("id") UUID id,
            @Param("payload") String payload,
            @Param("attempt") int attempt,
            @Param("errorCategory") String errorCategory,
            @Param("now") OffsetDateTime now
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE copy_execution_job
            SET status = 'PENDING',
                attempt = 0,
                next_run_at = :now,
                locked_at = NULL,
                locked_by = NULL,
                last_error_category = :errorCategory,
                last_error_message = NULL,
                last_error_at = NULL,
                updated_at = :now
            WHERE id = :id
              AND status = 'PROCESSING'
              AND payload IS DISTINCT FROM :payload
            """, nativeQuery = true)
    int requeueDoneWhenPayloadChanged(
            @Param("id") UUID id,
            @Param("payload") String payload,
            @Param("errorCategory") String errorCategory,
            @Param("now") OffsetDateTime now
    );

    long countByStatus(CopyJobStatus status);

    boolean existsByOriginIdAndUserIdAndAction(String originId, String userId, CopyJobAction action);


}
