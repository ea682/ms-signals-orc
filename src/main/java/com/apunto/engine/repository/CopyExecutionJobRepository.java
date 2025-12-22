package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyExecutionJobEntity;
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

    long countByStatus(CopyJobStatus status);
}
