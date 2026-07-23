package com.apunto.engine.repository;

import com.apunto.engine.entity.MicroLiveRecertificationRequestEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

public interface MicroLiveRecertificationRequestRepository
        extends JpaRepository<MicroLiveRecertificationRequestEntity, UUID> {

    Optional<MicroLiveRecertificationRequestEntity> findByIdempotencyKey(String idempotencyKey);

    @Modifying(flushAutomatically = true)
    @Query(value = """
            insert into futuros_operaciones.micro_live_recertification_request (
                id, certification_id, wallet_id, strategy_code, strategy_version,
                user_id, execution_account_id, requested_at, priority, status,
                attempts, reason_code, idempotency_key, next_attempt_at, updated_at
            ) values (
                :id, :certificationId, :walletId, :strategyCode, :strategyVersion,
                :userId, :executionAccountId, :now, :priority, 'PENDING_CAPACITY',
                0, :reasonCode, :idempotencyKey, :now, :now
            )
            on conflict (idempotency_key) do nothing
            """, nativeQuery = true)
    int insertIfAbsent(@Param("id") UUID id,
                       @Param("certificationId") UUID certificationId,
                       @Param("walletId") String walletId,
                       @Param("strategyCode") String strategyCode,
                       @Param("strategyVersion") String strategyVersion,
                       @Param("userId") UUID userId,
                       @Param("executionAccountId") UUID executionAccountId,
                       @Param("priority") int priority,
                       @Param("reasonCode") String reasonCode,
                       @Param("idempotencyKey") String idempotencyKey,
                       @Param("now") OffsetDateTime now);

    @Query(value = """
            select *
            from futuros_operaciones.micro_live_recertification_request
            where status = 'PENDING_CAPACITY'
              and next_attempt_at <= now()
            order by priority desc, requested_at, id
            limit 1
            for update skip locked
            """, nativeQuery = true)
    Optional<MicroLiveRecertificationRequestEntity> findNextPendingForUpdate();
}
