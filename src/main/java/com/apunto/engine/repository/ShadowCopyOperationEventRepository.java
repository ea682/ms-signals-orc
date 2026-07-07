package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowCopyOperationEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public interface ShadowCopyOperationEventRepository extends JpaRepository<ShadowCopyOperationEventEntity, UUID> {

    @Query(value = "select 1 from (select pg_advisory_xact_lock(hashtext(:lockKey))) locked", nativeQuery = true)
    Integer lockShadowEventIdempotency(@Param("lockKey") String lockKey);

    boolean existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
            Long shadowAllocationId,
            String idOrderOrigin,
            String eventType,
            String positionSide,
            OffsetDateTime eventTime
    );

    boolean existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
            Long walletProfileId,
            String idOrderOrigin,
            String eventType,
            String positionSide,
            OffsetDateTime eventTime
    );

    long countByWalletProfileIdAndDecision(Long walletProfileId, String decision);

    long countByShadowAllocationId(Long shadowAllocationId);
}
