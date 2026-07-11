package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowCopyOperationEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface ShadowCopyOperationEventRepository extends JpaRepository<ShadowCopyOperationEventEntity, UUID> {

    @Query(value = "select 1 from (select pg_advisory_xact_lock(hashtext(:lockKey))) locked", nativeQuery = true)
    Integer lockShadowEventIdempotency(@Param("lockKey") String lockKey);

    @Query(value = "select 1 from (select pg_advisory_xact_lock(hashtextextended(cast(:lockKey as text), 0))) locked", nativeQuery = true)
    Integer lockShadowProfileMutation(@Param("lockKey") String lockKey);

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

    @Query(value = ShadowCoverageSql.ROLLING_BATCH_QUERY, nativeQuery = true)
    List<ShadowCoverageCountsProjection> findRollingCoverageBatch(
            @Param("allocationIds") List<Long> allocationIds,
            @Param("windowStart") OffsetDateTime windowStart,
            @Param("windowEnd") OffsetDateTime windowEnd,
            @Param("maxEvents") int maxEvents
    );
}
