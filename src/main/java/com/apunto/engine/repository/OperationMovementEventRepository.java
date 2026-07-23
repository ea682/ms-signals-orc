package com.apunto.engine.repository;

import com.apunto.engine.entity.OperationMovementEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface OperationMovementEventRepository extends JpaRepository<OperationMovementEventEntity, UUID> {
    Optional<OperationMovementEventEntity> findByMovementKey(String movementKey);

    @Query(value = """
            SELECT EXISTS (
                SELECT 1
                FROM futuros_operaciones.operation_movement_event_dedupe d
                WHERE d.movement_key = :movementKey
            )
            """, nativeQuery = true)
    boolean existsByMovementKeyInGuard(@Param("movementKey") String movementKey);

    Optional<OperationMovementEventEntity> findTopByPositionKeyOrderByEventTimeDescDateCreationDesc(String positionKey);

    Optional<OperationMovementEventEntity> findTopByPositionKeyAndEventTimeLessThanEqualOrderByEventTimeDescDateCreationDesc(String positionKey, OffsetDateTime eventTime);

    @Query(value = """
            SELECT e.*
            FROM futuros_operaciones.operation_movement_event e
            WHERE e.position_key = :positionKey
              AND (
                e.event_time < :eventTime
                OR (
                  e.event_time = :eventTime
                  AND (
                    COALESCE(e.source_sequence, (-9223372036854775807 - 1))
                      < COALESCE(:sourceSequence, (-9223372036854775807 - 1))
                    OR (
                      COALESCE(e.source_sequence, (-9223372036854775807 - 1))
                        = COALESCE(:sourceSequence, (-9223372036854775807 - 1))
                      AND e.movement_key < :movementKey
                    )
                  )
                )
              )
            ORDER BY e.event_time DESC,
                     COALESCE(e.source_sequence, (-9223372036854775807 - 1)) DESC,
                     e.movement_key DESC
            LIMIT 1
            """, nativeQuery = true)
    Optional<OperationMovementEventEntity> findPreviousByEconomicOrder(
            @Param("positionKey") String positionKey,
            @Param("eventTime") OffsetDateTime eventTime,
            @Param("sourceSequence") Long sourceSequence,
            @Param("movementKey") String movementKey
    );

    @Query(value = """
            SELECT pg_advisory_xact_lock(
              hashtextextended(:positionKey, 0)
            )
            """, nativeQuery = true)
    Object lockEconomicPosition(@Param("positionKey") String positionKey);
}
