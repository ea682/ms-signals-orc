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
}
