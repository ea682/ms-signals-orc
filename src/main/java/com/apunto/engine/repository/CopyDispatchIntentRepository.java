package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyDispatchIntentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CopyDispatchIntentRepository extends JpaRepository<CopyDispatchIntentEntity, UUID> {

    Optional<CopyDispatchIntentEntity> findByIdempotencyKey(String idempotencyKey);
    List<CopyDispatchIntentEntity> findAllByClientOrderId(String clientOrderId);

    @Modifying(flushAutomatically = true)
    @Query(value = """
        INSERT INTO futuros_operaciones.copy_dispatch_intent (
            id, idempotency_key, id_user, user_copy_allocation_id, execution_mode,
            wallet_id, strategy_code, scope_type, scope_value, source_event_id,
            id_order_origin, source_event_type, copy_intent, symbol, side, position_side,
            reduce_only, requested_qty, requested_margin_usd, requested_notional_usd,
            reference_price, requested_leverage, reserved_position_count, reservation_status,
            client_order_id, average_price_status, status, request_hash, attempts,
            reconciliation_attempts, created_at, updated_at
        ) VALUES (
            :id, :key, :userId, :allocationId, :mode, :walletId, :strategy,
            :scopeType, :scopeValue, :sourceEventId, :originId, :sourceEventType,
            :copyIntent, :symbol, :side, :positionSide, :reduceOnly, :qty, :margin,
            :notional, :referencePrice, :leverage, :reservedPositions, 'UNRESERVED',
            :clientOrderId, 'NOT_AVAILABLE', 'CREATED', :requestHash, 0, 0, :now, :now
        ) ON CONFLICT (idempotency_key) DO NOTHING
        """, nativeQuery = true)
    int insertIfAbsent(
            @Param("id") UUID id,
            @Param("key") String key,
            @Param("userId") String userId,
            @Param("allocationId") Long allocationId,
            @Param("mode") String mode,
            @Param("walletId") String walletId,
            @Param("strategy") String strategy,
            @Param("scopeType") String scopeType,
            @Param("scopeValue") String scopeValue,
            @Param("sourceEventId") String sourceEventId,
            @Param("originId") String originId,
            @Param("sourceEventType") String sourceEventType,
            @Param("copyIntent") String copyIntent,
            @Param("symbol") String symbol,
            @Param("side") String side,
            @Param("positionSide") String positionSide,
            @Param("reduceOnly") boolean reduceOnly,
            @Param("qty") BigDecimal qty,
            @Param("margin") BigDecimal margin,
            @Param("notional") BigDecimal notional,
            @Param("referencePrice") BigDecimal referencePrice,
            @Param("leverage") Integer leverage,
            @Param("reservedPositions") int reservedPositions,
            @Param("clientOrderId") String clientOrderId,
            @Param("requestHash") String requestHash,
            @Param("now") OffsetDateTime now);

    @Query(value = """
        WITH active AS (
            SELECT COALESCE(SUM(size_usd / NULLIF(leverage, 0)), 0) AS used_margin,
                   COUNT(*) AS open_positions
            FROM futuros_operaciones.copy_operation
            WHERE id_user = :userId
              AND user_copy_allocation_id = :allocationId
              AND execution_mode = :mode
              AND is_active = true
              AND COALESCE(is_shadow, false) = false
        ), pending AS (
            SELECT COALESCE(SUM(requested_margin_usd), 0) AS reserved_margin,
                   COALESCE(SUM(reserved_position_count), 0) AS reserved_positions
            FROM futuros_operaciones.copy_dispatch_intent
            WHERE id_user = :userId
              AND user_copy_allocation_id = :allocationId
              AND execution_mode = :mode
              AND reservation_status = 'PENDING'
        )
        SELECT active.used_margin AS "usedMarginUsd",
               pending.reserved_margin AS "reservedPendingMarginUsd",
               active.open_positions AS "openPositions",
               pending.reserved_positions AS "reservedPositions"
        FROM active CROSS JOIN pending
        """, nativeQuery = true)
    CopyBudgetSnapshotProjection loadBudgetSnapshot(@Param("userId") String userId,
                                                    @Param("allocationId") Long allocationId,
                                                    @Param("mode") String mode);

    @Query(value = """
        SELECT id
        FROM futuros_operaciones.copy_dispatch_intent
        WHERE (
            (status = 'DISPATCHING' AND updated_at < :staleBefore)
            OR (
                status IN ('RECONCILING', 'PERSISTENCE_PENDING', 'NEW', 'PARTIALLY_FILLED', 'FILLED', 'ACKNOWLEDGED', 'PERSISTED')
                AND (next_reconciliation_at IS NULL OR next_reconciliation_at <= :now)
                AND (status IN ('NEW', 'PARTIALLY_FILLED')
                     OR copy_operation_id IS NULL OR average_price_status = 'PENDING_RESOLUTION')
            )
        )
        ORDER BY COALESCE(next_reconciliation_at, updated_at), created_at, id
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<UUID> findReconciliationIdsForUpdateSkipLocked(
            @Param("now") OffsetDateTime now,
            @Param("staleBefore") OffsetDateTime staleBefore,
            @Param("limit") int limit);
}
