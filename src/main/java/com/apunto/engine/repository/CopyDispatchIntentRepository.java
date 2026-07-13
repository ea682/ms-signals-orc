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

    @Query(value = """
        WITH allocation_ids AS (
            SELECT a.id
            FROM futuros_operaciones.user_copy_allocation a
            WHERE a.id IN (:allocationIds)
        ), intent_stats AS (
            SELECT i.user_copy_allocation_id AS allocation_id,
                   count(*) FILTER (WHERE i.sent_at IS NOT NULL) AS submitted_orders,
                   count(*) FILTER (WHERE i.acknowledged_at IS NOT NULL OR i.binance_order_id IS NOT NULL) AS acknowledged_orders,
                   count(*) FILTER (WHERE i.filled_at IS NOT NULL OR coalesce(i.executed_qty, 0) > 0) AS filled_orders,
                   count(*) FILTER (WHERE i.status IN ('REJECTED', 'FAILED_FINAL', 'CANCELLED', 'MANUAL_REVIEW')) AS dispatch_errors,
                   count(*) FILTER (WHERE i.status IN ('RECONCILING', 'PERSISTENCE_PENDING', 'NEW', 'PARTIALLY_FILLED')) AS reconciliation_pending,
                   count(*) - count(DISTINCT i.idempotency_key) AS duplicate_count,
                   count(*) FILTER (
                       WHERE i.status IN ('RECONCILING', 'MANUAL_REVIEW')
                          AND upper(coalesce(i.last_error_code, '')) IN (
                              'EXECUTION_TIMEOUT_RECONCILING',
                              'EXECUTION_AMBIGUOUS_RECONCILING',
                              'BINANCE_OUTCOME_AMBIGUOUS',
                              'BINANCE_RESPONSE_AMBIGUOUS',
                             'NEW_ORDER_RECONCILIATION_EXHAUSTED',
                             'ORDER_NOT_FOUND_REQUIRES_MANUAL_REVIEW'
                         )
                   ) AS unresolved_ambiguous_timeouts,
                   count(*) FILTER (WHERE coalesce(i.reference_price, 0) > 0 AND coalesce(i.average_price, 0) > 0) AS slippage_samples,
                   cast(percentile_cont(0.95) WITHIN GROUP (
                       ORDER BY greatest(
                           CASE WHEN upper(coalesce(i.side, 'BUY')) = 'BUY'
                                THEN (i.average_price - i.reference_price) / i.reference_price * 10000
                                ELSE (i.reference_price - i.average_price) / i.reference_price * 10000
                           END,
                           0
                       )
                   ) FILTER (WHERE coalesce(i.reference_price, 0) > 0 AND coalesce(i.average_price, 0) > 0) AS numeric) AS adverse_slippage_p95_bps,
                   min(i.sent_at) AS first_submitted_at
            FROM futuros_operaciones.copy_dispatch_intent i
            WHERE i.user_copy_allocation_id IN (:allocationIds)
              AND i.execution_mode = 'MICRO_LIVE'
            GROUP BY i.user_copy_allocation_id
        ), event_running AS (
            SELECT e.user_copy_allocation_id AS allocation_id,
                   coalesce(e.event_time, e.date_creation) AS event_order_at,
                   e.id_event,
                   upper(coalesce(e.event_type, '')) AS event_type,
                   coalesce(e.realized_pnl_usd, 0) AS realized_pnl_usd,
                   sum(coalesce(e.realized_pnl_usd, 0)) OVER (
                       PARTITION BY e.user_copy_allocation_id
                       ORDER BY coalesce(e.event_time, e.date_creation), e.id_event
                   ) AS cumulative_pnl
            FROM futuros_operaciones.copy_operation_event e
            WHERE e.user_copy_allocation_id IN (:allocationIds)
              AND coalesce(e.execution_mode, 'LIVE') = 'MICRO_LIVE'
              AND coalesce(e.is_shadow, false) = false
        ), event_peaks AS (
            SELECT allocation_id,
                   event_order_at,
                   id_event,
                   event_type,
                   realized_pnl_usd,
                   cumulative_pnl,
                   greatest(max(cumulative_pnl) OVER (
                       PARTITION BY allocation_id
                       ORDER BY event_order_at, id_event ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ), 0) AS peak_pnl
            FROM event_running
        ), event_stats AS (
            SELECT allocation_id,
                   count(*) FILTER (WHERE event_type = 'CLOSE') AS closed_operations,
                   coalesce(sum(realized_pnl_usd), 0) AS realized_pnl_usd,
                   coalesce(max(greatest(peak_pnl - cumulative_pnl, 0)), 0) AS max_drawdown_usd
            FROM event_peaks
            GROUP BY allocation_id
        )
        SELECT ai.id AS "allocationId",
               coalesce(i.submitted_orders, 0) AS "submittedOrders",
               coalesce(i.acknowledged_orders, 0) AS "acknowledgedOrders",
               coalesce(i.filled_orders, 0) AS "filledOrders",
               coalesce(e.closed_operations, 0) AS "closedOperations",
               coalesce(i.dispatch_errors, 0) AS "dispatchErrors",
               coalesce(i.reconciliation_pending, 0) AS "reconciliationPending",
               coalesce(i.duplicate_count, 0) AS "duplicateCount",
               coalesce(i.unresolved_ambiguous_timeouts, 0) AS "unresolvedAmbiguousTimeouts",
               coalesce(i.slippage_samples, 0) AS "slippageSamples",
               coalesce(e.realized_pnl_usd, 0) AS "realizedPnlUsd",
               coalesce(e.max_drawdown_usd, 0) AS "maxDrawdownUsd",
               coalesce(i.adverse_slippage_p95_bps, 0) AS "adverseSlippageP95Bps",
               i.first_submitted_at AS "firstSubmittedAt"
        FROM allocation_ids ai
        LEFT JOIN intent_stats i ON i.allocation_id = ai.id
        LEFT JOIN event_stats e ON e.allocation_id = ai.id
        ORDER BY ai.id
        """, nativeQuery = true)
    List<MicroLiveExecutionEvidenceProjection> findMicroLiveExecutionEvidence(
            @Param("allocationIds") List<Long> allocationIds);

    @Modifying(flushAutomatically = true)
    @Query(value = """
        INSERT INTO futuros_operaciones.copy_dispatch_intent (
            id, idempotency_key, id_user, user_copy_allocation_id, execution_mode,
            wallet_id, strategy_code, scope_type, scope_value, source_event_id,
            id_order_origin, source_event_type, copy_intent, symbol, side, position_side,
            reduce_only, requested_qty, requested_margin_usd, requested_notional_usd,
            reference_price, requested_leverage, user_max_concurrent_positions,
            reserved_position_count, reservation_status,
            client_order_id, average_price_status, status, request_hash, attempts,
            reconciliation_attempts, created_at, updated_at
        ) VALUES (
            :id, :key, :userId, :allocationId, :mode, :walletId, :strategy,
            :scopeType, :scopeValue, :sourceEventId, :originId, :sourceEventType,
            :copyIntent, :symbol, :side, :positionSide, :reduceOnly, :qty, :margin,
            :notional, :referencePrice, :leverage, :userMaxConcurrentPositions,
            :reservedPositions, 'UNRESERVED',
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
             @Param("userMaxConcurrentPositions") Integer userMaxConcurrentPositions,
             @Param("reservedPositions") int reservedPositions,
            @Param("clientOrderId") String clientOrderId,
            @Param("requestHash") String requestHash,
            @Param("now") OffsetDateTime now);

    @Query(value = """
        WITH active AS (
            SELECT COALESCE(SUM(co.size_usd / NULLIF(co.leverage, 0)), 0) AS used_margin,
                   COUNT(*) AS open_positions
            FROM futuros_operaciones.copy_operation co
            WHERE co.id_user = :userId
              AND lower(co.id_wallet_origin) = lower(:walletId)
              AND co.execution_mode = :mode
              AND co.is_active = true
              AND COALESCE(co.is_shadow, false) = false
        ), pending AS (
            SELECT COALESCE(SUM(cdi.requested_margin_usd), 0) AS reserved_margin,
                   COALESCE(SUM(cdi.reserved_position_count), 0) AS reserved_positions
            FROM futuros_operaciones.copy_dispatch_intent cdi
            WHERE cdi.id_user = :userId
              AND lower(cdi.wallet_id) = lower(:walletId)
              AND cdi.execution_mode = :mode
              AND cdi.reservation_status = 'PENDING'
        )
        SELECT active.used_margin AS "usedMarginUsd",
               pending.reserved_margin AS "reservedPendingMarginUsd",
               active.open_positions AS "openPositions",
               pending.reserved_positions AS "reservedPositions"
        FROM active CROSS JOIN pending
        """, nativeQuery = true)
    CopyBudgetSnapshotProjection loadBudgetSnapshot(@Param("userId") String userId,
                                                    @Param("walletId") String walletId,
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
