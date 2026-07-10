package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyOperationEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CopyOperationEventRepository extends JpaRepository<CopyOperationEventEntity, UUID> {
    Optional<CopyOperationEventEntity> findByClientOrderId(String clientOrderId);

    @Query(value = "select pg_advisory_xact_lock(hashtextextended(cast(:lockKey as text), 0))", nativeQuery = true)
    Object lockDispatchProgress(@Param("lockKey") String lockKey);

    @Query(value = """
            select *
            from futuros_operaciones.copy_operation_event e
            where e.dispatch_intent_id = :dispatchIntentId
              and e.event_type = :eventType
              and coalesce(e.qty_executed, 0) = coalesce(:qtyExecuted, 0)
              and coalesce(e.resulting_qty, 0) = coalesce(:resultingQty, 0)
            order by e.date_creation asc, e.id_event asc
            limit 1
            """, nativeQuery = true)
    Optional<CopyOperationEventEntity> findDispatchProgress(
            @Param("dispatchIntentId") UUID dispatchIntentId,
            @Param("eventType") String eventType,
            @Param("qtyExecuted") BigDecimal qtyExecuted,
            @Param("resultingQty") BigDecimal resultingQty
    );

    @Query(value = """
            select count(*)
            from futuros_operaciones.copy_operation_event e
            where e.user_copy_allocation_id = :allocationId
              and coalesce(e.execution_mode, 'LIVE') = :executionMode
              and coalesce(e.is_shadow, false) = false
            """, nativeQuery = true)
    long countRuntimeEventsForAllocation(
            @Param("allocationId") Long allocationId,
            @Param("executionMode") String executionMode
    );

    @Query(value = """
            select count(*)
            from futuros_operaciones.copy_operation_event e
            where e.user_copy_allocation_id = :allocationId
              and coalesce(e.execution_mode, 'LIVE') = :executionMode
              and coalesce(e.is_shadow, false) = false
              and (
                    lower(coalesce(e.decision, '')) in ('rejected', 'error', 'failed', 'skipped')
                 or lower(coalesce(e.reason_code, '')) like '%error%'
                 or lower(coalesce(e.reason_code, '')) like '%failed%'
                 or lower(coalesce(e.reason_code, '')) like '%rejected%'
                 or lower(coalesce(e.reason_code, '')) like '%price_source_unavailable%'
              )
            """, nativeQuery = true)
    long countRuntimeErrorEventsForAllocation(
            @Param("allocationId") Long allocationId,
            @Param("executionMode") String executionMode
    );

    @Query(value = """
            select coalesce(sum(e.realized_pnl_usd), 0)
            from futuros_operaciones.copy_operation_event e
            where e.user_copy_allocation_id = :allocationId
              and coalesce(e.execution_mode, 'LIVE') = :executionMode
              and coalesce(e.is_shadow, false) = false
            """, nativeQuery = true)
    BigDecimal sumRuntimeRealizedPnlUsdForAllocation(
            @Param("allocationId") Long allocationId,
            @Param("executionMode") String executionMode
    );

    @Query(value = """
            select min(e.event_time)
            from futuros_operaciones.copy_operation_event e
            where e.user_copy_allocation_id = :allocationId
              and coalesce(e.execution_mode, 'LIVE') = :executionMode
              and coalesce(e.is_shadow, false) = false
            """, nativeQuery = true)
    OffsetDateTime findFirstRuntimeEventTimeForAllocation(
            @Param("allocationId") Long allocationId,
            @Param("executionMode") String executionMode
    );
}
