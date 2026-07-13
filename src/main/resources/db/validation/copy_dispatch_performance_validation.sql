-- Copy dispatch integrity/performance validation.
-- SAFE DEFAULT: this script is read-only and never calls Binance.
-- Run first in staging with the same PostgreSQL major version and representative data.

BEGIN;
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '2s';
SET LOCAL idle_in_transaction_session_timeout = '30s';

-- 1. Schema, constraints and indexes.
SELECT c.conname, pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'futuros_operaciones'
  AND t.relname = 'copy_dispatch_intent'
ORDER BY c.conname;

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'futuros_operaciones'
  AND tablename IN ('copy_dispatch_intent', 'copy_operation', 'copy_operation_event', 'user_copy_allocation')
ORDER BY tablename, indexname;

-- 2. State distribution and oldest backlog.
SELECT execution_mode, status, reservation_status, count(*) AS intents,
       min(created_at) AS oldest_created_at,
       max(updated_at) AS newest_updated_at
FROM futuros_operaciones.copy_dispatch_intent
GROUP BY execution_mode, status, reservation_status
ORDER BY execution_mode, status, reservation_status;

-- 3. PERSISTED must have exchange identity, operation, ledger event and progress.
SELECT id, execution_mode, status, binance_order_id, copy_operation_id,
       copy_operation_event_id, executed_qty, persisted_executed_qty
FROM futuros_operaciones.copy_dispatch_intent
WHERE status = 'PERSISTED'
  AND (
       binance_order_id IS NULL
       OR copy_operation_id IS NULL
       OR copy_operation_event_id IS NULL
       OR coalesce(executed_qty, 0) <= 0
       OR coalesce(persisted_executed_qty, 0) <= 0
       OR persisted_executed_qty > executed_qty
  )
ORDER BY updated_at DESC;

-- 4. Links must resolve to the required local records.
SELECT i.id, i.copy_operation_id, i.copy_operation_event_id,
       (o.id_operation IS NOT NULL) AS operation_exists,
       (e.id_event IS NOT NULL) AS event_exists,
       e.dispatch_intent_id AS event_dispatch_intent_id
FROM futuros_operaciones.copy_dispatch_intent i
LEFT JOIN futuros_operaciones.copy_operation o ON o.id_operation = i.copy_operation_id
LEFT JOIN futuros_operaciones.copy_operation_event e ON e.id_event = i.copy_operation_event_id
WHERE i.status IN ('PERSISTED', 'PARTIALLY_FILLED')
  AND (o.id_operation IS NULL OR e.id_event IS NULL OR e.dispatch_intent_id IS DISTINCT FROM i.id)
ORDER BY i.updated_at DESC;

-- 5. No duplicate idempotency key or Binance economic identity per user.
SELECT idempotency_key, count(*) AS duplicate_count
FROM futuros_operaciones.copy_dispatch_intent
GROUP BY idempotency_key
HAVING count(*) > 1;

SELECT id_user, binance_order_id, count(*) AS duplicate_count
FROM futuros_operaciones.copy_dispatch_intent
WHERE binance_order_id IS NOT NULL
GROUP BY id_user, binance_order_id
HAVING count(*) > 1;

SELECT client_order_id, count(*) AS duplicate_count
FROM futuros_operaciones.copy_operation_event
WHERE client_order_id IS NOT NULL
  AND dispatch_intent_id IS NULL
GROUP BY client_order_id
HAVING count(*) > 1;

-- 6. MICRO_LIVE V3 budget snapshot. Capital is shared by user + wallet.
-- There is no fixed per-order margin and no global position-count threshold.
WITH active AS (
    SELECT id_user, lower(id_wallet_origin) AS wallet_id, execution_mode,
           coalesce(sum(size_usd / nullif(leverage, 0)), 0) AS active_margin,
           count(*) AS open_positions
    FROM futuros_operaciones.copy_operation
    WHERE is_active = true
      AND coalesce(is_shadow, false) = false
      AND execution_mode = 'MICRO_LIVE'
    GROUP BY id_user, lower(id_wallet_origin), execution_mode
), pending AS (
    SELECT id_user, lower(wallet_id) AS wallet_id, execution_mode,
           coalesce(sum(requested_margin_usd), 0) AS pending_margin,
           coalesce(sum(reserved_position_count), 0) AS reserved_positions
    FROM futuros_operaciones.copy_dispatch_intent
    WHERE execution_mode = 'MICRO_LIVE'
      AND reservation_status = 'PENDING'
    GROUP BY id_user, lower(wallet_id), execution_mode
), combined AS (
    SELECT coalesce(a.id_user, p.id_user) AS id_user,
           coalesce(a.wallet_id, p.wallet_id) AS wallet_id,
           coalesce(a.active_margin, 0) AS active_margin,
           coalesce(p.pending_margin, 0) AS pending_margin,
           coalesce(a.open_positions, 0) AS open_positions,
           coalesce(p.reserved_positions, 0) AS reserved_positions
    FROM active a
    FULL JOIN pending p USING (id_user, wallet_id, execution_mode)
)
SELECT *, active_margin + pending_margin AS projected_margin,
          open_positions + reserved_positions AS projected_positions
FROM combined
WHERE active_margin + pending_margin > 100
ORDER BY projected_margin DESC;

-- An optional user position limit is checked per allocation only when present.
WITH active_positions AS (
    SELECT user_copy_allocation_id, count(*) AS open_positions
    FROM futuros_operaciones.copy_operation
    WHERE execution_mode = 'MICRO_LIVE'
      AND is_active = true
      AND coalesce(is_shadow, false) = false
      AND user_copy_allocation_id IS NOT NULL
    GROUP BY user_copy_allocation_id
), pending_positions AS (
    SELECT user_copy_allocation_id,
           coalesce(sum(reserved_position_count), 0) AS reserved_positions
    FROM futuros_operaciones.copy_dispatch_intent
    WHERE execution_mode = 'MICRO_LIVE'
      AND reservation_status = 'PENDING'
      AND user_copy_allocation_id IS NOT NULL
    GROUP BY user_copy_allocation_id
)
SELECT allocation.id AS allocation_id,
       allocation.id_user,
       allocation.wallet_id,
       allocation.user_max_concurrent_positions,
       coalesce(active.open_positions, 0) AS open_positions,
       coalesce(pending.reserved_positions, 0) AS reserved_positions
FROM futuros_operaciones.user_copy_allocation allocation
LEFT JOIN active_positions active ON active.user_copy_allocation_id = allocation.id
LEFT JOIN pending_positions pending ON pending.user_copy_allocation_id = allocation.id
WHERE allocation.execution_mode = 'MICRO_LIVE'
  AND allocation.user_max_concurrent_positions IS NOT NULL
  AND coalesce(active.open_positions, 0) + coalesce(pending.reserved_positions, 0)
      > allocation.user_max_concurrent_positions
ORDER BY allocation.id;

-- 7. Ambiguous/manual states must retain a fail-closed reservation unless the
-- economic effect is already confirmed and only price review remains.
SELECT id, status, reservation_status, last_error_code, binance_order_id,
       executed_qty, copy_operation_id, copy_operation_event_id
FROM futuros_operaciones.copy_dispatch_intent
WHERE status IN ('RECONCILING', 'PERSISTENCE_PENDING', 'MANUAL_REVIEW')
  AND reservation_status = 'RELEASED'
ORDER BY updated_at DESC;

-- 8. Reconciliation backlog and age.
SELECT count(*) AS reconciliation_backlog,
       min(created_at) AS oldest_created_at,
       max(reconciliation_attempts) AS max_attempts,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM now() - created_at)) AS age_p50_seconds,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM now() - created_at)) AS age_p95_seconds,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM now() - created_at)) AS age_p99_seconds
FROM futuros_operaciones.copy_dispatch_intent
WHERE status IN ('DISPATCHING', 'NEW', 'PARTIALLY_FILLED', 'FILLED', 'RECONCILING', 'PERSISTENCE_PENDING');

-- 9. Plan: idempotency lookup.
EXPLAIN (COSTS, VERBOSE, SETTINGS)
SELECT *
FROM futuros_operaciones.copy_dispatch_intent
WHERE idempotency_key = coalesce(
    (SELECT idempotency_key FROM futuros_operaciones.copy_dispatch_intent ORDER BY created_at DESC LIMIT 1),
    repeat('0', 64));

-- 10. Plan: atomic allocation budget snapshot.
EXPLAIN (COSTS, VERBOSE, SETTINGS)
WITH sample AS (
    SELECT id_user, user_copy_allocation_id, execution_mode
    FROM futuros_operaciones.copy_dispatch_intent
    WHERE user_copy_allocation_id IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 1
), active AS (
    SELECT coalesce(sum(o.size_usd / nullif(o.leverage, 0)), 0) AS used_margin,
           count(*) AS open_positions
    FROM futuros_operaciones.copy_operation o, sample s
    WHERE o.id_user = s.id_user
      AND o.user_copy_allocation_id = s.user_copy_allocation_id
      AND o.execution_mode = s.execution_mode
      AND o.is_active = true
      AND coalesce(o.is_shadow, false) = false
), pending AS (
    SELECT coalesce(sum(i.requested_margin_usd), 0) AS reserved_margin,
           coalesce(sum(i.reserved_position_count), 0) AS reserved_positions
    FROM futuros_operaciones.copy_dispatch_intent i, sample s
    WHERE i.id_user = s.id_user
      AND i.user_copy_allocation_id = s.user_copy_allocation_id
      AND i.execution_mode = s.execution_mode
      AND i.reservation_status = 'PENDING'
)
SELECT * FROM active CROSS JOIN pending;

-- 11. Plan: reconciliation claim candidate scan. EXPLAIN only; no row locks run.
EXPLAIN (COSTS, VERBOSE, SETTINGS)
SELECT id
FROM futuros_operaciones.copy_dispatch_intent
WHERE (
    (status = 'DISPATCHING' AND updated_at < now() - interval '30 seconds')
    OR (
        status IN ('RECONCILING', 'PERSISTENCE_PENDING', 'NEW', 'PARTIALLY_FILLED', 'FILLED', 'ACKNOWLEDGED', 'PERSISTED')
        AND (next_reconciliation_at IS NULL OR next_reconciliation_at <= now())
        AND (status IN ('NEW', 'PARTIALLY_FILLED') OR copy_operation_id IS NULL
             OR average_price_status = 'PENDING_RESOLUTION')
    )
)
ORDER BY coalesce(next_reconciliation_at, updated_at), created_at, id
LIMIT 50;

-- 12. Plan: required ledger cumulative-progress lookup.
EXPLAIN (COSTS, VERBOSE, SETTINGS)
SELECT e.id_event
FROM futuros_operaciones.copy_operation_event e
WHERE e.dispatch_intent_id = (
        SELECT id FROM futuros_operaciones.copy_dispatch_intent
        WHERE binance_order_id IS NOT NULL ORDER BY updated_at DESC LIMIT 1)
  AND e.event_type = 'OPEN'
  AND coalesce(e.qty_executed, 0) = 0
  AND coalesce(e.resulting_qty, 0) = 0
LIMIT 1;

-- 13. Plan: out-of-band runtime allocation snapshot.
EXPLAIN (COSTS, VERBOSE, SETTINGS)
SELECT *
FROM futuros_operaciones.user_copy_allocation
WHERE ends_at IS NULL
  AND is_active = true
  AND lower(status) = 'active'
  AND coalesce(execution_mode, 'LIVE') IN ('LIVE', 'MICRO_LIVE');

-- 14. Low-cardinality operational gauge source.
SELECT
    count(*) FILTER (WHERE reservation_status = 'PENDING') AS pending_intents,
    count(*) FILTER (WHERE status = 'RECONCILING') AS ambiguous_intents,
    count(*) FILTER (WHERE status = 'PERSISTENCE_PENDING') AS persistence_pending_intents,
    count(*) FILTER (WHERE status = 'MANUAL_REVIEW') AS manual_review_intents,
    coalesce(sum(requested_margin_usd) FILTER (WHERE reservation_status = 'PENDING'), 0) AS reserved_margin
FROM futuros_operaciones.copy_dispatch_intent;

COMMIT;
