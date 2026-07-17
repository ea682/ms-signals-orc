-- Read-only validation after V202607110002. Any returned row requires review.
WITH expected_indexes(index_name) AS (
    VALUES
        ('ix_copy_dispatch_intent_micro_wallet_budget'),
        ('ix_copy_operation_micro_wallet_budget')
)
SELECT 'futuros_operaciones' AS schema_name,
       expected.index_name,
       (idx.indexrelid IS NOT NULL) AS index_exists,
       coalesce(idx.indisvalid, false) AS indisvalid,
       coalesce(idx.indisready, false) AS indisready
FROM expected_indexes expected
LEFT JOIN pg_namespace namespace
  ON namespace.nspname = 'futuros_operaciones'
LEFT JOIN pg_class relation
  ON relation.relnamespace = namespace.oid
 AND relation.relname = expected.index_name
LEFT JOIN pg_index idx
  ON idx.indexrelid = relation.oid
WHERE idx.indexrelid IS NULL
   OR NOT idx.indisvalid
   OR NOT idx.indisready;

SELECT id, id_user, user_copy_allocation_id, status, reservation_status
FROM futuros_operaciones.copy_dispatch_intent
WHERE execution_mode = 'MICRO_LIVE'
  AND reservation_status = 'PENDING'
  AND (wallet_id IS NULL OR btrim(wallet_id) = '');

WITH active AS (
    SELECT id_user,
           lower(id_wallet_origin) AS wallet_id,
           sum(size_usd / nullif(leverage, 0)) AS used_margin_usdc,
           count(*) AS open_positions
    FROM futuros_operaciones.copy_operation
    WHERE execution_mode = 'MICRO_LIVE'
      AND is_active = true
      AND coalesce(is_shadow, false) = false
    GROUP BY id_user, lower(id_wallet_origin)
), pending AS (
    SELECT id_user,
           lower(wallet_id) AS wallet_id,
           sum(requested_margin_usd) AS pending_margin_usdc,
           sum(reserved_position_count) AS pending_positions
    FROM futuros_operaciones.copy_dispatch_intent
    WHERE execution_mode = 'MICRO_LIVE'
      AND reservation_status = 'PENDING'
    GROUP BY id_user, lower(wallet_id)
)
SELECT coalesce(a.id_user, p.id_user) AS id_user,
       coalesce(a.wallet_id, p.wallet_id) AS wallet_id,
       coalesce(a.used_margin_usdc, 0) AS used_margin_usdc,
       coalesce(p.pending_margin_usdc, 0) AS pending_margin_usdc,
       coalesce(a.open_positions, 0) AS open_positions,
       coalesce(p.pending_positions, 0) AS pending_positions
FROM active a
FULL OUTER JOIN pending p
  ON p.id_user = a.id_user AND p.wallet_id = a.wallet_id
WHERE coalesce(a.used_margin_usdc, 0) + coalesce(p.pending_margin_usdc, 0) > 100;

-- Position count is constrained only when the user configured an explicit
-- allocation limit. V3 has no global five-position threshold and no fixed
-- per-order margin threshold.
WITH active_positions AS (
    SELECT user_copy_allocation_id,
           count(*) AS open_positions
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
SELECT allocation.id AS user_copy_allocation_id,
       allocation.id_user,
       allocation.wallet_id,
       allocation.user_max_concurrent_positions,
       coalesce(active.open_positions, 0) AS open_positions,
       coalesce(pending.reserved_positions, 0) AS reserved_positions
FROM futuros_operaciones.user_copy_allocation allocation
LEFT JOIN active_positions active
  ON active.user_copy_allocation_id = allocation.id
LEFT JOIN pending_positions pending
  ON pending.user_copy_allocation_id = allocation.id
WHERE allocation.execution_mode = 'MICRO_LIVE'
  AND allocation.user_max_concurrent_positions IS NOT NULL
  AND coalesce(active.open_positions, 0) + coalesce(pending.reserved_positions, 0)
      > allocation.user_max_concurrent_positions;
