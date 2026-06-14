-- Active duplicate copied positions must not exist.
SELECT
    id_order_origin,
    id_user,
    type_operation,
    count(*) AS active_rows
FROM futuros_operaciones.copy_operation
WHERE is_active = true
GROUP BY id_order_origin, id_user, type_operation
HAVING count(*) > 1;

-- Active copies without allocation linkage should be reviewed.
SELECT
    id_operation,
    id_user,
    id_wallet_origin,
    id_order_origin,
    type_operation,
    parsymbol,
    date_creation
FROM futuros_operaciones.copy_operation
WHERE is_active = true
  AND user_copy_allocation_id IS NULL
ORDER BY date_creation DESC
LIMIT 100;

-- Shadow/live event distribution.
SELECT
    execution_mode,
    is_shadow,
    decision,
    decision_reason,
    count(*) AS rows_count,
    min(event_time) AS first_event_time,
    max(event_time) AS last_event_time
FROM futuros_operaciones.copy_operation_event
WHERE event_time >= now() - interval '24 hours'
GROUP BY execution_mode, is_shadow, decision, decision_reason
ORDER BY rows_count DESC;

-- Recent copy events by intent/action.
SELECT
    copy_strategy_code,
    copy_intent,
    event_type,
    position_side,
    count(*) AS rows_count,
    min(event_time) AS first_event_time,
    max(event_time) AS last_event_time
FROM futuros_operaciones.copy_operation_event
WHERE event_time >= now() - interval '24 hours'
GROUP BY copy_strategy_code, copy_intent, event_type, position_side
ORDER BY rows_count DESC;

-- Active allocation status summary.
SELECT
    execution_mode,
    status,
    copy_strategy_code,
    count(*) AS active_allocations
FROM futuros_operaciones.user_copy_allocation
WHERE ends_at IS NULL
  AND is_active = true
GROUP BY execution_mode, status, copy_strategy_code
ORDER BY active_allocations DESC;
