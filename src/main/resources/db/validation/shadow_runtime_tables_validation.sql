-- Post-deploy validation for SHADOW runtime schema.
-- Run manually after Flyway migration if shadow tables were dropped/recreated.

WITH required_tables(table_name) AS (
    VALUES
        ('copy_wallet_profile'),
        ('shadow_wallet_profile_validation'),
        ('shadow_copy_allocation'),
        ('shadow_copy_operation'),
        ('shadow_copy_operation_event'),
        ('shadow_position_state')
), table_status AS (
    SELECT
        r.table_name,
        CASE WHEN t.table_name IS NULL THEN 'MISSING' ELSE 'OK' END AS status
    FROM required_tables r
    LEFT JOIN information_schema.tables t
      ON t.table_schema = 'futuros_operaciones'
     AND t.table_name = r.table_name
)
SELECT * FROM table_status ORDER BY table_name;

WITH required_indexes(index_name) AS (
    VALUES
        ('ux_copy_wallet_profile_key'),
        ('ux_shadow_copy_allocation_profile_key'),
        ('ux_shadow_event_wallet_profile_idempotency'),
        ('ux_shadow_position_state_wallet_profile_open'),
        ('ix_shadow_position_state_wallet_profile_closed'),
        ('ix_shadow_operation_wallet_profile_position_active')
), index_status AS (
    SELECT
        r.index_name,
        CASE WHEN i.indexname IS NULL THEN 'MISSING' ELSE 'OK' END AS status
    FROM required_indexes r
    LEFT JOIN pg_indexes i
      ON i.schemaname = 'futuros_operaciones'
     AND i.indexname = r.index_name
)
SELECT * FROM index_status ORDER BY index_name;

SELECT
    'shadow_copy_operation_event_without_profile' AS check_name,
    count(*) AS rows_count
FROM futuros_operaciones.shadow_copy_operation_event
WHERE wallet_profile_id IS NULL;

SELECT
    'shadow_position_state_open_by_profile' AS check_name,
    wallet_profile_id,
    parsymbol,
    position_side,
    count(*) AS open_rows
FROM futuros_operaciones.shadow_position_state
WHERE status = 'OPEN'
GROUP BY wallet_profile_id, parsymbol, position_side
HAVING count(*) > 1;

SELECT
    'shadow_closed_position_with_open_operation' AS check_name,
    o.id_operation,
    o.wallet_profile_id,
    o.shadow_allocation_id,
    o.parsymbol,
    o.type_operation,
    o.status AS operation_status,
    o.date_creation,
    o.date_close,
    o.realized_pnl_usd AS operation_realized_pnl,
    p.id AS position_id,
    p.status AS position_status,
    p.opened_at,
    p.closed_at,
    p.realized_pnl_usd AS position_realized_pnl
FROM futuros_operaciones.shadow_copy_operation o
JOIN futuros_operaciones.shadow_position_state p
  ON p.wallet_profile_id = o.wallet_profile_id
 AND p.shadow_allocation_id = o.shadow_allocation_id
 AND p.parsymbol = o.parsymbol
 AND p.position_side = o.type_operation
WHERE p.status = 'CLOSED'
  AND o.status = 'OPEN';

SELECT
    'shadow_position_event_without_position_id' AS check_name,
    *
FROM futuros_operaciones.shadow_copy_operation_event
WHERE reason_code IN (
    'SHADOW_POSITION_CLOSED',
    'SHADOW_POSITION_CLOSED_BY_FLIP',
    'SHADOW_POSITION_OPENED',
    'SHADOW_POSITION_OPENED_BY_FLIP',
    'SHADOW_POSITION_RESIZED'
)
AND shadow_position_id IS NULL;

SELECT
    'shadow_flip_audit' AS check_name,
    event_type,
    reason_code,
    decision,
    count(*) AS events
FROM futuros_operaciones.shadow_copy_operation_event
WHERE reason_code ILIKE '%FLIP%'
GROUP BY event_type, reason_code, decision
ORDER BY events DESC;

SELECT
    'shadow_event_decision_counts' AS check_name,
    shadow_validation_id,
    decision,
    count(*) AS events
FROM futuros_operaciones.shadow_copy_operation_event
GROUP BY shadow_validation_id, decision
ORDER BY shadow_validation_id, decision;

SELECT
    'shadow_validation_event_counters' AS check_name,
    id AS id_validation,
    simulated_events,
    recorded_events,
    skipped_events,
    duplicate_events,
    error_events
FROM futuros_operaciones.shadow_wallet_profile_validation
ORDER BY id;
