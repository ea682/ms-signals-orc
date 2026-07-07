-- Post-deploy validation for SHADOW -> MICRO_LIVE -> LIVE promotion.
-- Run after Flyway migrations with:
-- psql "<DB_URL>" -f src/main/resources/db/validation/shadow_micro_live_promotion_validation.sql

SET search_path TO futuros_operaciones, public;
\pset pager off
\pset null '<null>'

SELECT
    'tables' AS check_name,
    to_regclass('futuros_operaciones.user_wallet_copy_plan') IS NOT NULL AS user_wallet_copy_plan,
    to_regclass('futuros_operaciones.copy_promotion_audit') IS NOT NULL AS copy_promotion_audit,
    to_regclass('futuros_operaciones.shadow_copy_allocation') IS NOT NULL AS shadow_copy_allocation,
    to_regclass('futuros_operaciones.user_copy_allocation') IS NOT NULL AS user_copy_allocation;

SELECT
    'required_indexes' AS check_name,
    r.index_name,
    CASE WHEN i.indexname IS NULL THEN 'MISSING' ELSE 'OK' END AS status
FROM (
    VALUES
        ('ix_shadow_copy_allocation_promotion_candidates'),
        ('ix_user_copy_allocation_micro_live_promotion'),
        ('ix_copy_operation_event_allocation_mode_time'),
        ('ux_user_copy_allocation_user_wallet_strategy_scope_active')
) AS r(index_name)
LEFT JOIN pg_indexes i
  ON i.schemaname = 'futuros_operaciones'
 AND i.indexname = r.index_name
ORDER BY r.index_name;

SELECT
    'flyway_recent' AS check_name,
    installed_rank,
    version,
    description,
    success,
    installed_on
FROM futuros_operaciones.flyway_schema_history
WHERE version >= '202607040001'
ORDER BY installed_rank DESC
LIMIT 20;

SELECT
    'active_allocations_by_mode' AS check_name,
    COALESCE(execution_mode, 'LIVE') AS execution_mode,
    count(*) AS rows
FROM futuros_operaciones.user_copy_allocation
WHERE ends_at IS NULL
  AND is_active = true
GROUP BY COALESCE(execution_mode, 'LIVE')
ORDER BY execution_mode;

SELECT
    'shadow_candidates_unlinked' AS check_name,
    count(*) AS rows
FROM futuros_operaciones.shadow_copy_allocation
WHERE is_active = true
  AND ends_at IS NULL
  AND linked_live_allocation_id IS NULL;

SELECT
    'duplicate_active_user_wallet_strategy_scope' AS check_name,
    id_user,
    lower(wallet_id) AS wallet_lc,
    copy_strategy_code,
    scope_type,
    scope_value,
    count(*) AS active_rows,
    string_agg(id::text || ':' || COALESCE(execution_mode, 'LIVE'), ', ' ORDER BY id) AS allocations
FROM futuros_operaciones.user_copy_allocation
WHERE ends_at IS NULL
  AND is_active = true
  AND COALESCE(execution_mode, 'LIVE') IN ('MICRO_LIVE', 'LIVE')
GROUP BY id_user, lower(wallet_id), copy_strategy_code, scope_type, scope_value
HAVING count(*) > 1
ORDER BY active_rows DESC, wallet_lc
LIMIT 20;

SELECT
    'broken_shadow_links' AS check_name,
    s.id AS shadow_id,
    s.wallet_id,
    s.copy_strategy_code,
    s.linked_live_allocation_id
FROM futuros_operaciones.shadow_copy_allocation s
LEFT JOIN futuros_operaciones.user_copy_allocation u
       ON u.id = s.linked_live_allocation_id
WHERE s.linked_live_allocation_id IS NOT NULL
  AND u.id IS NULL
LIMIT 20;

SELECT
    'recent_promotion_audit' AS check_name,
    id,
    created_at,
    wallet_id,
    copy_strategy_code,
    source_execution_mode,
    target_execution_mode,
    decision,
    reason_code,
    shadow_allocation_id,
    micro_live_allocation_id,
    live_allocation_id
FROM futuros_operaciones.copy_promotion_audit
ORDER BY created_at DESC
LIMIT 20;

SELECT
    'target_wallet_shadow' AS check_name,
    id,
    id_user,
    wallet_id,
    copy_strategy_code,
    scope_type,
    scope_value,
    status,
    is_active,
    ends_at,
    linked_live_allocation_id,
    copy_guard_status,
    copy_guard_action,
    last_validation_reason,
    decision_score,
    strategy_score,
    updated_at
FROM futuros_operaciones.shadow_copy_allocation
WHERE lower(wallet_id) = '0xa445a0a15b1d50fa0c4bfe6796d9447e0da5329d'
  AND copy_strategy_code = 'MOVEMENT_ALL'
ORDER BY updated_at DESC NULLS LAST
LIMIT 20;

SELECT
    'target_wallet_allocations' AS check_name,
    id,
    id_user,
    wallet_id,
    copy_strategy_code,
    scope_type,
    scope_value,
    status,
    execution_mode,
    is_active,
    ends_at,
    linked_shadow_allocation_id,
    promoted_from_shadow_at,
    target_symbol,
    symbol_resolution_status,
    symbol_resolution_reason,
    allocation_pct,
    updated_at
FROM futuros_operaciones.user_copy_allocation
WHERE lower(wallet_id) = '0xa445a0a15b1d50fa0c4bfe6796d9447e0da5329d'
  AND copy_strategy_code = 'MOVEMENT_ALL'
ORDER BY updated_at DESC NULLS LAST
LIMIT 20;

SELECT
    'target_wallet_audit' AS check_name,
    id,
    created_at,
    wallet_id,
    copy_strategy_code,
    source_execution_mode,
    target_execution_mode,
    decision,
    reason_code,
    shadow_allocation_id,
    micro_live_allocation_id,
    live_allocation_id,
    reason_details
FROM futuros_operaciones.copy_promotion_audit
WHERE lower(wallet_id) = '0xa445a0a15b1d50fa0c4bfe6796d9447e0da5329d'
  AND copy_strategy_code = 'MOVEMENT_ALL'
ORDER BY created_at DESC
LIMIT 20;
