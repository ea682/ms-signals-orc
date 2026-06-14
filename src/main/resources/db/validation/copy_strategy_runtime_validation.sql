-- 1) No debe existir mas de una copia activa para la misma estrategia.
SELECT
    id_order_origin,
    id_user,
    type_operation,
    COALESCE(copy_strategy_code, 'MOVEMENT_ALL') AS copy_strategy_code,
    count(*) AS active_rows
FROM futuros_operaciones.copy_operation
WHERE is_active = true
GROUP BY id_order_origin, id_user, type_operation, COALESCE(copy_strategy_code, 'MOVEMENT_ALL')
HAVING count(*) > 1;

-- 2) Diagnostico permitido: mismo usuario/origen con varias estrategias activas.
-- Esto ya no es error si cada fila tiene estrategia distinta.
SELECT
    id_order_origin,
    id_user,
    type_operation,
    count(DISTINCT COALESCE(copy_strategy_code, 'MOVEMENT_ALL')) AS active_strategies,
    string_agg(DISTINCT COALESCE(copy_strategy_code, 'MOVEMENT_ALL'), ', ' ORDER BY COALESCE(copy_strategy_code, 'MOVEMENT_ALL')) AS strategies
FROM futuros_operaciones.copy_operation
WHERE is_active = true
GROUP BY id_order_origin, id_user, type_operation
HAVING count(DISTINCT COALESCE(copy_strategy_code, 'MOVEMENT_ALL')) > 1
ORDER BY active_strategies DESC;

-- 3) Nuevas aperturas sin allocation no deberian aparecer salvo legacy/manual.
SELECT
    id_operation,
    id_order_origin,
    id_user,
    id_wallet_origin,
    parsymbol,
    type_operation,
    copy_strategy_code,
    user_copy_allocation_id,
    execution_mode,
    is_shadow,
    date_creation
FROM futuros_operaciones.copy_operation
WHERE is_active = true
  AND user_copy_allocation_id IS NULL
ORDER BY date_creation DESC
LIMIT 100;

-- 4) Eventos recientes por decision/estrategia para Loki/SQL.
SELECT
    execution_mode,
    is_shadow,
    COALESCE(copy_strategy_code, 'MOVEMENT_ALL') AS copy_strategy_code,
    decision,
    decision_reason,
    event_type,
    copy_intent,
    count(*) AS rows_count,
    min(event_time) AS first_event_time,
    max(event_time) AS last_event_time
FROM futuros_operaciones.copy_operation_event
WHERE event_time >= now() - interval '24 hours'
GROUP BY execution_mode, is_shadow, COALESCE(copy_strategy_code, 'MOVEMENT_ALL'), decision, decision_reason, event_type, copy_intent
ORDER BY rows_count DESC;

-- 5) Asignaciones activas por estado y estrategia.
SELECT
    copy_strategy_code,
    execution_mode,
    status,
    count(*) AS rows_count,
    min(status_updated_at) AS oldest_status_update,
    max(status_updated_at) AS newest_status_update
FROM futuros_operaciones.user_copy_allocation
WHERE ends_at IS NULL
  AND is_active = true
GROUP BY copy_strategy_code, execution_mode, status
ORDER BY rows_count DESC;
