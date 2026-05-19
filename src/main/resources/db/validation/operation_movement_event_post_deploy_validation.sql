-- Post-deploy validation for operation_movement_event canonical idempotency.
-- Run after deploying V20260520 and after a few real copy-trading movements.

-- 1) Must be empty. If rows appear, the event table has duplicate canonical keys.
SELECT
    movement_key,
    count(*) AS rows_count,
    min(event_time) AS first_event_time,
    max(event_time) AS last_event_time,
    array_agg(id_event ORDER BY date_creation, id_event) AS event_ids
FROM futuros_operaciones.operation_movement_event
GROUP BY movement_key
HAVING count(*) > 1
ORDER BY rows_count DESC, last_event_time DESC;

-- 2) Must be empty or reviewed row by row. Detects semantic duplicates even if movement_key differs.
SELECT *
FROM futuros_operaciones.operation_movement_event_semantic_duplicates_v
ORDER BY rows_count DESC, last_seen_at DESC
LIMIT 100;

-- 3) Must be empty. Dedupe guard and ledger table must not drift.
SELECT *
FROM futuros_operaciones.operation_movement_event_dedupe_drift_v
ORDER BY issue_type, event_time DESC
LIMIT 100;

-- 4) Default partition should stay close to zero. Rows here mean the monthly partition was not pre-created.
SELECT count(*) AS default_partition_rows
FROM futuros_operaciones.operation_movement_event_default;

-- 5) Partition inventory and size.
SELECT *
FROM futuros_operaciones.operation_movement_event_partition_health_v;

-- 6) Quick daily volume sanity check by source/delta/event type.
SELECT
    date_trunc('day', event_time) AS day,
    COALESCE(source, 'unknown') AS source,
    delta_type,
    event_type,
    count(*) AS rows_count,
    count(DISTINCT movement_key) AS distinct_movements,
    count(DISTINCT idempotency_key) FILTER (WHERE idempotency_key IS NOT NULL) AS distinct_external_idempotency_keys
FROM futuros_operaciones.operation_movement_event
WHERE event_time >= now() - interval '7 days'
GROUP BY 1, 2, 3, 4
ORDER BY day DESC, rows_count DESC;
