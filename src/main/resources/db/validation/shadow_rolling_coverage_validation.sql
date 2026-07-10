-- Read-only post-deploy validation for SHADOW rolling coverage.
-- Production formula preserves legacy decision semantics:
-- successful = greatest(SIMULATED, RECORDED)
-- countable failures = SKIPPED + ERROR; DUPLICATE is excluded.

-- 1. Historical and rolling coverage by exact allocation.
WITH params AS (
    SELECT now() AS window_end,
           now() - interval '14 days' AS window_start,
           500::integer AS max_events,
           100::bigint AS min_events,
           95::numeric AS threshold_pct
), coverage AS (
    SELECT a.id AS shadow_allocation_id,
           a.wallet_id,
           a.copy_strategy_code,
           a.scope_type,
           a.scope_value,
           a.linked_live_allocation_id,
           greatest(h.simulated_events, h.recorded_events) AS historical_simulated,
           h.recorded_events AS historical_recorded_raw,
           h.skipped_events AS historical_skipped,
           h.error_events AS historical_errors,
           greatest(r.simulated_events, r.recorded_events) AS rolling_simulated,
           r.recorded_events AS rolling_recorded_raw,
           r.skipped_events AS rolling_skipped,
           r.error_events AS rolling_errors,
           r.oldest_event_time,
           r.newest_event_time,
           p.*
    FROM futuros_operaciones.shadow_copy_allocation a
    CROSS JOIN params p
    CROSS JOIN LATERAL (
        SELECT count(*) FILTER (WHERE e.decision = 'SIMULATED') AS simulated_events,
               count(*) FILTER (WHERE e.decision = 'RECORDED') AS recorded_events,
               count(*) FILTER (WHERE e.decision = 'SKIPPED') AS skipped_events,
               count(*) FILTER (WHERE e.decision = 'ERROR') AS error_events
        FROM futuros_operaciones.shadow_copy_operation_event e
        WHERE e.shadow_allocation_id = a.id
          AND e.decision IN ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR')
    ) h
    CROSS JOIN LATERAL (
        SELECT count(*) FILTER (WHERE recent.decision = 'SIMULATED') AS simulated_events,
               count(*) FILTER (WHERE recent.decision = 'RECORDED') AS recorded_events,
               count(*) FILTER (WHERE recent.decision = 'SKIPPED') AS skipped_events,
               count(*) FILTER (WHERE recent.decision = 'ERROR') AS error_events,
               min(recent.event_time) AS oldest_event_time,
               max(recent.event_time) AS newest_event_time
        FROM (
            SELECT e.decision, e.event_time
            FROM futuros_operaciones.shadow_copy_operation_event e
            WHERE e.shadow_allocation_id = a.id
              AND e.event_time >= p.window_start
              AND e.event_time <= p.window_end
              AND e.decision IN ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR')
            ORDER BY e.event_time DESC, e.id_event DESC
            LIMIT p.max_events
        ) recent
    ) r
    WHERE a.is_active = true
      AND a.ends_at IS NULL
)
SELECT shadow_allocation_id,
       wallet_id,
       copy_strategy_code,
       scope_type,
       scope_value,
       historical_simulated,
       historical_recorded_raw,
       historical_skipped,
       historical_errors,
       historical_simulated + historical_skipped + historical_errors AS historical_evaluable,
       round(100 * historical_simulated::numeric
             / nullif(historical_simulated + historical_skipped + historical_errors, 0), 6) AS historical_coverage_pct,
       rolling_simulated,
       rolling_recorded_raw,
       rolling_skipped,
       rolling_errors,
       rolling_simulated + rolling_skipped + rolling_errors AS rolling_evaluable,
       round(100 * rolling_simulated::numeric
             / nullif(rolling_simulated + rolling_skipped + rolling_errors, 0), 6) AS rolling_coverage_pct,
       window_start,
       window_end,
       oldest_event_time,
       newest_event_time,
       CASE
           WHEN linked_live_allocation_id IS NOT NULL THEN 'YA_PROMOVIDA'
           WHEN rolling_simulated + rolling_skipped + rolling_errors = 0 THEN 'SHADOW_COVERAGE_ROLLING_NO_EVENTS'
           WHEN rolling_simulated + rolling_skipped + rolling_errors < min_events THEN 'SHADOW_COVERAGE_ROLLING_INSUFFICIENT_SAMPLE'
           WHEN 100 * rolling_simulated::numeric
                    / nullif(rolling_simulated + rolling_skipped + rolling_errors, 0) >= threshold_pct
               THEN 'SHADOW_COVERAGE_ROLLING_READY'
           ELSE 'SHADOW_COVERAGE_ROLLING_BELOW_THRESHOLD'
       END AS coverage_reason_code
FROM coverage
ORDER BY shadow_allocation_id;

-- 2. Daily coverage for the last 14 UTC days.
WITH daily AS (
    SELECT e.shadow_allocation_id,
           date_trunc('day', e.event_time AT TIME ZONE 'UTC') AS event_day_utc,
           count(*) FILTER (WHERE e.decision = 'SIMULATED') AS simulated_events,
           count(*) FILTER (WHERE e.decision = 'RECORDED') AS recorded_events,
           count(*) FILTER (WHERE e.decision = 'SKIPPED') AS skipped_events,
           count(*) FILTER (WHERE e.decision = 'ERROR') AS error_events
    FROM futuros_operaciones.shadow_copy_operation_event e
    WHERE e.event_time >= now() - interval '14 days'
      AND e.event_time <= now()
      AND e.decision IN ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR')
    GROUP BY e.shadow_allocation_id, date_trunc('day', e.event_time AT TIME ZONE 'UTC')
)
SELECT shadow_allocation_id,
       event_day_utc,
       greatest(simulated_events, recorded_events) AS successful_events,
       skipped_events,
       error_events,
       greatest(simulated_events, recorded_events) + skipped_events + error_events AS evaluable_events,
       round(100 * greatest(simulated_events, recorded_events)::numeric
             / nullif(greatest(simulated_events, recorded_events) + skipped_events + error_events, 0), 6) AS coverage_pct
FROM daily
ORDER BY shadow_allocation_id, event_day_utc DESC;

-- 3. Exact events used by the rolling window. Replace 12 as needed.
SELECT e.id_event,
       e.shadow_allocation_id,
       e.id_order_origin,
       e.copy_strategy_code,
       e.scope_type,
       e.scope_value,
       e.event_type,
       e.position_side,
       e.decision,
       e.reason_code,
       e.event_time,
       e.date_creation
FROM futuros_operaciones.shadow_copy_operation_event e
WHERE e.shadow_allocation_id = 12
  AND e.event_time >= now() - interval '14 days'
  AND e.event_time <= now()
  AND e.decision IN ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR')
ORDER BY e.event_time DESC, e.id_event DESC
LIMIT 500;

-- 4. Counts by decision and reason_code in the current rolling time window.
SELECT e.shadow_allocation_id,
       e.decision,
       e.reason_code,
       count(*) AS event_count,
       min(e.event_time) AS first_event_time,
       max(e.event_time) AS last_event_time
FROM futuros_operaciones.shadow_copy_operation_event e
WHERE e.event_time >= now() - interval '14 days'
  AND e.event_time <= now()
GROUP BY e.shadow_allocation_id, e.decision, e.reason_code
ORDER BY e.shadow_allocation_id, event_count DESC, e.decision, e.reason_code;

-- 5. Rolling-ready allocations whose latest promoter result failed another gate.
WITH rolling AS (
    SELECT a.id AS shadow_allocation_id,
           a.wallet_id,
           a.copy_strategy_code,
           a.scope_type,
           a.scope_value,
           greatest(x.simulated_events, x.recorded_events) AS successful_events,
           x.skipped_events,
           x.error_events
    FROM futuros_operaciones.shadow_copy_allocation a
    CROSS JOIN LATERAL (
        SELECT count(*) FILTER (WHERE recent.decision = 'SIMULATED') AS simulated_events,
               count(*) FILTER (WHERE recent.decision = 'RECORDED') AS recorded_events,
               count(*) FILTER (WHERE recent.decision = 'SKIPPED') AS skipped_events,
               count(*) FILTER (WHERE recent.decision = 'ERROR') AS error_events
        FROM (
            SELECT e.decision
            FROM futuros_operaciones.shadow_copy_operation_event e
            WHERE e.shadow_allocation_id = a.id
              AND e.event_time >= now() - interval '14 days'
              AND e.event_time <= now()
              AND e.decision IN ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR')
            ORDER BY e.event_time DESC, e.id_event DESC
            LIMIT 500
        ) recent
    ) x
    WHERE a.is_active = true
      AND a.ends_at IS NULL
      AND a.linked_live_allocation_id IS NULL
)
SELECT r.*,
       r.successful_events + r.skipped_events + r.error_events AS rolling_evaluable,
       round(100 * r.successful_events::numeric
             / nullif(r.successful_events + r.skipped_events + r.error_events, 0), 6) AS rolling_coverage_pct,
       latest.decision AS latest_promotion_decision,
       latest.reason_code AS other_gate_reason_code,
       latest.created_at AS evaluated_at
FROM rolling r
CROSS JOIN LATERAL (
    SELECT a.decision, a.reason_code, a.created_at
    FROM futuros_operaciones.copy_promotion_audit a
    WHERE a.shadow_allocation_id = r.shadow_allocation_id
    ORDER BY a.created_at DESC, a.id DESC
    LIMIT 1
) latest
WHERE r.successful_events + r.skipped_events + r.error_events >= 100
  AND 100 * r.successful_events::numeric
          / nullif(r.successful_events + r.skipped_events + r.error_events, 0) >= 95
  AND latest.decision = 'SHADOW_PROMOTION_REJECTED'
  AND latest.reason_code NOT IN (
      'SHADOW_NOT_READY_COVERAGE',
      'SHADOW_COVERAGE_ROLLING_BELOW_THRESHOLD',
      'SHADOW_COVERAGE_ROLLING_INSUFFICIENT_SAMPLE',
      'SHADOW_COVERAGE_ROLLING_NO_EVENTS',
      'SHADOW_COVERAGE_ROLLING_QUERY_FAILED'
  )
ORDER BY latest.created_at DESC;

-- 6. Allocations whose latest exact blocker is insufficient rolling sample.
SELECT a.id AS shadow_allocation_id,
       a.wallet_id,
       a.copy_strategy_code,
       a.scope_type,
       a.scope_value,
       audit.reason_code,
       audit.reason_details ->> 'rollingEvaluableEvents' AS rolling_evaluable_events,
       audit.reason_details ->> 'rollingCoveragePct' AS rolling_coverage_pct,
       audit.created_at
FROM futuros_operaciones.shadow_copy_allocation a
CROSS JOIN LATERAL (
    SELECT p.reason_code, p.reason_details, p.created_at
    FROM futuros_operaciones.copy_promotion_audit p
    WHERE p.shadow_allocation_id = a.id
    ORDER BY p.created_at DESC, p.id DESC
    LIMIT 1
) audit
WHERE a.is_active = true
  AND a.ends_at IS NULL
  AND a.linked_live_allocation_id IS NULL
  AND audit.reason_code = 'SHADOW_COVERAGE_ROLLING_INSUFFICIENT_SAMPLE'
ORDER BY audit.created_at DESC;

-- 7. Historical versus rolling comparison, based on emitted audit snapshots.
SELECT DISTINCT ON (a.shadow_allocation_id)
       a.shadow_allocation_id,
       a.wallet_id,
       a.copy_strategy_code,
       a.reason_details ->> 'coverageSourceUsed' AS coverage_source_used,
       (a.reason_details ->> 'historicalCoveragePct')::numeric AS historical_coverage_pct,
       (a.reason_details ->> 'rollingCoveragePct')::numeric AS rolling_coverage_pct,
       (a.reason_details ->> 'rollingCoveragePct')::numeric
           - (a.reason_details ->> 'historicalCoveragePct')::numeric AS coverage_delta_pct,
       (a.reason_details ->> 'rollingEvaluableEvents')::bigint AS rolling_evaluable_events,
       a.reason_details ->> 'coverageDecision' AS coverage_decision,
       a.reason_details ->> 'coverageReasonCode' AS coverage_reason_code,
       a.created_at
FROM futuros_operaciones.copy_promotion_audit a
WHERE a.shadow_allocation_id IS NOT NULL
  AND a.reason_details ? 'historicalCoveragePct'
  AND a.reason_details ? 'rollingCoveragePct'
ORDER BY a.shadow_allocation_id, a.created_at DESC, a.id DESC;

-- 8. Requested validation for allocations 12, 80 and 85.
WITH requested(id) AS (VALUES (12::bigint), (80::bigint), (85::bigint))
SELECT a.id AS shadow_allocation_id,
       a.wallet_id,
       a.copy_strategy_code,
       a.scope_type,
       a.scope_value,
       a.status,
       a.linked_live_allocation_id,
       CASE WHEN a.linked_live_allocation_id IS NOT NULL THEN 'YA_PROMOVIDA' ELSE 'PENDIENTE_EVALUACION' END AS promotion_state,
       latest.decision AS latest_promotion_decision,
       latest.reason_code AS latest_reason_code,
       latest.reason_details ->> 'historicalCoveragePct' AS historical_coverage_pct,
       latest.reason_details ->> 'rollingCoveragePct' AS rolling_coverage_pct,
       latest.reason_details ->> 'rollingEvaluableEvents' AS rolling_evaluable_events,
       latest.reason_details ->> 'coverageSourceUsed' AS coverage_source_used,
       latest.created_at AS evaluated_at
FROM requested q
LEFT JOIN futuros_operaciones.shadow_copy_allocation a ON a.id = q.id
LEFT JOIN LATERAL (
    SELECT p.decision, p.reason_code, p.reason_details, p.created_at
    FROM futuros_operaciones.copy_promotion_audit p
    WHERE p.shadow_allocation_id = a.id
    ORDER BY p.created_at DESC, p.id DESC
    LIMIT 1
) latest ON true
ORDER BY q.id;

-- 9. event_time quality: current schema expects zero nulls and zero future events.
SELECT count(*) AS total_events,
       count(*) FILTER (WHERE event_time IS NULL) AS null_event_time,
       count(*) FILTER (WHERE event_time > now()) AS future_event_time,
       count(*) FILTER (WHERE date_creation IS NULL) AS null_date_creation,
       round(100 * count(*) FILTER (WHERE event_time IS NULL)::numeric
             / nullif(count(*), 0), 6) AS null_event_time_pct
FROM futuros_operaciones.shadow_copy_operation_event;

-- 10. Execute on a read-only replica/session and inspect actual plan for one allocation.
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT count(*) FILTER (WHERE recent.decision = 'SIMULATED') AS simulated_events,
       count(*) FILTER (WHERE recent.decision = 'RECORDED') AS recorded_events,
       count(*) FILTER (WHERE recent.decision = 'SKIPPED') AS skipped_events,
       count(*) FILTER (WHERE recent.decision = 'ERROR') AS error_events
FROM (
    SELECT e.decision, e.event_time
    FROM futuros_operaciones.shadow_copy_operation_event e
    WHERE e.shadow_allocation_id = 12
      AND e.event_time >= now() - interval '14 days'
      AND e.event_time <= now()
      AND e.decision IN ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR')
    ORDER BY e.event_time DESC, e.id_event DESC
    LIMIT 500
) recent;

-- 11. Index existence, validity and usage counters.
SELECT n.nspname AS schema_name,
       c.relname AS index_name,
       i.indisvalid,
       i.indisready,
       pg_size_pretty(pg_relation_size(c.oid)) AS index_size,
       s.idx_scan,
       s.idx_tup_read,
       s.idx_tup_fetch
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_index i ON i.indexrelid = c.oid
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = c.oid
WHERE n.nspname = 'futuros_operaciones'
  AND c.relname = 'ix_shadow_event_allocation_rolling_coverage';
