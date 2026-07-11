\set ON_ERROR_STOP on
\pset pager off

SET application_name = 'copy_performance_audit';
SET default_transaction_read_only = on;
SET statement_timeout = '15s';
SET lock_timeout = '2s';
SET idle_in_transaction_session_timeout = '30s';
SET search_path = futuros_operaciones, public;

-- Gate de identidad y protecciones.
BEGIN READ ONLY;
SELECT current_user, current_database(), current_schema(),
       inet_server_addr(), inet_server_port();

SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolcanlogin
FROM pg_roles
WHERE rolname = current_user;

SHOW default_transaction_read_only;
SHOW transaction_read_only;
SHOW statement_timeout;
SHOW lock_timeout;
SHOW idle_in_transaction_session_timeout;

SELECT has_database_privilege(current_user, current_database(), 'CONNECT') AS can_connect,
       has_schema_privilege(current_user, current_schema(), 'USAGE') AS can_use_schema,
       pg_has_role(current_user, 'pg_read_all_stats', 'member') AS can_read_all_stats;

SELECT count(*) AS current_tables,
       bool_and(has_table_privilege(current_user, format('%I.%I', schemaname, tablename), 'SELECT'))
           AS select_all_current_tables
FROM pg_tables
WHERE schemaname = current_schema();
ROLLBACK;

-- Servidor, sizes, settings y extensiones.
BEGIN READ ONLY;
SELECT current_setting('server_version') AS server_version,
       current_setting('server_version_num') AS server_version_num,
       pg_postmaster_start_time() AS postmaster_started_at,
       date_trunc('second', clock_timestamp() - pg_postmaster_start_time()) AS uptime;

SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size,
       pg_database_size(current_database()) AS database_bytes;

SELECT pg_size_pretty(coalesce(sum(pg_total_relation_size(c.oid)), 0)) AS schema_size,
       coalesce(sum(pg_total_relation_size(c.oid)), 0) AS schema_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema()
  AND c.relkind IN ('r', 'p', 'm');

SELECT count(*) FILTER (WHERE c.relkind IN ('r', 'p')) AS tables,
       count(*) FILTER (WHERE c.relkind = 'i') AS indexes,
       count(*) FILTER (WHERE c.relkind = 'S') AS sequences
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema();

SELECT extname, extversion FROM pg_extension ORDER BY extname;

SELECT name, setting, unit, source, pending_restart
FROM pg_settings
WHERE name IN (
  'shared_buffers', 'effective_cache_size', 'work_mem', 'maintenance_work_mem',
  'random_page_cost', 'effective_io_concurrency', 'max_connections',
  'checkpoint_timeout', 'max_wal_size', 'wal_compression', 'autovacuum',
  'autovacuum_max_workers', 'autovacuum_naptime',
  'autovacuum_vacuum_scale_factor', 'autovacuum_analyze_scale_factor',
  'track_io_timing', 'shared_preload_libraries',
  'max_parallel_workers_per_gather', 'deadlock_timeout', 'log_lock_waits'
)
ORDER BY name;
ROLLBACK;

-- Connections, waits, locks y database stats.
BEGIN READ ONLY;
SELECT count(*) AS total_sessions,
       count(*) FILTER (WHERE backend_type = 'client backend') AS client_backends,
       count(*) FILTER (WHERE state = 'active') AS active,
       count(*) FILTER (WHERE state = 'idle') AS idle,
       count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
       count(*) FILTER (WHERE cardinality(pg_blocking_pids(pid)) > 0) AS blocked,
       max(clock_timestamp() - xact_start) FILTER (WHERE xact_start IS NOT NULL) AS longest_xact
FROM pg_stat_activity;

SELECT application_name, usename, datname, state, wait_event_type, wait_event,
       count(*) AS sessions
FROM pg_stat_activity
WHERE backend_type = 'client backend'
GROUP BY application_name, usename, datname, state, wait_event_type, wait_event
ORDER BY sessions DESC, application_name;

SELECT locktype, mode, granted, count(*) AS locks
FROM pg_locks
GROUP BY locktype, mode, granted
ORDER BY locktype, granted, mode;

SELECT a.pid AS blocked_pid, pg_blocking_pids(a.pid) AS blocker_pids,
       a.application_name, a.wait_event_type, a.wait_event,
       date_trunc('second', clock_timestamp() - a.query_start) AS blocked_for
FROM pg_stat_activity a
WHERE cardinality(pg_blocking_pids(a.pid)) > 0
ORDER BY a.query_start;

SELECT datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
       round(100.0 * blks_hit / nullif(blks_hit + blks_read, 0), 3) AS cache_hit_pct,
       temp_files, pg_size_pretty(temp_bytes) AS temp_bytes, deadlocks,
       sessions_abandoned, stats_reset
FROM pg_stat_database
WHERE datname = current_database();

SELECT EXISTS (
  SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
) AS pg_stat_statements_installed;
ROLLBACK;

-- Cardinalidad, size, churn y autovacuum de relaciones relevantes.
BEGIN READ ONLY;
SELECT c.relname, c.relkind, c.reltuples::bigint AS estimated_rows,
       s.n_live_tup, s.n_dead_tup,
       round(100.0 * s.n_dead_tup / nullif(s.n_live_tup + s.n_dead_tup, 0), 2) AS dead_pct,
       pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
       pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       s.seq_scan, s.idx_scan, s.n_tup_ins, s.n_tup_upd, s.n_tup_del,
       s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze,
       s.autovacuum_count, s.autoanalyze_count,
       age(c.relfrozenxid) AS xid_age, mxid_age(c.relminmxid) AS mxid_age,
       c.reloptions
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE n.nspname = current_schema()
  AND c.relname IN (
    'copy_dispatch_intent', 'copy_operation', 'copy_operation_event',
    'user_copy_allocation', 'users', 'detail_user', 'user_api_keys',
    'futures_position', 'operation_movement_event',
    'operation_movement_event_dedupe', 'operation_movement_event_2026_05',
    'operation_movement_event_2026_06', 'operation_movement_event_2026_07'
  )
ORDER BY pg_total_relation_size(c.oid) DESC, c.relname;

SELECT parent.relname AS parent_table, child.relname AS partition_table,
       pg_get_expr(child.relpartbound, child.oid) AS partition_bound,
       child.reltuples::bigint AS estimated_rows,
       pg_size_pretty(pg_total_relation_size(child.oid)) AS total_size
FROM pg_inherits i
JOIN pg_class parent ON parent.oid = i.inhparent
JOIN pg_namespace n ON n.oid = parent.relnamespace
JOIN pg_class child ON child.oid = i.inhrelid
WHERE n.nspname = current_schema()
  AND parent.relname = 'operation_movement_event'
ORDER BY child.relname;
ROLLBACK;

-- Indices, validity, uso, I/O y duplicados exactos.
BEGIN READ ONLY;
SELECT t.relname AS table_name, i.relname AS index_name,
       am.amname AS access_method,
       ix.indisprimary, ix.indisunique, ix.indisvalid, ix.indisready,
       ix.indnkeyatts, ix.indnatts,
       pg_size_pretty(pg_relation_size(i.oid)) AS index_size,
       coalesce(si.idx_scan, 0) AS idx_scan,
       coalesce(si.idx_tup_read, 0) AS idx_tup_read,
       coalesce(si.idx_tup_fetch, 0) AS idx_tup_fetch,
       coalesce(io.idx_blks_read, 0) AS idx_blks_read,
       coalesce(io.idx_blks_hit, 0) AS idx_blks_hit,
       pg_get_expr(ix.indpred, ix.indrelid) AS predicate,
       pg_get_indexdef(i.oid) AS definition
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_am am ON am.oid = i.relam
LEFT JOIN pg_stat_all_indexes si ON si.indexrelid = i.oid
LEFT JOIN pg_statio_all_indexes io ON io.indexrelid = i.oid
WHERE n.nspname = current_schema()
  AND t.relname IN (
    'copy_dispatch_intent', 'copy_operation', 'copy_operation_event',
    'user_copy_allocation', 'users', 'detail_user', 'user_api_keys',
    'operation_movement_event', 'operation_movement_event_dedupe',
    'operation_movement_event_2026_06', 'operation_movement_event_2026_07'
  )
ORDER BY t.relname, ix.indisprimary DESC, ix.indisunique DESC, i.relname;

SELECT t.relname AS table_name, i.relname AS index_name,
       ix.indisvalid, ix.indisready, pg_get_indexdef(i.oid) AS definition
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = current_schema()
  AND (NOT ix.indisvalid OR NOT ix.indisready)
ORDER BY t.relname, i.relname;

WITH signatures AS (
  SELECT ix.indrelid, t.relname AS table_name, am.amname,
         ix.indisunique, ix.indisprimary, ix.indisexclusion,
         ix.indkey::text, ix.indclass::text, ix.indcollation::text,
         ix.indoption::text,
         coalesce(pg_get_expr(ix.indexprs, ix.indrelid), '') AS expressions,
         coalesce(pg_get_expr(ix.indpred, ix.indrelid), '') AS predicate,
         array_agg(i.relname ORDER BY i.relname) AS index_names,
         count(*) AS duplicate_count
  FROM pg_index ix
  JOIN pg_class i ON i.oid = ix.indexrelid
  JOIN pg_class t ON t.oid = ix.indrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  JOIN pg_am am ON am.oid = i.relam
  WHERE n.nspname = current_schema() AND ix.indisvalid
  GROUP BY ix.indrelid, t.relname, am.amname, ix.indisunique,
           ix.indisprimary, ix.indisexclusion, ix.indkey::text,
           ix.indclass::text, ix.indcollation::text, ix.indoption::text,
           coalesce(pg_get_expr(ix.indexprs, ix.indrelid), ''),
           coalesce(pg_get_expr(ix.indpred, ix.indrelid), '')
)
SELECT table_name, duplicate_count, index_names, predicate
FROM signatures
WHERE duplicate_count > 1
ORDER BY table_name, index_names::text;
ROLLBACK;

-- Migration e invariantes del ledger real.
BEGIN READ ONLY;
SELECT version, description, type, installed_on, execution_time, success
FROM futuros_operaciones.flyway_schema_history
WHERE version IN ('202607090001', '202607100002')
ORDER BY installed_rank;

SELECT t.relname AS table_name, c.conname, c.convalidated,
       pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = current_schema()
  AND t.relname = 'copy_dispatch_intent'
ORDER BY c.conname;

SELECT count(*) FILTER (WHERE status = 'PERSISTED' AND
           (binance_order_id IS NULL OR copy_operation_id IS NULL OR copy_operation_event_id IS NULL))
           AS persisted_missing_links,
       count(*) FILTER (WHERE status IN (
           'RECONCILING', 'PERSISTENCE_PENDING', 'NEW', 'PARTIALLY_FILLED')
           AND reservation_status <> 'PENDING') AS ambiguous_without_reservation,
       count(*) FILTER (WHERE status = 'MANUAL_REVIEW'
           AND reservation_status = 'RELEASED') AS manual_review_released
FROM copy_dispatch_intent;

SELECT count(*) AS persisted_without_operation
FROM copy_dispatch_intent i
LEFT JOIN copy_operation o ON o.id_operation = i.copy_operation_id
WHERE i.status = 'PERSISTED' AND o.id_operation IS NULL;

SELECT count(*) AS persisted_without_event
FROM copy_dispatch_intent i
LEFT JOIN copy_operation_event e ON e.id_event = i.copy_operation_event_id
WHERE i.status = 'PERSISTED' AND e.id_event IS NULL;
ROLLBACK;

-- Planes preliminares. El claim con row lock se prueba fuera de QA.
BEGIN READ ONLY;
EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT id FROM copy_dispatch_intent
WHERE idempotency_key = 'audit-synthetic-miss';

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
WITH active AS (
  SELECT coalesce(sum(size_usd / nullif(leverage, 0)), 0) AS used_margin,
         count(*) AS open_positions
  FROM copy_operation
  WHERE id_user = 'audit-synthetic-user' AND user_copy_allocation_id = -1
    AND execution_mode = 'MICRO_LIVE' AND is_active = true
    AND coalesce(is_shadow, false) = false
), pending AS (
  SELECT coalesce(sum(requested_margin_usd), 0) AS reserved_margin,
         coalesce(sum(reserved_position_count), 0) AS reserved_positions
  FROM copy_dispatch_intent
  WHERE id_user = 'audit-synthetic-user' AND user_copy_allocation_id = -1
    AND execution_mode = 'MICRO_LIVE' AND reservation_status = 'PENDING'
)
SELECT * FROM active CROSS JOIN pending;

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT * FROM user_copy_allocation
WHERE ends_at IS NULL AND is_active = true AND lower(status) = 'active'
  AND coalesce(execution_mode, 'LIVE') IN ('LIVE', 'MICRO_LIVE');

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT id
FROM copy_dispatch_intent
WHERE (status = 'DISPATCHING'
       AND updated_at < clock_timestamp() - interval '30 seconds')
   OR (status IN (
         'RECONCILING', 'PERSISTENCE_PENDING', 'NEW', 'PARTIALLY_FILLED',
         'FILLED', 'ACKNOWLEDGED', 'PERSISTED')
       AND (next_reconciliation_at IS NULL OR next_reconciliation_at <= clock_timestamp())
       AND (status IN ('NEW', 'PARTIALLY_FILLED')
            OR copy_operation_id IS NULL
            OR average_price_status = 'PENDING_RESOLUTION'))
ORDER BY coalesce(next_reconciliation_at, updated_at), created_at, id
LIMIT 50;

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT EXISTS (
  SELECT 1 FROM operation_movement_event_dedupe
  WHERE movement_key = 'audit-synthetic-miss'
);

EXPLAIN (VERBOSE, SETTINGS, FORMAT TEXT)
SELECT * FROM operation_movement_event
WHERE position_key = 'audit-synthetic-miss'
ORDER BY event_time DESC, date_creation DESC
LIMIT 1;
ROLLBACK;
