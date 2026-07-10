-- Read-only by default. This script never sends, cancels or closes Binance orders.
-- Input is a Binance order export obtained separately by an authorized operator.
-- Dry run:
--   psql "$DB_URL" -v binance_csv="'C:/secure/binance-orders.csv'" -f scripts/reconcile-copy-dispatch-incident.sql
-- Existing-intent repair only (no synthetic intents/copy operations are created):
--   psql "$DB_URL" -v binance_csv="'C:/secure/binance-orders.csv'" -v apply=1 \
--     -v confirm_apply="'I_UNDERSTAND_DB_BACKFILL_ONLY'" -f scripts/reconcile-copy-dispatch-incident.sql

\set ON_ERROR_STOP on
\if :{?apply}
\else
  \set apply 0
\endif
\if :{?confirm_apply}
\else
  \set confirm_apply 'NOT_CONFIRMED'
\endif

CREATE TEMP TABLE incident_binance_order (
    user_id varchar(80) NOT NULL,
    user_copy_allocation_id bigint,
    source_event_id varchar(600),
    client_order_id varchar(36) NOT NULL,
    symbol varchar(40) NOT NULL,
    order_id bigint NOT NULL,
    status varchar(32) NOT NULL,
    executed_qty numeric(38, 18),
    avg_price numeric(38, 18),
    cum_quote numeric(38, 18),
    leverage numeric(38, 12),
    order_time timestamptz,
    PRIMARY KEY (user_id, order_id)
) ON COMMIT PRESERVE ROWS;

\copy incident_binance_order FROM :binance_csv WITH (FORMAT csv, HEADER true)

\echo '=== DRY RUN: Binance orders with no local copy_operation ==='
SELECT b.user_id,
       b.user_copy_allocation_id,
       b.source_event_id,
       b.client_order_id,
       b.symbol,
       b.order_id,
       b.status,
       b.executed_qty,
       COALESCE(NULLIF(b.avg_price, 0),
                b.cum_quote / NULLIF(b.executed_qty, 0)) AS effective_price,
       COALESCE(b.cum_quote,
                b.executed_qty * NULLIF(b.avg_price, 0)) AS notional_usd,
       COALESCE(b.cum_quote,
                b.executed_qty * NULLIF(b.avg_price, 0)) / NULLIF(b.leverage, 0) AS estimated_margin_usd,
       b.order_time
FROM incident_binance_order b
LEFT JOIN futuros_operaciones.copy_operation o
  ON o.id_user = b.user_id
 AND (o.id_orden = b.order_id::text OR o.client_order_id = b.client_order_id)
WHERE o.id_operation IS NULL
ORDER BY b.user_id, b.user_copy_allocation_id, b.source_event_id, b.symbol, b.order_time;

\echo '=== DRY RUN: repeated orders for the same exact allocation/source intent ==='
SELECT user_id,
       user_copy_allocation_id,
       source_event_id,
       client_order_id,
       symbol,
       COUNT(*) AS order_count,
       ARRAY_AGG(order_id ORDER BY order_time, order_id) AS order_ids,
       SUM(COALESCE(executed_qty, 0)) AS total_executed_qty,
       SUM(COALESCE(cum_quote, executed_qty * NULLIF(avg_price, 0), 0)) AS total_notional_usd,
       SUM(COALESCE(cum_quote, executed_qty * NULLIF(avg_price, 0), 0)
           / NULLIF(leverage, 0)) AS estimated_margin_usd
FROM incident_binance_order
GROUP BY user_id, user_copy_allocation_id, source_event_id, client_order_id, symbol
HAVING COUNT(*) > 1
ORDER BY order_count DESC, user_id, user_copy_allocation_id;

\echo '=== DRY RUN: symbol summary, including BTCUSDC/ETHUSDC/SOLUSDC and all detected symbols ==='
SELECT symbol,
       COUNT(*) AS orders,
       COUNT(DISTINCT user_id) AS users,
       COUNT(DISTINCT user_copy_allocation_id) AS allocations,
       SUM(COALESCE(executed_qty, 0)) AS executed_qty,
       SUM(COALESCE(cum_quote, executed_qty * NULLIF(avg_price, 0), 0)) AS notional_usd,
       ARRAY_AGG(order_id ORDER BY order_time, order_id) AS order_ids
FROM incident_binance_order
GROUP BY symbol
ORDER BY symbol;

\echo '=== DRY RUN: existing intents that can be repaired from this export ==='
SELECT i.id AS dispatch_intent_id,
       i.id_user,
       i.user_copy_allocation_id,
       i.source_event_id,
       i.client_order_id,
       i.status AS local_status,
       b.order_id,
       b.status AS binance_status,
       b.executed_qty,
       b.avg_price,
       b.cum_quote
FROM futuros_operaciones.copy_dispatch_intent i
JOIN incident_binance_order b
  ON b.user_id = i.id_user
 AND b.client_order_id = i.client_order_id
 AND (b.user_copy_allocation_id IS NULL OR b.user_copy_allocation_id = i.user_copy_allocation_id)
WHERE i.copy_operation_id IS NULL
ORDER BY i.created_at, i.id;

\echo '=== DRY RUN: legacy orphan orders without a durable intent; manual mapping required ==='
SELECT b.*
FROM incident_binance_order b
LEFT JOIN futuros_operaciones.copy_dispatch_intent i
  ON i.id_user = b.user_id
 AND i.client_order_id = b.client_order_id
 AND (b.user_copy_allocation_id IS NULL OR b.user_copy_allocation_id = i.user_copy_allocation_id)
LEFT JOIN futuros_operaciones.copy_operation o
  ON o.id_user = b.user_id
 AND (o.id_orden = b.order_id::text OR o.client_order_id = b.client_order_id)
WHERE i.id IS NULL
  AND o.id_operation IS NULL
ORDER BY b.user_id, b.order_time, b.order_id;

SELECT :'apply' = '1' AS apply_requested \gset
\if :apply_requested
  SELECT :'confirm_apply' = 'I_UNDERSTAND_DB_BACKFILL_ONLY' AS apply_confirmed \gset
  \if :apply_confirmed
    BEGIN;

    -- This only enriches an already-existing durable intent and wakes the normal
    -- reconciliation worker. It deliberately does not create copy_operation rows.
    WITH safe_binance_order AS (
      SELECT b.*
      FROM incident_binance_order b
      JOIN (
        SELECT user_id,
               user_copy_allocation_id,
               source_event_id,
               client_order_id,
               symbol
        FROM incident_binance_order b
        WHERE b.user_copy_allocation_id IS NOT NULL
          AND b.source_event_id IS NOT NULL
        GROUP BY user_id,
                 user_copy_allocation_id,
                 source_event_id,
                 client_order_id,
                 symbol
        HAVING COUNT(*) = 1
      ) exact
        ON exact.user_id = b.user_id
       AND exact.user_copy_allocation_id = b.user_copy_allocation_id
       AND exact.source_event_id = b.source_event_id
       AND exact.client_order_id = b.client_order_id
       AND exact.symbol = b.symbol
      WHERE b.status IN ('NEW', 'PARTIALLY_FILLED', 'FILLED')
    )
    UPDATE futuros_operaciones.copy_dispatch_intent i
       SET binance_order_id = b.order_id,
           binance_status = b.status,
           executed_qty = b.executed_qty,
           average_price = NULLIF(b.avg_price, 0),
           cumulative_quote_qty = b.cum_quote,
           average_price_status = CASE
               WHEN COALESCE(NULLIF(b.avg_price, 0), b.cum_quote / NULLIF(b.executed_qty, 0)) IS NULL
                   THEN 'PENDING_RESOLUTION'
               ELSE 'AVAILABLE'
           END,
           status = CASE WHEN i.status = 'PERSISTED' THEN 'PERSISTED' ELSE 'RECONCILING' END,
           reservation_status = CASE WHEN i.status = 'PERSISTED' THEN 'CONFIRMED' ELSE 'PENDING' END,
           acknowledged_at = COALESCE(i.acknowledged_at, b.order_time, now()),
           filled_at = CASE WHEN b.status = 'FILLED' THEN COALESCE(i.filled_at, b.order_time, now()) ELSE i.filled_at END,
           next_reconciliation_at = CASE WHEN i.status = 'PERSISTED' THEN NULL ELSE now() END,
           last_error_code = 'INCIDENT_EXPORT_RECONCILIATION',
           last_error_detail = 'Binance export applied; worker must verify and persist without resending',
           updated_at = now()
      FROM safe_binance_order b
     WHERE i.id_user = b.user_id
       AND i.client_order_id = b.client_order_id
       AND i.user_copy_allocation_id = b.user_copy_allocation_id
       AND i.source_event_id = b.source_event_id
       AND i.symbol = b.symbol
       AND (i.binance_order_id IS NULL OR i.binance_order_id = b.order_id)
       AND 1 = (
         SELECT COUNT(*)
         FROM futuros_operaciones.copy_dispatch_intent exact_intent
         WHERE exact_intent.id_user = b.user_id
           AND exact_intent.user_copy_allocation_id = b.user_copy_allocation_id
           AND exact_intent.source_event_id = b.source_event_id
           AND exact_intent.client_order_id = b.client_order_id
           AND exact_intent.symbol = b.symbol
       );

    COMMIT;
    \echo 'APPLY completed for existing intents only. No Binance mutation and no synthetic operation was performed.'
  \else
    \echo 'APPLY refused: pass -v confirm_apply="I_UNDERSTAND_DB_BACKFILL_ONLY".'
    \quit 3
  \endif
\else
  \echo 'DRY RUN complete. No persistent database rows or Binance orders were modified.'
\endif

\echo '=== Post-check: FILLED/reconciliation intents without copy_operation ==='
SELECT i.id,
       i.id_user,
       i.user_copy_allocation_id,
       i.execution_mode,
       i.status,
       i.reservation_status,
       i.source_event_id,
       i.client_order_id,
       i.binance_order_id,
       i.executed_qty,
       i.average_price_status,
       i.reconciliation_attempts,
       i.next_reconciliation_at
FROM futuros_operaciones.copy_dispatch_intent i
LEFT JOIN futuros_operaciones.copy_operation o ON o.dispatch_intent_id = i.id
WHERE o.id_operation IS NULL
  AND i.status IN ('FILLED', 'PARTIALLY_FILLED', 'RECONCILING', 'PERSISTENCE_PENDING', 'FAILED_FINAL')
ORDER BY i.updated_at, i.id;
