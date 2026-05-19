-- Canonical idempotency and partitioning for operation_movement_event.
-- This migration is intentionally NOT using CREATE INDEX CONCURRENTLY so it can run under Flyway transactions.
-- If V20260519 was already applied as a regular table, this migrates data into a partitioned table.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION futuros_operaciones.fn_operation_movement_decimal_key(p_value numeric)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_value IS NULL THEN 'NA'
        ELSE COALESCE(NULLIF(regexp_replace(regexp_replace(p_value::text, '(\.\d*?)0+$', '\1'), '\.$', ''), ''), '0')
    END;
$$;

CREATE OR REPLACE FUNCTION futuros_operaciones.fn_operation_movement_safe_part(p_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_value IS NULL OR btrim(p_value) = '' THEN 'NA'
        ELSE replace(replace(replace(replace(btrim(p_value), '|', '/'), chr(10), ' '), chr(13), ' '), chr(9), ' ')
    END;
$$;

CREATE OR REPLACE FUNCTION futuros_operaciones.fn_operation_movement_time_key(p_value timestamptz)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_value IS NULL THEN 'NA'
        ELSE to_char(p_value AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    END;
$$;

CREATE OR REPLACE FUNCTION futuros_operaciones.fn_operation_movement_canonical_key(
    p_source text,
    p_id_order_origin uuid,
    p_position_key text,
    p_wallet text,
    p_symbol text,
    p_side text,
    p_delta_type text,
    p_source_event_type text,
    p_status text,
    p_event_time timestamptz,
    p_source_ts_ms bigint,
    p_size_qty numeric,
    p_notional_usd numeric,
    p_margin_used_usd numeric,
    p_entry_price numeric,
    p_mark_or_exit_price numeric,
    p_external_id text DEFAULT NULL,
    p_raw_reference text DEFAULT NULL
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    WITH payload AS (
        SELECT concat_ws('|',
            'movement_v2',
            'origin=' || futuros_operaciones.fn_operation_movement_safe_part(p_id_order_origin::text),
            'position=' || futuros_operaciones.fn_operation_movement_safe_part(p_position_key),
            'wallet=' || futuros_operaciones.fn_operation_movement_safe_part(lower(COALESCE(NULLIF(p_wallet, ''), 'na'))),
            'symbol=' || futuros_operaciones.fn_operation_movement_safe_part(upper(COALESCE(NULLIF(p_symbol, ''), 'UNKNOWN'))),
            'side=' || futuros_operaciones.fn_operation_movement_safe_part(upper(COALESCE(NULLIF(p_side, ''), 'UNKNOWN'))),
            'delta=' || futuros_operaciones.fn_operation_movement_safe_part(upper(COALESCE(NULLIF(p_delta_type, ''), 'UNKNOWN'))),
            'sourceEvent=' || futuros_operaciones.fn_operation_movement_safe_part(upper(COALESCE(NULLIF(p_source_event_type, ''), 'UNKNOWN'))),
            'status=' || futuros_operaciones.fn_operation_movement_safe_part(upper(COALESCE(NULLIF(p_status, ''), 'UNKNOWN'))),
            'eventTime=' || futuros_operaciones.fn_operation_movement_time_key(p_event_time),
            'sizeQty=' || futuros_operaciones.fn_operation_movement_decimal_key(p_size_qty),
            'notionalUsd=' || futuros_operaciones.fn_operation_movement_decimal_key(p_notional_usd),
            'marginUsedUsd=' || futuros_operaciones.fn_operation_movement_decimal_key(p_margin_used_usd),
            'tradePrice=' || futuros_operaciones.fn_operation_movement_decimal_key(COALESCE(p_mark_or_exit_price, p_entry_price))
        ) AS value
    )
    SELECT 'movement|sha256:' || encode(digest(value, 'sha256'), 'hex')
    FROM payload;
$$;

DO $$
DECLARE
    v_relkind char;
BEGIN
    SELECT c.relkind
    INTO v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'futuros_operaciones'
      AND c.relname = 'operation_movement_event';

    IF v_relkind IS NOT NULL AND v_relkind <> 'p' THEN
        IF to_regclass('futuros_operaciones.operation_movement_event_legacy_20260520') IS NOT NULL THEN
            RAISE EXCEPTION 'operation_movement_event_legacy_20260520 already exists; review before rerunning migration';
        END IF;

        ALTER TABLE futuros_operaciones.operation_movement_event RENAME TO operation_movement_event_legacy_20260520;
        ALTER TABLE futuros_operaciones.operation_movement_event_legacy_20260520 DROP CONSTRAINT IF EXISTS operation_movement_event_pkey;
        DROP INDEX IF EXISTS futuros_operaciones.ux_operation_movement_event_movement_key;
        DROP INDEX IF EXISTS futuros_operaciones.ix_operation_movement_event_origin_time;
        DROP INDEX IF EXISTS futuros_operaciones.ix_operation_movement_event_wallet_symbol_side_time;
        DROP INDEX IF EXISTS futuros_operaciones.ix_operation_movement_event_position_time;
        DROP INDEX IF EXISTS futuros_operaciones.ix_operation_movement_event_trace_id;
        DROP INDEX IF EXISTS futuros_operaciones.ix_operation_movement_event_type_time;
        DROP INDEX IF EXISTS futuros_operaciones.ix_operation_movement_event_reason_time;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS futuros_operaciones.operation_movement_event (
    id_event uuid NOT NULL DEFAULT gen_random_uuid(),
    id_order_origin uuid NOT NULL,
    movement_key varchar(600) NOT NULL,
    idempotency_key varchar(600) NULL,
    position_key varchar(300) NOT NULL,
    id_wallet_origin varchar(180) NOT NULL,
    parsymbol varchar(40) NOT NULL,
    type_operation varchar(20) NOT NULL,
    event_type varchar(30) NOT NULL,
    delta_type varchar(30) NOT NULL,
    source_event_type varchar(30) NULL,
    status varchar(30) NULL,
    size_qty numeric(38,18) NULL,
    signed_size_qty numeric(38,18) NULL,
    previous_size_qty numeric(38,18) NULL,
    resulting_size_qty numeric(38,18) NULL,
    delta_size_qty numeric(38,18) NULL,
    notional_usd numeric(38,18) NULL,
    margin_used_usd numeric(38,18) NULL,
    entry_price numeric(38,18) NULL,
    mark_price numeric(38,18) NULL,
    exit_price numeric(38,18) NULL,
    realized_pnl_usd numeric(38,18) NULL,
    leverage numeric(38,18) NULL,
    wallet_version bigint NULL,
    snapshot_version bigint NULL,
    source_ts timestamptz NULL,
    detected_at timestamptz NULL,
    published_at timestamptz NULL,
    event_time timestamptz NOT NULL,
    trace_id varchar(128) NULL,
    source varchar(80) NULL,
    reason_code varchar(120) NULL,
    copy_eligible_users integer NULL,
    copy_submitted_tasks integer NULL,
    copy_business_skipped integer NULL,
    copy_fallback_jobs integer NULL,
    copy_fallback_used boolean NULL,
    raw jsonb NULL,
    date_creation timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT operation_movement_event_pkey PRIMARY KEY (id_event, event_time),
    CONSTRAINT chk_operation_movement_event_type CHECK (event_type IN ('OPEN', 'INCREASE', 'REDUCE', 'CLOSE', 'FLIP', 'UPDATE', 'NO_CHANGE', 'UNKNOWN')),
    CONSTRAINT chk_operation_movement_qty_non_negative CHECK (
        (size_qty IS NULL OR size_qty >= 0) AND
        (previous_size_qty IS NULL OR previous_size_qty >= 0) AND
        (resulting_size_qty IS NULL OR resulting_size_qty >= 0)
    )
) PARTITION BY RANGE (event_time);

CREATE TABLE IF NOT EXISTS futuros_operaciones.operation_movement_event_default
PARTITION OF futuros_operaciones.operation_movement_event DEFAULT;

CREATE OR REPLACE FUNCTION futuros_operaciones.ensure_operation_movement_event_partition(p_month date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_start date := date_trunc('month', p_month)::date;
    v_end date := (date_trunc('month', p_month)::date + interval '1 month')::date;
    v_partition text := 'operation_movement_event_' || to_char(date_trunc('month', p_month), 'YYYY_MM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS futuros_operaciones.%I PARTITION OF futuros_operaciones.operation_movement_event FOR VALUES FROM (%L) TO (%L)',
        v_partition,
        v_start::timestamptz,
        v_end::timestamptz
    );
END;
$$;

SELECT futuros_operaciones.ensure_operation_movement_event_partition(gs::date)
FROM generate_series(
    date_trunc('month', CURRENT_DATE)::date - interval '2 months',
    date_trunc('month', CURRENT_DATE)::date + interval '6 months',
    interval '1 month'
) AS gs;

CREATE TABLE IF NOT EXISTS futuros_operaciones.operation_movement_event_dedupe (
    movement_key varchar(600) NOT NULL,
    first_id_event uuid NOT NULL,
    first_event_time timestamptz NOT NULL,
    first_idempotency_key varchar(600) NULL,
    date_creation timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_operation_movement_event_movement_key
ON futuros_operaciones.operation_movement_event_dedupe (movement_key);

DO $$
BEGIN
    IF to_regclass('futuros_operaciones.operation_movement_event_legacy_20260520') IS NOT NULL THEN
        INSERT INTO futuros_operaciones.operation_movement_event (
            id_event,
            id_order_origin,
            movement_key,
            idempotency_key,
            position_key,
            id_wallet_origin,
            parsymbol,
            type_operation,
            event_type,
            delta_type,
            source_event_type,
            status,
            size_qty,
            signed_size_qty,
            previous_size_qty,
            resulting_size_qty,
            delta_size_qty,
            notional_usd,
            margin_used_usd,
            entry_price,
            mark_price,
            exit_price,
            realized_pnl_usd,
            leverage,
            wallet_version,
            snapshot_version,
            source_ts,
            detected_at,
            published_at,
            event_time,
            trace_id,
            source,
            reason_code,
            copy_eligible_users,
            copy_submitted_tasks,
            copy_business_skipped,
            copy_fallback_jobs,
            copy_fallback_used,
            raw,
            date_creation
        )
        SELECT DISTINCT ON (
            futuros_operaciones.fn_operation_movement_canonical_key(
                COALESCE(NULLIF(l.source, ''), 'operation_event'),
                l.id_order_origin,
                l.position_key,
                l.id_wallet_origin,
                l.parsymbol,
                l.type_operation,
                l.delta_type,
                l.source_event_type,
                l.status,
                l.event_time,
                CASE WHEN l.source_ts IS NULL THEN NULL ELSE (extract(epoch from l.source_ts) * 1000)::bigint END,
                COALESCE(l.size_qty, l.signed_size_qty),
                l.notional_usd,
                l.margin_used_usd,
                l.entry_price,
                COALESCE(l.exit_price, l.entry_price),
                l.raw #>> '{request,externalId}',
                l.raw #>> '{request,rawReference}'
            ),
            l.event_time
        )
            l.id_event,
            l.id_order_origin,
            futuros_operaciones.fn_operation_movement_canonical_key(
                COALESCE(NULLIF(l.source, ''), 'operation_event'),
                l.id_order_origin,
                l.position_key,
                l.id_wallet_origin,
                l.parsymbol,
                l.type_operation,
                l.delta_type,
                l.source_event_type,
                l.status,
                l.event_time,
                CASE WHEN l.source_ts IS NULL THEN NULL ELSE (extract(epoch from l.source_ts) * 1000)::bigint END,
                COALESCE(l.size_qty, l.signed_size_qty),
                l.notional_usd,
                l.margin_used_usd,
                l.entry_price,
                COALESCE(l.exit_price, l.entry_price),
                l.raw #>> '{request,externalId}',
                l.raw #>> '{request,rawReference}'
            ) AS movement_key,
            l.idempotency_key,
            l.position_key,
            l.id_wallet_origin,
            upper(l.parsymbol),
            upper(l.type_operation),
            l.event_type,
            l.delta_type,
            l.source_event_type,
            l.status,
            l.size_qty,
            l.signed_size_qty,
            l.previous_size_qty,
            l.resulting_size_qty,
            l.delta_size_qty,
            l.notional_usd,
            l.margin_used_usd,
            l.entry_price,
            l.mark_price,
            l.exit_price,
            l.realized_pnl_usd,
            l.leverage,
            l.wallet_version,
            l.snapshot_version,
            l.source_ts,
            l.detected_at,
            l.published_at,
            l.event_time,
            l.trace_id,
            l.source,
            l.reason_code,
            l.copy_eligible_users,
            l.copy_submitted_tasks,
            l.copy_business_skipped,
            l.copy_fallback_jobs,
            l.copy_fallback_used,
            l.raw,
            l.date_creation
        FROM futuros_operaciones.operation_movement_event_legacy_20260520 l
        ORDER BY 3, l.event_time, l.date_creation, l.id_event;
    END IF;
END $$;

INSERT INTO futuros_operaciones.operation_movement_event_dedupe (
    movement_key,
    first_id_event,
    first_event_time,
    first_idempotency_key,
    date_creation
)
SELECT DISTINCT ON (movement_key)
    movement_key,
    id_event,
    event_time,
    idempotency_key,
    date_creation
FROM futuros_operaciones.operation_movement_event
ORDER BY movement_key, event_time, date_creation, id_event
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION futuros_operaciones.operation_movement_event_dedupe_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO futuros_operaciones.operation_movement_event_dedupe (
        movement_key,
        first_id_event,
        first_event_time,
        first_idempotency_key
    ) VALUES (
        NEW.movement_key,
        NEW.id_event,
        NEW.event_time,
        NEW.idempotency_key
    );
    RETURN NEW;
EXCEPTION WHEN unique_violation THEN
    RAISE EXCEPTION USING
        ERRCODE = '23505',
        CONSTRAINT = 'ux_operation_movement_event_movement_key',
        MESSAGE = 'duplicate movement_key in operation_movement_event: ' || COALESCE(NEW.movement_key, 'NULL');
END;
$$;

DROP TRIGGER IF EXISTS trg_operation_movement_event_dedupe_guard ON futuros_operaciones.operation_movement_event;
CREATE TRIGGER trg_operation_movement_event_dedupe_guard
BEFORE INSERT ON futuros_operaciones.operation_movement_event
FOR EACH ROW
EXECUTE FUNCTION futuros_operaciones.operation_movement_event_dedupe_guard();

CREATE UNIQUE INDEX IF NOT EXISTS ux_operation_movement_event_movement_key_event_time
ON futuros_operaciones.operation_movement_event (movement_key, event_time);

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_id_event
ON futuros_operaciones.operation_movement_event (id_event);

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_origin_time
ON futuros_operaciones.operation_movement_event (id_order_origin, event_time DESC);

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_wallet_symbol_side_time
ON futuros_operaciones.operation_movement_event (id_wallet_origin, parsymbol, type_operation, event_time DESC);

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_position_time
ON futuros_operaciones.operation_movement_event (position_key, event_time DESC, date_creation DESC);

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_trace_id
ON futuros_operaciones.operation_movement_event (trace_id)
WHERE trace_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_type_time
ON futuros_operaciones.operation_movement_event (event_type, delta_type, event_time DESC);

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_reason_time
ON futuros_operaciones.operation_movement_event (reason_code, event_time DESC)
WHERE reason_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_time_brin
ON futuros_operaciones.operation_movement_event USING brin (event_time)
WITH (pages_per_range = 64);

COMMENT ON TABLE futuros_operaciones.operation_movement_event IS 'Partitioned ledger of original operation movements. Complements copy_operation_event and preserves movements that were skipped or not copied.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.movement_key IS 'Canonical deterministic hash of the original movement payload. It must not depend on the external idempotency_key.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.idempotency_key IS 'External publisher/exchange idempotency key kept only for audit and troubleshooting, not used as the canonical ledger key.';
COMMENT ON TABLE futuros_operaciones.operation_movement_event_dedupe IS 'Global dedupe guard for operation_movement_event. Required because PostgreSQL partitioned unique indexes must include event_time.';

CREATE OR REPLACE VIEW futuros_operaciones.operation_movement_event_semantic_duplicates_v AS
WITH grouped AS (
    SELECT
        COALESCE(source, 'operation_event') AS source,
        position_key,
        id_wallet_origin,
        parsymbol,
        type_operation,
        delta_type,
        source_event_type,
        status,
        event_time,
        size_qty,
        signed_size_qty,
        notional_usd,
        margin_used_usd,
        entry_price,
        exit_price,
        count(*) AS rows_count,
        count(DISTINCT movement_key) AS movement_keys_count,
        count(DISTINCT idempotency_key) FILTER (WHERE idempotency_key IS NOT NULL) AS idempotency_keys_count,
        min(date_creation) AS first_seen_at,
        max(date_creation) AS last_seen_at,
        (array_agg(id_event ORDER BY date_creation, id_event))[1:20] AS sample_event_ids,
        (array_agg(movement_key ORDER BY date_creation, id_event))[1:20] AS sample_movement_keys,
        (array_agg(idempotency_key ORDER BY date_creation, id_event) FILTER (WHERE idempotency_key IS NOT NULL))[1:20] AS sample_idempotency_keys
    FROM futuros_operaciones.operation_movement_event
    GROUP BY
        COALESCE(source, 'operation_event'),
        position_key,
        id_wallet_origin,
        parsymbol,
        type_operation,
        delta_type,
        source_event_type,
        status,
        event_time,
        size_qty,
        signed_size_qty,
        notional_usd,
        margin_used_usd,
        entry_price,
        exit_price
)
SELECT *
FROM grouped
WHERE rows_count > 1
   OR movement_keys_count > 1;

CREATE OR REPLACE VIEW futuros_operaciones.operation_movement_event_partition_health_v AS
SELECT
    n.nspname AS schema_name,
    c.relname AS partition_name,
    pg_get_expr(c.relpartbound, c.oid) AS partition_bound,
    pg_total_relation_size(c.oid) AS total_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_inherits i ON i.inhrelid = c.oid
JOIN pg_class p ON p.oid = i.inhparent
JOIN pg_namespace pn ON pn.oid = p.relnamespace
WHERE pn.nspname = 'futuros_operaciones'
  AND p.relname = 'operation_movement_event'
ORDER BY c.relname;

CREATE OR REPLACE VIEW futuros_operaciones.operation_movement_event_dedupe_drift_v AS
SELECT
    'missing_in_guard' AS issue_type,
    e.movement_key,
    e.id_event,
    e.event_time,
    e.idempotency_key
FROM futuros_operaciones.operation_movement_event e
LEFT JOIN futuros_operaciones.operation_movement_event_dedupe d ON d.movement_key = e.movement_key
WHERE d.movement_key IS NULL
UNION ALL
SELECT
    'guard_without_event' AS issue_type,
    d.movement_key,
    d.first_id_event AS id_event,
    d.first_event_time AS event_time,
    d.first_idempotency_key AS idempotency_key
FROM futuros_operaciones.operation_movement_event_dedupe d
LEFT JOIN futuros_operaciones.operation_movement_event e ON e.movement_key = d.movement_key
WHERE e.movement_key IS NULL;

CREATE OR REPLACE VIEW futuros_operaciones.operation_copy_pnl_daily_v AS
SELECT
    day,
    id_wallet_origin,
    parsymbol,
    type_operation,
    sum(origin_realized_pnl_usd) AS origin_realized_pnl_usd,
    sum(copy_realized_pnl_usd) AS copy_realized_pnl_usd,
    sum(copy_fee_usd) AS copy_fee_usd,
    sum(copy_realized_pnl_usd) - sum(copy_fee_usd) AS copy_net_pnl_usd
FROM (
    SELECT
        date_trunc('day', ome.event_time) AS day,
        ome.id_wallet_origin,
        ome.parsymbol,
        ome.type_operation,
        COALESCE(ome.realized_pnl_usd, 0) AS origin_realized_pnl_usd,
        0::numeric AS copy_realized_pnl_usd,
        0::numeric AS copy_fee_usd
    FROM futuros_operaciones.operation_movement_event ome

    UNION ALL

    SELECT
        date_trunc('day', coe.event_time) AS day,
        coe.id_wallet_origin,
        coe.parsymbol,
        coe.type_operation,
        0::numeric AS origin_realized_pnl_usd,
        COALESCE(coe.realized_pnl_usd, 0) AS copy_realized_pnl_usd,
        COALESCE(coe.fee_usd, 0) AS copy_fee_usd
    FROM futuros_operaciones.copy_operation_event coe
) x
GROUP BY day, id_wallet_origin, parsymbol, type_operation;

ANALYZE futuros_operaciones.operation_movement_event;
ANALYZE futuros_operaciones.operation_movement_event_dedupe;
