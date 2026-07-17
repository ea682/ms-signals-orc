-- Canonical Wallet Metric V2 identity and durable, generation-aware Signals snapshot cache.
-- This migration is additive and preserves every V1/V2 business row.

SET lock_timeout = '60s';
SET statement_timeout = '15min';

-- Take the final DDL lock mode before the backfill starts. A weaker EXCLUSIVE
-- lock still requires an ACCESS EXCLUSIVE upgrade at ALTER TABLE time; a queued
-- writer can then deadlock with that upgrade even though its lock is not yet
-- granted. Holding the final mode up front removes the conversion entirely.
LOCK TABLE
    futuros_operaciones.shadow_copy_allocation,
    futuros_operaciones.copy_wallet_profile,
    futuros_operaciones.shadow_copy_operation,
    futuros_operaciones.shadow_copy_operation_event,
    futuros_operaciones.shadow_position_state,
    futuros_operaciones.user_copy_allocation
IN ACCESS EXCLUSIVE MODE;

CREATE OR REPLACE FUNCTION pg_temp.metric_strategy_code(raw_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $function$
    SELECT COALESCE(NULLIF(upper(replace(btrim(raw_value), '-', '_')), ''), 'MOVEMENT_ALL')
$function$;

CREATE OR REPLACE FUNCTION pg_temp.metric_scope_type(raw_value text, raw_strategy text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $function$
    SELECT CASE
        WHEN NULLIF(upper(btrim(raw_value)), '') IS NOT NULL
             AND upper(btrim(raw_value)) NOT IN ('STRATEGY', 'DEFAULT')
            THEN upper(btrim(raw_value))
        WHEN pg_temp.metric_strategy_code(raw_strategy) IN ('LONG_ONLY', 'SHORT_ONLY') THEN 'DIRECTION'
        WHEN pg_temp.metric_strategy_code(raw_strategy) = 'SYMBOL_SPECIALIST' THEN 'SYMBOL'
        WHEN pg_temp.metric_strategy_code(raw_strategy) = 'LOW_LEVERAGE_ONLY' THEN 'LEVERAGE_RANGE'
        ELSE 'ALL'
    END
$function$;

CREATE OR REPLACE FUNCTION pg_temp.metric_scope_value(raw_value text, raw_strategy text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $function$
    SELECT CASE
        WHEN NULLIF(upper(btrim(raw_value)), '') IS NULL
             OR upper(btrim(raw_value)) IN ('STRATEGY', 'DEFAULT')
             OR upper(replace(btrim(raw_value), '-', '_')) = pg_temp.metric_strategy_code(raw_strategy)
            THEN CASE pg_temp.metric_strategy_code(raw_strategy)
                WHEN 'LONG_ONLY' THEN 'LONG'
                WHEN 'SHORT_ONLY' THEN 'SHORT'
                ELSE 'ALL'
            END
        ELSE upper(btrim(raw_value))
    END
$function$;

-- Stop safely if legacy spellings would collapse two active allocations into one identity.
DO $block$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM futuros_operaciones.user_copy_allocation u
        WHERE u.ends_at IS NULL
        GROUP BY u.id_user,
                 lower(btrim(u.wallet_id)),
                 pg_temp.metric_strategy_code(u.copy_strategy_code),
                 pg_temp.metric_scope_type(u.scope_type, u.copy_strategy_code),
                 pg_temp.metric_scope_value(u.scope_value, u.copy_strategy_code)
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'METRIC_V2_CANONICAL_USER_ALLOCATION_COLLISION';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM futuros_operaciones.shadow_copy_allocation s
        WHERE s.ends_at IS NULL AND s.is_active = true
        GROUP BY s.id_user,
                 lower(btrim(s.wallet_id)),
                 pg_temp.metric_strategy_code(s.copy_strategy_code),
                 pg_temp.metric_scope_type(s.scope_type, s.copy_strategy_code),
                 pg_temp.metric_scope_value(s.scope_value, s.copy_strategy_code),
                 s.shadow_version
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'METRIC_V2_CANONICAL_SHADOW_ALLOCATION_COLLISION';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM futuros_operaciones.copy_wallet_profile p
        GROUP BY lower(btrim(p.wallet_id)),
                 pg_temp.metric_strategy_code(p.copy_profile_code),
                 pg_temp.metric_scope_type(p.scope_type, p.copy_profile_code),
                 pg_temp.metric_scope_value(p.scope_value, p.copy_profile_code)
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'METRIC_V2_CANONICAL_WALLET_PROFILE_COLLISION';
    END IF;
END
$block$;

UPDATE futuros_operaciones.user_copy_allocation
SET wallet_id = lower(btrim(wallet_id)),
    copy_strategy_code = pg_temp.metric_strategy_code(copy_strategy_code),
    scope_type = pg_temp.metric_scope_type(scope_type, copy_strategy_code),
    scope_value = pg_temp.metric_scope_value(scope_value, copy_strategy_code),
    strategy_key = lower(btrim(wallet_id)) || '|'
        || pg_temp.metric_strategy_code(copy_strategy_code) || '|'
        || pg_temp.metric_scope_type(scope_type, copy_strategy_code) || '|'
        || pg_temp.metric_scope_value(scope_value, copy_strategy_code);

UPDATE futuros_operaciones.shadow_copy_allocation
SET wallet_id = lower(btrim(wallet_id)),
    copy_strategy_code = pg_temp.metric_strategy_code(copy_strategy_code),
    scope_type = pg_temp.metric_scope_type(scope_type, copy_strategy_code),
    scope_value = pg_temp.metric_scope_value(scope_value, copy_strategy_code),
    strategy_key = lower(btrim(wallet_id)) || '|'
        || pg_temp.metric_strategy_code(copy_strategy_code) || '|'
        || pg_temp.metric_scope_type(scope_type, copy_strategy_code) || '|'
        || pg_temp.metric_scope_value(scope_value, copy_strategy_code);

UPDATE futuros_operaciones.copy_wallet_profile
SET wallet_id = lower(btrim(wallet_id)),
    copy_profile_code = pg_temp.metric_strategy_code(copy_profile_code),
    scope_type = pg_temp.metric_scope_type(scope_type, copy_profile_code),
    scope_value = pg_temp.metric_scope_value(scope_value, copy_profile_code),
    profile_key = lower(btrim(wallet_id)) || '|'
        || pg_temp.metric_strategy_code(copy_profile_code) || '|'
        || pg_temp.metric_scope_type(scope_type, copy_profile_code) || '|'
        || pg_temp.metric_scope_value(scope_value, copy_profile_code);

UPDATE futuros_operaciones.shadow_copy_operation
SET id_wallet_origin = lower(btrim(id_wallet_origin)),
    copy_strategy_code = pg_temp.metric_strategy_code(copy_strategy_code),
    scope_type = pg_temp.metric_scope_type(scope_type, copy_strategy_code),
    scope_value = pg_temp.metric_scope_value(scope_value, copy_strategy_code),
    strategy_key = lower(btrim(id_wallet_origin)) || '|'
        || pg_temp.metric_strategy_code(copy_strategy_code) || '|'
        || pg_temp.metric_scope_type(scope_type, copy_strategy_code) || '|'
        || pg_temp.metric_scope_value(scope_value, copy_strategy_code);

UPDATE futuros_operaciones.shadow_copy_operation_event
SET id_wallet_origin = lower(btrim(id_wallet_origin)),
    copy_strategy_code = pg_temp.metric_strategy_code(copy_strategy_code),
    scope_type = pg_temp.metric_scope_type(scope_type, copy_strategy_code),
    scope_value = pg_temp.metric_scope_value(scope_value, copy_strategy_code),
    strategy_key = lower(btrim(id_wallet_origin)) || '|'
        || pg_temp.metric_strategy_code(copy_strategy_code) || '|'
        || pg_temp.metric_scope_type(scope_type, copy_strategy_code) || '|'
        || pg_temp.metric_scope_value(scope_value, copy_strategy_code);

UPDATE futuros_operaciones.shadow_position_state
SET wallet_id = lower(btrim(wallet_id)),
    copy_strategy_code = pg_temp.metric_strategy_code(copy_strategy_code),
    scope_type = pg_temp.metric_scope_type(scope_type, copy_strategy_code),
    scope_value = pg_temp.metric_scope_value(scope_value, copy_strategy_code),
    strategy_key = lower(btrim(wallet_id)) || '|'
        || pg_temp.metric_strategy_code(copy_strategy_code) || '|'
        || pg_temp.metric_scope_type(scope_type, copy_strategy_code) || '|'
        || pg_temp.metric_scope_value(scope_value, copy_strategy_code);

ALTER TABLE futuros_operaciones.user_copy_allocation
    ALTER COLUMN scope_type SET DEFAULT 'ALL',
    ALTER COLUMN scope_value SET DEFAULT 'ALL',
    ALTER COLUMN strategy_key SET NOT NULL;

ALTER TABLE futuros_operaciones.shadow_copy_allocation
    ALTER COLUMN scope_type SET DEFAULT 'ALL',
    ALTER COLUMN scope_value SET DEFAULT 'ALL';

ALTER TABLE futuros_operaciones.copy_wallet_profile
    ALTER COLUMN scope_type SET DEFAULT 'ALL',
    ALTER COLUMN scope_value SET DEFAULT 'ALL';

ALTER TABLE futuros_operaciones.shadow_copy_operation
    ALTER COLUMN scope_type SET DEFAULT 'ALL',
    ALTER COLUMN scope_value SET DEFAULT 'ALL';

ALTER TABLE futuros_operaciones.shadow_copy_operation_event
    ALTER COLUMN scope_type SET DEFAULT 'ALL',
    ALTER COLUMN scope_value SET DEFAULT 'ALL';

ALTER TABLE futuros_operaciones.shadow_position_state
    ALTER COLUMN scope_type SET DEFAULT 'ALL',
    ALTER COLUMN scope_value SET DEFAULT 'ALL';

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_strategy_key_active
    ON futuros_operaciones.user_copy_allocation (strategy_key, id_user)
    WHERE ends_at IS NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_copy_allocation_strategy_key_active
    ON futuros_operaciones.shadow_copy_allocation (strategy_key, id_user)
    WHERE ends_at IS NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_copy_operation_strategy_key_active
    ON futuros_operaciones.shadow_copy_operation (strategy_key, parsymbol, type_operation)
    WHERE is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_position_strategy_key_active
    ON futuros_operaciones.shadow_position_state (strategy_key, parsymbol, position_side)
    WHERE status = 'OPEN';

CREATE TABLE IF NOT EXISTS futuros_operaciones.metric_strategy_snapshot_v2 (
    strategy_key varchar(420) NOT NULL,
    snapshot_type varchar(16) NOT NULL,
    generation_id varchar(80) NOT NULL,
    metric_version integer NOT NULL,
    source_version varchar(80) NOT NULL,
    wallet_id varchar(128) NOT NULL,
    strategy_code varchar(64) NOT NULL,
    scope_type varchar(32) NOT NULL,
    scope_value varchar(180) NOT NULL,
    computed_at timestamptz NOT NULL,
    data_as_of timestamptz NOT NULL,
    fetched_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    decision_final boolean NOT NULL,
    allow_new_entries boolean NOT NULL,
    reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
    payload jsonb NOT NULL,
    CONSTRAINT pk_metric_strategy_snapshot_v2 PRIMARY KEY (snapshot_type, strategy_key),
    CONSTRAINT chk_metric_strategy_snapshot_v2_type
        CHECK (snapshot_type IN ('SUMMARY', 'FULL', 'COPY_GUARD')),
    CONSTRAINT chk_metric_strategy_snapshot_v2_version CHECK (metric_version = 2),
    CONSTRAINT chk_metric_strategy_snapshot_v2_expiry CHECK (expires_at >= fetched_at)
);

CREATE INDEX IF NOT EXISTS ix_metric_strategy_snapshot_v2_generation
    ON futuros_operaciones.metric_strategy_snapshot_v2 (generation_id, snapshot_type);

CREATE INDEX IF NOT EXISTS ix_metric_strategy_snapshot_v2_expiry
    ON futuros_operaciones.metric_strategy_snapshot_v2 (expires_at);
