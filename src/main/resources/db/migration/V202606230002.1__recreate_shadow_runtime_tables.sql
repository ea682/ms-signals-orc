-- Recreates and hardens the SHADOW runtime tables if they were manually dropped after Flyway
-- had already marked the older SHADOW migrations as applied.
--
-- This migration is intentionally idempotent. It creates the full current SHADOW schema used by
-- the entities in ms-signals-orc and reapplies the indexes required by wallet+strategy SHADOW,
-- async runtime, idempotency, and future LIVE validation.

CREATE SCHEMA IF NOT EXISTS futuros_operaciones;

-- -----------------------------------------------------------------------------
-- Global profile: wallet + copy_profile_code + scope_type + scope_value
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_wallet_profile (
    id bigserial PRIMARY KEY,
    wallet_id varchar(128) NOT NULL,
    copy_profile_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    copy_profile_category varchar(40) NOT NULL DEFAULT 'CORE_COPY_PROFILE',
    scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    scope_value varchar(160) NOT NULL DEFAULT 'MOVEMENT_ALL',
    profile_config_hash varchar(80),
    profile_key varchar(420) NOT NULL,
    status varchar(40) NOT NULL DEFAULT 'SHADOW_TESTING',
    historical_score numeric(18, 6),
    shadow_score numeric(18, 6),
    validated_ranking_score numeric(18, 6),
    copy_guard_status varchar(40),
    copy_guard_action varchar(40),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    wallet_last_activity_at timestamptz,
    wallet_last_opened_at timestamptz,
    wallet_last_closed_at timestamptz,
    strategy_last_activity_at timestamptz,
    strategy_last_opened_at timestamptz,
    strategy_last_closed_at timestamptz,
    last_validation_reason varchar(300),
    last_validation_reason_code varchar(120),
    cooldown_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE futuros_operaciones.copy_wallet_profile
    ADD COLUMN IF NOT EXISTS wallet_id varchar(128),
    ADD COLUMN IF NOT EXISTS copy_profile_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    ADD COLUMN IF NOT EXISTS copy_profile_category varchar(40) NOT NULL DEFAULT 'CORE_COPY_PROFILE',
    ADD COLUMN IF NOT EXISTS scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    ADD COLUMN IF NOT EXISTS scope_value varchar(160) NOT NULL DEFAULT 'MOVEMENT_ALL',
    ADD COLUMN IF NOT EXISTS profile_config_hash varchar(80),
    ADD COLUMN IF NOT EXISTS profile_key varchar(420),
    ADD COLUMN IF NOT EXISTS status varchar(40) NOT NULL DEFAULT 'SHADOW_TESTING',
    ADD COLUMN IF NOT EXISTS historical_score numeric(18, 6),
    ADD COLUMN IF NOT EXISTS shadow_score numeric(18, 6),
    ADD COLUMN IF NOT EXISTS validated_ranking_score numeric(18, 6),
    ADD COLUMN IF NOT EXISTS copy_guard_status varchar(40),
    ADD COLUMN IF NOT EXISTS copy_guard_action varchar(40),
    ADD COLUMN IF NOT EXISTS last_seen_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS wallet_last_activity_at timestamptz,
    ADD COLUMN IF NOT EXISTS wallet_last_opened_at timestamptz,
    ADD COLUMN IF NOT EXISTS wallet_last_closed_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_activity_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_opened_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_closed_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_validation_reason varchar(300),
    ADD COLUMN IF NOT EXISTS last_validation_reason_code varchar(120),
    ADD COLUMN IF NOT EXISTS cooldown_until timestamptz,
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

UPDATE futuros_operaciones.copy_wallet_profile
SET wallet_id = lower(btrim(wallet_id)),
    copy_profile_code = COALESCE(NULLIF(btrim(copy_profile_code), ''), 'MOVEMENT_ALL'),
    copy_profile_category = COALESCE(NULLIF(btrim(copy_profile_category), ''), 'CORE_COPY_PROFILE'),
    scope_type = COALESCE(NULLIF(btrim(scope_type), ''), 'strategy'),
    scope_value = COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_profile_code), ''), 'MOVEMENT_ALL')),
    profile_key = COALESCE(
        NULLIF(btrim(profile_key), ''),
        lower(btrim(wallet_id)) || '|' || COALESCE(NULLIF(btrim(copy_profile_code), ''), 'MOVEMENT_ALL') || '|' ||
        COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') || '|' ||
        COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_profile_code), ''), 'MOVEMENT_ALL'))
    ),
    last_seen_at = COALESCE(last_seen_at, now()),
    created_at = COALESCE(created_at, now()),
    updated_at = COALESCE(updated_at, now())
WHERE wallet_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_wallet_profile_key
    ON futuros_operaciones.copy_wallet_profile (profile_key);

CREATE INDEX IF NOT EXISTS ix_copy_wallet_profile_wallet_status
    ON futuros_operaciones.copy_wallet_profile (lower(wallet_id), status, copy_profile_code, scope_type, scope_value);

CREATE INDEX IF NOT EXISTS ix_copy_wallet_profile_config_hash
    ON futuros_operaciones.copy_wallet_profile (profile_config_hash)
    WHERE profile_config_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_copy_wallet_profile_ranking
    ON futuros_operaciones.copy_wallet_profile (status, validated_ranking_score DESC, updated_at DESC);

-- -----------------------------------------------------------------------------
-- Shadow validation aggregate by wallet_profile_id.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS futuros_operaciones.shadow_wallet_profile_validation (
    id bigserial PRIMARY KEY,
    wallet_profile_id bigint NOT NULL,
    status varchar(40) NOT NULL DEFAULT 'SHADOW_TESTING',
    started_at timestamptz NOT NULL DEFAULT now(),
    validated_at timestamptz,
    rejected_at timestamptz,
    closed_positions bigint NOT NULL DEFAULT 0,
    open_positions bigint NOT NULL DEFAULT 0,
    net_pnl_usd numeric(38, 12) NOT NULL DEFAULT 0,
    gross_pnl_usd numeric(38, 12) NOT NULL DEFAULT 0,
    fees_usd numeric(38, 12) NOT NULL DEFAULT 0,
    slippage_usd numeric(38, 12) NOT NULL DEFAULT 0,
    win_rate numeric(18, 6),
    max_drawdown numeric(38, 12),
    avg_slippage_bps numeric(18, 6),
    skipped_events bigint NOT NULL DEFAULT 0,
    duplicate_events bigint NOT NULL DEFAULT 0,
    last_validation_reason varchar(300),
    last_validation_reason_code varchar(120),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE futuros_operaciones.shadow_wallet_profile_validation
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS status varchar(40) NOT NULL DEFAULT 'SHADOW_TESTING',
    ADD COLUMN IF NOT EXISTS started_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS validated_at timestamptz,
    ADD COLUMN IF NOT EXISTS rejected_at timestamptz,
    ADD COLUMN IF NOT EXISTS closed_positions bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS open_positions bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS net_pnl_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS gross_pnl_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fees_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS slippage_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS win_rate numeric(18, 6),
    ADD COLUMN IF NOT EXISTS max_drawdown numeric(38, 12),
    ADD COLUMN IF NOT EXISTS avg_slippage_bps numeric(18, 6),
    ADD COLUMN IF NOT EXISTS skipped_events bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duplicate_events bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_validation_reason varchar(300),
    ADD COLUMN IF NOT EXISTS last_validation_reason_code varchar(120),
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS ix_shadow_wallet_profile_validation_profile_time
    ON futuros_operaciones.shadow_wallet_profile_validation (wallet_profile_id, started_at DESC);

-- -----------------------------------------------------------------------------
-- Shadow allocation/profile representative. Kept for compatibility with existing
-- services while wallet_profile_id is the global runtime/certification key.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS futuros_operaciones.shadow_copy_allocation (
    id bigserial PRIMARY KEY,
    id_user uuid NOT NULL,
    wallet_id varchar(128) NOT NULL,
    copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    copy_strategy_slug varchar(80),
    copy_strategy_label varchar(120),
    copy_mode varchar(80),
    strategy_source_endpoint varchar(180),
    scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    scope_value varchar(160) NOT NULL DEFAULT 'default',
    strategy_key varchar(420) NOT NULL,
    wallet_profile_id bigint,
    shadow_validation_id bigint,
    shadow_version integer NOT NULL DEFAULT 1,
    linked_live_allocation_id bigint,
    promoted_to_live_at timestamptz,
    source_ranking_version varchar(80),
    status varchar(40) NOT NULL DEFAULT 'SHADOW_ACTIVE',
    allocation_pct numeric(9, 6) NOT NULL DEFAULT 0,
    target_live_allocation_pct numeric(9, 6),
    rank_within_strategy integer,
    global_rank integer,
    strategy_score numeric(18, 6),
    decision_score integer,
    copy_guard_status varchar(40),
    copy_guard_action varchar(40),
    copy_guard_reasons text,
    last_validation_reason varchar(300),
    wallet_last_activity_at timestamptz,
    wallet_last_opened_at timestamptz,
    wallet_last_closed_at timestamptz,
    strategy_last_activity_at timestamptz,
    strategy_last_opened_at timestamptz,
    strategy_last_closed_at timestamptz,
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    ends_at timestamptz,
    is_active boolean NOT NULL DEFAULT true
);

ALTER TABLE futuros_operaciones.shadow_copy_allocation
    ADD COLUMN IF NOT EXISTS id_user uuid,
    ADD COLUMN IF NOT EXISTS wallet_id varchar(128),
    ADD COLUMN IF NOT EXISTS copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    ADD COLUMN IF NOT EXISTS copy_strategy_slug varchar(80),
    ADD COLUMN IF NOT EXISTS copy_strategy_label varchar(120),
    ADD COLUMN IF NOT EXISTS copy_mode varchar(80),
    ADD COLUMN IF NOT EXISTS strategy_source_endpoint varchar(180),
    ADD COLUMN IF NOT EXISTS scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    ADD COLUMN IF NOT EXISTS scope_value varchar(160) NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS strategy_key varchar(420),
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_validation_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_version integer NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS linked_live_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS promoted_to_live_at timestamptz,
    ADD COLUMN IF NOT EXISTS source_ranking_version varchar(80),
    ADD COLUMN IF NOT EXISTS status varchar(40) NOT NULL DEFAULT 'SHADOW_ACTIVE',
    ADD COLUMN IF NOT EXISTS allocation_pct numeric(9, 6) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS target_live_allocation_pct numeric(9, 6),
    ADD COLUMN IF NOT EXISTS rank_within_strategy integer,
    ADD COLUMN IF NOT EXISTS global_rank integer,
    ADD COLUMN IF NOT EXISTS strategy_score numeric(18, 6),
    ADD COLUMN IF NOT EXISTS decision_score integer,
    ADD COLUMN IF NOT EXISTS copy_guard_status varchar(40),
    ADD COLUMN IF NOT EXISTS copy_guard_action varchar(40),
    ADD COLUMN IF NOT EXISTS copy_guard_reasons text,
    ADD COLUMN IF NOT EXISTS last_validation_reason varchar(300),
    ADD COLUMN IF NOT EXISTS wallet_last_activity_at timestamptz,
    ADD COLUMN IF NOT EXISTS wallet_last_opened_at timestamptz,
    ADD COLUMN IF NOT EXISTS wallet_last_closed_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_activity_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_opened_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_closed_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_seen_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS ends_at timestamptz,
    ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true;

UPDATE futuros_operaciones.shadow_copy_allocation
SET wallet_id = lower(btrim(wallet_id)),
    copy_strategy_code = COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL'),
    scope_type = COALESCE(NULLIF(btrim(scope_type), ''), 'strategy'),
    scope_value = COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL')),
    strategy_key = COALESCE(
        NULLIF(btrim(strategy_key), ''),
        lower(btrim(wallet_id)) || '|' || COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
        COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') || '|' ||
        COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL'))
    ),
    shadow_version = COALESCE(NULLIF(shadow_version, 0), 1),
    status = COALESCE(NULLIF(btrim(status), ''), 'SHADOW_ACTIVE'),
    allocation_pct = COALESCE(allocation_pct, 0),
    last_seen_at = COALESCE(last_seen_at, now()),
    created_at = COALESCE(created_at, now()),
    updated_at = COALESCE(updated_at, now()),
    is_active = COALESCE(is_active, true)
WHERE wallet_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Shadow operations: aggregate simulated operation per profile/origin/side.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS futuros_operaciones.shadow_copy_operation (
    id_operation uuid PRIMARY KEY,
    shadow_allocation_id bigint NOT NULL,
    linked_live_allocation_id bigint,
    wallet_profile_id bigint,
    shadow_validation_id bigint,
    id_user varchar(80) NOT NULL,
    id_order_origin varchar(120) NOT NULL,
    id_wallet_origin varchar(128) NOT NULL,
    copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    scope_value varchar(160) NOT NULL DEFAULT 'default',
    strategy_key varchar(420) NOT NULL,
    parsymbol varchar(40) NOT NULL,
    type_operation varchar(20) NOT NULL,
    size_usd numeric(38, 12),
    size_par numeric(38, 12),
    price_entry numeric(38, 12),
    price_close numeric(38, 12),
    simulated_fee_usd numeric(38, 12) NOT NULL DEFAULT 0,
    simulated_slippage_usd numeric(38, 12) NOT NULL DEFAULT 0,
    realized_pnl_usd numeric(38, 12),
    status varchar(32) NOT NULL DEFAULT 'OPEN',
    date_creation timestamptz NOT NULL DEFAULT now(),
    date_close timestamptz,
    is_active boolean NOT NULL DEFAULT true
);

ALTER TABLE futuros_operaciones.shadow_copy_operation
    ADD COLUMN IF NOT EXISTS shadow_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS linked_live_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_validation_id bigint,
    ADD COLUMN IF NOT EXISTS id_user varchar(80),
    ADD COLUMN IF NOT EXISTS id_order_origin varchar(120),
    ADD COLUMN IF NOT EXISTS id_wallet_origin varchar(128),
    ADD COLUMN IF NOT EXISTS copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    ADD COLUMN IF NOT EXISTS scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    ADD COLUMN IF NOT EXISTS scope_value varchar(160) NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS strategy_key varchar(420),
    ADD COLUMN IF NOT EXISTS parsymbol varchar(40),
    ADD COLUMN IF NOT EXISTS type_operation varchar(20),
    ADD COLUMN IF NOT EXISTS size_usd numeric(38, 12),
    ADD COLUMN IF NOT EXISTS size_par numeric(38, 12),
    ADD COLUMN IF NOT EXISTS price_entry numeric(38, 12),
    ADD COLUMN IF NOT EXISTS price_close numeric(38, 12),
    ADD COLUMN IF NOT EXISTS simulated_fee_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS simulated_slippage_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS realized_pnl_usd numeric(38, 12),
    ADD COLUMN IF NOT EXISTS status varchar(32) NOT NULL DEFAULT 'OPEN',
    ADD COLUMN IF NOT EXISTS date_creation timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS date_close timestamptz,
    ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true;

-- -----------------------------------------------------------------------------
-- Shadow event ledger: every simulated/open/resize/close/skip/duplicate decision.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS futuros_operaciones.shadow_copy_operation_event (
    id_event uuid PRIMARY KEY,
    shadow_operation_id uuid,
    shadow_position_id uuid,
    shadow_allocation_id bigint NOT NULL,
    linked_live_allocation_id bigint,
    wallet_profile_id bigint,
    shadow_validation_id bigint,
    id_order_origin varchar(120) NOT NULL,
    id_user varchar(80) NOT NULL,
    id_wallet_origin varchar(128) NOT NULL,
    copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    scope_value varchar(160) NOT NULL DEFAULT 'default',
    strategy_key varchar(420) NOT NULL,
    parsymbol varchar(40) NOT NULL,
    type_operation varchar(20),
    event_type varchar(32) NOT NULL,
    position_side varchar(20),
    qty_requested numeric(38, 12),
    qty_executed numeric(38, 12),
    price numeric(38, 12),
    notional_usd numeric(38, 12),
    previous_qty numeric(38, 12),
    resulting_qty numeric(38, 12),
    realized_pnl_usd numeric(38, 12),
    fee_usd numeric(38, 12) NOT NULL DEFAULT 0,
    slippage_bps numeric(18, 6),
    slippage_usd numeric(38, 12) NOT NULL DEFAULT 0,
    decision varchar(40),
    decision_reason varchar(180),
    source_movement_key varchar(160),
    delay_ms bigint,
    trace_id varchar(120),
    source varchar(80),
    reason_code varchar(80),
    event_time timestamptz NOT NULL DEFAULT now(),
    date_creation timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE futuros_operaciones.shadow_copy_operation_event
    ADD COLUMN IF NOT EXISTS shadow_operation_id uuid,
    ADD COLUMN IF NOT EXISTS shadow_position_id uuid,
    ADD COLUMN IF NOT EXISTS shadow_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS linked_live_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_validation_id bigint,
    ADD COLUMN IF NOT EXISTS id_order_origin varchar(120),
    ADD COLUMN IF NOT EXISTS id_user varchar(80),
    ADD COLUMN IF NOT EXISTS id_wallet_origin varchar(128),
    ADD COLUMN IF NOT EXISTS copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    ADD COLUMN IF NOT EXISTS scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    ADD COLUMN IF NOT EXISTS scope_value varchar(160) NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS strategy_key varchar(420),
    ADD COLUMN IF NOT EXISTS parsymbol varchar(40),
    ADD COLUMN IF NOT EXISTS type_operation varchar(20),
    ADD COLUMN IF NOT EXISTS event_type varchar(32),
    ADD COLUMN IF NOT EXISTS position_side varchar(20),
    ADD COLUMN IF NOT EXISTS qty_requested numeric(38, 12),
    ADD COLUMN IF NOT EXISTS qty_executed numeric(38, 12),
    ADD COLUMN IF NOT EXISTS price numeric(38, 12),
    ADD COLUMN IF NOT EXISTS notional_usd numeric(38, 12),
    ADD COLUMN IF NOT EXISTS previous_qty numeric(38, 12),
    ADD COLUMN IF NOT EXISTS resulting_qty numeric(38, 12),
    ADD COLUMN IF NOT EXISTS realized_pnl_usd numeric(38, 12),
    ADD COLUMN IF NOT EXISTS fee_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS slippage_bps numeric(18, 6),
    ADD COLUMN IF NOT EXISTS slippage_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS decision varchar(40),
    ADD COLUMN IF NOT EXISTS decision_reason varchar(180),
    ADD COLUMN IF NOT EXISTS source_movement_key varchar(160),
    ADD COLUMN IF NOT EXISTS delay_ms bigint,
    ADD COLUMN IF NOT EXISTS trace_id varchar(120),
    ADD COLUMN IF NOT EXISTS source varchar(80),
    ADD COLUMN IF NOT EXISTS reason_code varchar(80),
    ADD COLUMN IF NOT EXISTS event_time timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS date_creation timestamptz NOT NULL DEFAULT now();

-- -----------------------------------------------------------------------------
-- Shadow position state: current/open and closed positions by profile/symbol/side.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS futuros_operaciones.shadow_position_state (
    id uuid PRIMARY KEY,
    shadow_allocation_id bigint NOT NULL,
    linked_live_allocation_id bigint,
    wallet_profile_id bigint,
    shadow_validation_id bigint,
    id_user varchar(80) NOT NULL,
    wallet_id varchar(128) NOT NULL,
    copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    scope_value varchar(160) NOT NULL DEFAULT 'default',
    strategy_key varchar(420) NOT NULL,
    parsymbol varchar(40) NOT NULL,
    position_side varchar(20) NOT NULL,
    qty numeric(38, 12) NOT NULL DEFAULT 0,
    entry_price numeric(38, 12),
    mark_price numeric(38, 12),
    notional_usd numeric(38, 12),
    realized_pnl_usd numeric(38, 12) NOT NULL DEFAULT 0,
    unrealized_pnl_usd numeric(38, 12) NOT NULL DEFAULT 0,
    fees_usd numeric(38, 12) NOT NULL DEFAULT 0,
    slippage_usd numeric(38, 12) NOT NULL DEFAULT 0,
    status varchar(32) NOT NULL DEFAULT 'OPEN',
    last_source_event_id varchar(120),
    opened_at timestamptz,
    closed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE futuros_operaciones.shadow_position_state
    ADD COLUMN IF NOT EXISTS shadow_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS linked_live_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_validation_id bigint,
    ADD COLUMN IF NOT EXISTS id_user varchar(80),
    ADD COLUMN IF NOT EXISTS wallet_id varchar(128),
    ADD COLUMN IF NOT EXISTS copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    ADD COLUMN IF NOT EXISTS scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    ADD COLUMN IF NOT EXISTS scope_value varchar(160) NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS strategy_key varchar(420),
    ADD COLUMN IF NOT EXISTS parsymbol varchar(40),
    ADD COLUMN IF NOT EXISTS position_side varchar(20),
    ADD COLUMN IF NOT EXISTS qty numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS entry_price numeric(38, 12),
    ADD COLUMN IF NOT EXISTS mark_price numeric(38, 12),
    ADD COLUMN IF NOT EXISTS notional_usd numeric(38, 12),
    ADD COLUMN IF NOT EXISTS realized_pnl_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unrealized_pnl_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fees_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS slippage_usd numeric(38, 12) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS status varchar(32) NOT NULL DEFAULT 'OPEN',
    ADD COLUMN IF NOT EXISTS last_source_event_id varchar(120),
    ADD COLUMN IF NOT EXISTS opened_at timestamptz,
    ADD COLUMN IF NOT EXISTS closed_at timestamptz,
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

-- -----------------------------------------------------------------------------
-- user_copy_allocation compatibility columns used by LIVE promotion/profile link.
-- -----------------------------------------------------------------------------
ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    ADD COLUMN IF NOT EXISTS strategy_score numeric(18, 6),
    ADD COLUMN IF NOT EXISTS execution_mode varchar(16) NOT NULL DEFAULT 'LIVE',
    ADD COLUMN IF NOT EXISTS status_reason varchar(160),
    ADD COLUMN IF NOT EXISTS scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    ADD COLUMN IF NOT EXISTS scope_value varchar(160) NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS strategy_key varchar(420),
    ADD COLUMN IF NOT EXISTS linked_shadow_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS promoted_from_shadow_at timestamptz,
    ADD COLUMN IF NOT EXISTS source_ranking_version varchar(80),
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint;

UPDATE futuros_operaciones.user_copy_allocation
SET scope_type = COALESCE(NULLIF(btrim(scope_type), ''), 'strategy'),
    scope_value = CASE
        WHEN scope_value IS NULL OR btrim(scope_value) = '' OR lower(btrim(scope_value)) = 'default'
            THEN COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL')
        ELSE btrim(scope_value)
    END,
    strategy_key = COALESCE(
        NULLIF(btrim(strategy_key), ''),
        lower(wallet_id) || '|' || COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
        COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') || '|' ||
        CASE
            WHEN scope_value IS NULL OR btrim(scope_value) = '' OR lower(btrim(scope_value)) = 'default'
                THEN COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL')
            ELSE btrim(scope_value)
        END
    )
WHERE wallet_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Rebuild/link global profiles and validations from surviving allocations/live rows.
-- If shadow_copy_allocation was also deleted, this remains safe and will only use live rows.
-- -----------------------------------------------------------------------------
WITH shadow_profiles AS (
    SELECT DISTINCT ON (
        COALESCE(
            NULLIF(btrim(strategy_key), ''),
            lower(wallet_id) || '|' ||
            COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
            COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') || '|' ||
            COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL'))
        )
    )
        lower(wallet_id) AS wallet_id,
        COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') AS copy_profile_code,
        COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') AS scope_type,
        COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL')) AS scope_value,
        COALESCE(
            NULLIF(btrim(strategy_key), ''),
            lower(wallet_id) || '|' ||
            COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
            COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') || '|' ||
            COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL'))
        ) AS profile_key,
        COALESCE(NULLIF(btrim(status), ''), 'SHADOW_TESTING') AS status,
        strategy_score,
        decision_score,
        copy_guard_status,
        copy_guard_action,
        last_seen_at,
        wallet_last_activity_at,
        wallet_last_opened_at,
        wallet_last_closed_at,
        strategy_last_activity_at,
        strategy_last_opened_at,
        strategy_last_closed_at,
        last_validation_reason,
        created_at,
        updated_at
    FROM futuros_operaciones.shadow_copy_allocation
    WHERE wallet_id IS NOT NULL
    ORDER BY profile_key, last_seen_at DESC NULLS LAST, id DESC
),
live_profiles AS (
    SELECT DISTINCT ON (
        COALESCE(
            NULLIF(btrim(strategy_key), ''),
            lower(wallet_id) || '|' ||
            COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
            COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') || '|' ||
            COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL'))
        )
    )
        lower(wallet_id) AS wallet_id,
        COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') AS copy_profile_code,
        COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') AS scope_type,
        COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL')) AS scope_value,
        COALESCE(
            NULLIF(btrim(strategy_key), ''),
            lower(wallet_id) || '|' ||
            COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
            COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') || '|' ||
            COALESCE(NULLIF(btrim(scope_value), ''), COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL'))
        ) AS profile_key,
        CASE WHEN COALESCE(execution_mode, 'LIVE') = 'LIVE' AND ends_at IS NULL THEN 'LIVE_ACTIVE' ELSE 'LIVE_ELIGIBLE' END AS status,
        strategy_score,
        NULL::integer AS decision_score,
        NULL::varchar AS copy_guard_status,
        NULL::varchar AS copy_guard_action,
        COALESCE(updated_at, now()) AS last_seen_at,
        NULL::timestamptz AS wallet_last_activity_at,
        NULL::timestamptz AS wallet_last_opened_at,
        NULL::timestamptz AS wallet_last_closed_at,
        NULL::timestamptz AS strategy_last_activity_at,
        NULL::timestamptz AS strategy_last_opened_at,
        NULL::timestamptz AS strategy_last_closed_at,
        status_reason AS last_validation_reason,
        COALESCE(updated_at, now()) AS created_at,
        COALESCE(updated_at, now()) AS updated_at
    FROM futuros_operaciones.user_copy_allocation
    WHERE wallet_id IS NOT NULL
    ORDER BY profile_key, updated_at DESC NULLS LAST, id DESC
),
profiles AS (
    SELECT * FROM shadow_profiles
    UNION ALL
    SELECT * FROM live_profiles
)
INSERT INTO futuros_operaciones.copy_wallet_profile (
    wallet_id,
    copy_profile_code,
    copy_profile_category,
    scope_type,
    scope_value,
    profile_key,
    status,
    historical_score,
    shadow_score,
    validated_ranking_score,
    copy_guard_status,
    copy_guard_action,
    last_seen_at,
    wallet_last_activity_at,
    wallet_last_opened_at,
    wallet_last_closed_at,
    strategy_last_activity_at,
    strategy_last_opened_at,
    strategy_last_closed_at,
    last_validation_reason,
    created_at,
    updated_at
)
SELECT DISTINCT ON (profile_key)
    wallet_id,
    copy_profile_code,
    CASE
        WHEN copy_profile_code IN ('RECENT_7D', 'RECENT_14D', 'RECENT_30D') THEN 'SCORING_WINDOW'
        WHEN copy_profile_code IN ('ROBUST_EX_TOP_1', 'ROBUST_EX_TOP_5') THEN 'ROBUSTNESS_CHECK'
        WHEN copy_profile_code IN ('PARTIAL_REDUCE', 'FINAL_CLOSE_ONLY', 'REDUCE_ONLY', 'CLOSE_ONLY') THEN 'DIAGNOSTIC_ONLY'
        WHEN copy_profile_code IN ('MOVEMENT_ALL', 'LONG_ONLY', 'SHORT_ONLY') THEN 'CORE_COPY_PROFILE'
        ELSE 'ADVANCED_COPY_PROFILE'
    END,
    scope_type,
    scope_value,
    profile_key,
    status,
    strategy_score,
    NULL,
    CASE WHEN status IN ('VALIDATED', 'LIVE_ELIGIBLE', 'LIVE_ACTIVE', 'PROMOTED_TO_LIVE') THEN strategy_score ELSE NULL END,
    copy_guard_status,
    copy_guard_action,
    COALESCE(last_seen_at, now()),
    wallet_last_activity_at,
    wallet_last_opened_at,
    wallet_last_closed_at,
    strategy_last_activity_at,
    strategy_last_opened_at,
    strategy_last_closed_at,
    last_validation_reason,
    COALESCE(created_at, now()),
    COALESCE(updated_at, now())
FROM profiles
WHERE profile_key IS NOT NULL
ORDER BY profile_key, last_seen_at DESC NULLS LAST
ON CONFLICT (profile_key) DO UPDATE
SET status = EXCLUDED.status,
    historical_score = COALESCE(EXCLUDED.historical_score, futuros_operaciones.copy_wallet_profile.historical_score),
    validated_ranking_score = COALESCE(EXCLUDED.validated_ranking_score, futuros_operaciones.copy_wallet_profile.validated_ranking_score),
    copy_guard_status = COALESCE(EXCLUDED.copy_guard_status, futuros_operaciones.copy_wallet_profile.copy_guard_status),
    copy_guard_action = COALESCE(EXCLUDED.copy_guard_action, futuros_operaciones.copy_wallet_profile.copy_guard_action),
    last_seen_at = GREATEST(futuros_operaciones.copy_wallet_profile.last_seen_at, EXCLUDED.last_seen_at),
    updated_at = now();

UPDATE futuros_operaciones.shadow_copy_allocation s
SET wallet_profile_id = p.id
FROM futuros_operaciones.copy_wallet_profile p
WHERE s.wallet_profile_id IS NULL
  AND p.profile_key = COALESCE(
        NULLIF(btrim(s.strategy_key), ''),
        lower(s.wallet_id) || '|' ||
        COALESCE(NULLIF(btrim(s.copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
        COALESCE(NULLIF(btrim(s.scope_type), ''), 'strategy') || '|' ||
        COALESCE(NULLIF(btrim(s.scope_value), ''), COALESCE(NULLIF(btrim(s.copy_strategy_code), ''), 'MOVEMENT_ALL'))
  );

UPDATE futuros_operaciones.user_copy_allocation u
SET wallet_profile_id = p.id
FROM futuros_operaciones.copy_wallet_profile p
WHERE u.wallet_profile_id IS NULL
  AND p.profile_key = COALESCE(
        NULLIF(btrim(u.strategy_key), ''),
        lower(u.wallet_id) || '|' ||
        COALESCE(NULLIF(btrim(u.copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
        COALESCE(NULLIF(btrim(u.scope_type), ''), 'strategy') || '|' ||
        COALESCE(NULLIF(btrim(u.scope_value), ''), COALESCE(NULLIF(btrim(u.copy_strategy_code), ''), 'MOVEMENT_ALL'))
  );

INSERT INTO futuros_operaciones.shadow_wallet_profile_validation (
    wallet_profile_id,
    status,
    started_at,
    closed_positions,
    open_positions,
    net_pnl_usd,
    gross_pnl_usd,
    fees_usd,
    slippage_usd,
    last_validation_reason,
    last_validation_reason_code,
    created_at,
    updated_at
)
SELECT
    p.id,
    p.status,
    now(),
    COALESCE(stats.closed_positions, 0),
    COALESCE(stats.open_positions, 0),
    COALESCE(stats.net_pnl_usd, 0),
    COALESCE(stats.net_pnl_usd, 0) + COALESCE(stats.slippage_usd, 0),
    COALESCE(stats.fees_usd, 0),
    COALESCE(stats.slippage_usd, 0),
    p.last_validation_reason,
    split_part(COALESCE(p.last_validation_reason, ''), ':', 1),
    now(),
    now()
FROM futuros_operaciones.copy_wallet_profile p
LEFT JOIN (
    SELECT
        s.wallet_profile_id,
        count(*) FILTER (WHERE s.status = 'CLOSED' AND s.closed_at IS NOT NULL) AS closed_positions,
        count(*) FILTER (WHERE s.status = 'OPEN') AS open_positions,
        sum(s.realized_pnl_usd) FILTER (WHERE s.status = 'CLOSED' AND s.closed_at IS NOT NULL) AS net_pnl_usd,
        sum(s.fees_usd) AS fees_usd,
        sum(s.slippage_usd) AS slippage_usd
    FROM futuros_operaciones.shadow_position_state s
    WHERE s.wallet_profile_id IS NOT NULL
    GROUP BY s.wallet_profile_id
) stats ON stats.wallet_profile_id = p.id
WHERE NOT EXISTS (
    SELECT 1
    FROM futuros_operaciones.shadow_wallet_profile_validation v
    WHERE v.wallet_profile_id = p.id
);

UPDATE futuros_operaciones.shadow_copy_allocation s
SET shadow_validation_id = v.id
FROM futuros_operaciones.shadow_wallet_profile_validation v
WHERE s.shadow_validation_id IS NULL
  AND s.wallet_profile_id = v.wallet_profile_id;

UPDATE futuros_operaciones.shadow_copy_operation o
SET wallet_profile_id = s.wallet_profile_id,
    shadow_validation_id = s.shadow_validation_id
FROM futuros_operaciones.shadow_copy_allocation s
WHERE o.shadow_allocation_id = s.id
  AND o.wallet_profile_id IS NULL;

UPDATE futuros_operaciones.shadow_copy_operation_event e
SET wallet_profile_id = s.wallet_profile_id,
    shadow_validation_id = s.shadow_validation_id
FROM futuros_operaciones.shadow_copy_allocation s
WHERE e.shadow_allocation_id = s.id
  AND e.wallet_profile_id IS NULL;

UPDATE futuros_operaciones.shadow_position_state ps
SET wallet_profile_id = s.wallet_profile_id,
    shadow_validation_id = s.shadow_validation_id
FROM futuros_operaciones.shadow_copy_allocation s
WHERE ps.shadow_allocation_id = s.id
  AND ps.wallet_profile_id IS NULL;

-- -----------------------------------------------------------------------------
-- Constraints. Use DO blocks because PostgreSQL has no ADD CONSTRAINT IF NOT EXISTS.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_copy_wallet_profile_category') THEN
        ALTER TABLE futuros_operaciones.copy_wallet_profile
            ADD CONSTRAINT chk_copy_wallet_profile_category
            CHECK (copy_profile_category IN ('CORE_COPY_PROFILE', 'ADVANCED_COPY_PROFILE', 'SCORING_WINDOW', 'ROBUSTNESS_CHECK', 'DIAGNOSTIC_ONLY'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_shadow_copy_operation_status') THEN
        ALTER TABLE futuros_operaciones.shadow_copy_operation
            ADD CONSTRAINT chk_shadow_copy_operation_status
            CHECK (status IN ('OPEN', 'CLOSED', 'REJECTED', 'IGNORED', 'ERROR'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_shadow_position_state_status') THEN
        ALTER TABLE futuros_operaciones.shadow_position_state
            ADD CONSTRAINT chk_shadow_position_state_status
            CHECK (status IN ('OPEN', 'CLOSED', 'IGNORED', 'ERROR'));
    END IF;
END $$;

-- -----------------------------------------------------------------------------
-- Indexes for profile runtime, idempotency, hot path lookup, and validation.
-- -----------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS ux_shadow_copy_allocation_profile_key
    ON futuros_operaciones.shadow_copy_allocation (
        id_user,
        lower(wallet_id),
        copy_strategy_code,
        scope_type,
        scope_value,
        shadow_version
    )
    WHERE ends_at IS NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_copy_allocation_runtime_wallet_status
    ON futuros_operaciones.shadow_copy_allocation (
        lower(wallet_id),
        status,
        copy_strategy_code,
        scope_type,
        scope_value
    )
    WHERE ends_at IS NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_copy_allocation_runtime_wallet_profile
    ON futuros_operaciones.shadow_copy_allocation (lower(wallet_id), status, wallet_profile_id, copy_strategy_code, scope_type, scope_value)
    WHERE ends_at IS NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_wallet_profile_active
    ON futuros_operaciones.user_copy_allocation (wallet_profile_id, id_user)
    WHERE ends_at IS NULL AND is_active = true AND execution_mode = 'LIVE';

CREATE UNIQUE INDEX IF NOT EXISTS ux_shadow_operation_profile_origin_type_active
    ON futuros_operaciones.shadow_copy_operation (wallet_profile_id, id_order_origin, type_operation)
    WHERE wallet_profile_id IS NOT NULL AND is_active = true;

CREATE UNIQUE INDEX IF NOT EXISTS ux_shadow_operation_allocation_origin_type_active
    ON futuros_operaciones.shadow_copy_operation (shadow_allocation_id, id_order_origin, type_operation)
    WHERE shadow_allocation_id IS NOT NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_operation_wallet_profile_position_active
    ON futuros_operaciones.shadow_copy_operation (wallet_profile_id, parsymbol, type_operation, date_creation DESC)
    WHERE wallet_profile_id IS NOT NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_operation_allocation_position_active
    ON futuros_operaciones.shadow_copy_operation (shadow_allocation_id, parsymbol, type_operation, date_creation DESC)
    WHERE is_active = true;

CREATE UNIQUE INDEX IF NOT EXISTS ux_shadow_event_allocation_idempotency
    ON futuros_operaciones.shadow_copy_operation_event (
        shadow_allocation_id,
        id_order_origin,
        event_type,
        (COALESCE(position_side, 'BOTH')),
        event_time
    )
    WHERE shadow_allocation_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_shadow_event_wallet_profile_idempotency
    ON futuros_operaciones.shadow_copy_operation_event (
        wallet_profile_id,
        id_order_origin,
        event_type,
        (COALESCE(position_side, 'BOTH')),
        event_time
    )
    WHERE wallet_profile_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_shadow_event_wallet_profile_lookup
    ON futuros_operaciones.shadow_copy_operation_event (wallet_profile_id, id_order_origin, event_type, position_side, event_time)
    WHERE wallet_profile_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_shadow_event_position_time
    ON futuros_operaciones.shadow_copy_operation_event (shadow_position_id, event_time DESC)
    WHERE shadow_position_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_shadow_event_trace_id
    ON futuros_operaciones.shadow_copy_operation_event (trace_id)
    WHERE trace_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_shadow_position_state_wallet_profile_open
    ON futuros_operaciones.shadow_position_state (wallet_profile_id, parsymbol, position_side)
    WHERE wallet_profile_id IS NOT NULL AND status = 'OPEN';

CREATE UNIQUE INDEX IF NOT EXISTS ux_shadow_position_state_allocation_open
    ON futuros_operaciones.shadow_position_state (shadow_allocation_id, parsymbol, position_side)
    WHERE shadow_allocation_id IS NOT NULL AND status = 'OPEN';

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_wallet_profile_open
    ON futuros_operaciones.shadow_position_state (wallet_profile_id, parsymbol, position_side, status)
    WHERE wallet_profile_id IS NOT NULL AND status = 'OPEN';

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_wallet_profile_closed
    ON futuros_operaciones.shadow_position_state (wallet_profile_id, status, closed_at)
    WHERE wallet_profile_id IS NOT NULL AND status = 'CLOSED';

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_wallet_profile_closed_position
    ON futuros_operaciones.shadow_position_state (wallet_profile_id, parsymbol, position_side, closed_at DESC)
    WHERE wallet_profile_id IS NOT NULL AND status = 'CLOSED';

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_allocation_closed_position
    ON futuros_operaciones.shadow_position_state (shadow_allocation_id, parsymbol, position_side, closed_at DESC)
    WHERE status = 'CLOSED';

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_profile_last_event
    ON futuros_operaciones.shadow_position_state (wallet_profile_id, last_source_event_id)
    WHERE wallet_profile_id IS NOT NULL AND last_source_event_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Lightweight repair for operation rows that were closed through position state
-- before the operation aggregate existed or before close propagation was fixed.
-- -----------------------------------------------------------------------------
UPDATE futuros_operaciones.shadow_copy_operation o
SET status = 'CLOSED',
    is_active = false,
    date_close = COALESCE(o.date_close, ps.closed_at),
    price_close = COALESCE(o.price_close, ps.mark_price),
    realized_pnl_usd = COALESCE(o.realized_pnl_usd, ps.realized_pnl_usd)
FROM futuros_operaciones.shadow_position_state ps
WHERE o.wallet_profile_id = ps.wallet_profile_id
  AND o.parsymbol = ps.parsymbol
  AND o.type_operation = ps.position_side
  AND ps.status = 'CLOSED'
  AND ps.closed_at IS NOT NULL
  AND o.status = 'OPEN';
