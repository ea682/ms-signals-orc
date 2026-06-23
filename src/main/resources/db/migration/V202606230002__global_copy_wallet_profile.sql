-- Introduce el perfil global wallet + estrategia + scope.
-- Mantiene compatibilidad con shadow_copy_allocation por usuario, pero permite que
-- SHADOW/runtime/idempotencia se agrupen por wallet_profile_id.

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

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_wallet_profile_key
    ON futuros_operaciones.copy_wallet_profile (profile_key);

CREATE INDEX IF NOT EXISTS ix_copy_wallet_profile_wallet_status
    ON futuros_operaciones.copy_wallet_profile (lower(wallet_id), status, copy_profile_code, scope_type, scope_value);

CREATE INDEX IF NOT EXISTS ix_copy_wallet_profile_config_hash
    ON futuros_operaciones.copy_wallet_profile (profile_config_hash)
    WHERE profile_config_hash IS NOT NULL;

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

CREATE INDEX IF NOT EXISTS ix_shadow_wallet_profile_validation_profile_time
    ON futuros_operaciones.shadow_wallet_profile_validation (wallet_profile_id, started_at DESC);

ALTER TABLE futuros_operaciones.shadow_copy_allocation
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_validation_id bigint;

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint;

ALTER TABLE futuros_operaciones.shadow_copy_operation
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_validation_id bigint;

ALTER TABLE futuros_operaciones.shadow_copy_operation_event
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_validation_id bigint;

ALTER TABLE futuros_operaciones.shadow_position_state
    ADD COLUMN IF NOT EXISTS wallet_profile_id bigint,
    ADD COLUMN IF NOT EXISTS shadow_validation_id bigint;

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
        CASE WHEN execution_mode = 'LIVE' AND ends_at IS NULL THEN 'LIVE_ACTIVE' ELSE 'LIVE_ELIGIBLE' END AS status,
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
    0,
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

CREATE INDEX IF NOT EXISTS ix_shadow_copy_allocation_runtime_wallet_profile
    ON futuros_operaciones.shadow_copy_allocation (lower(wallet_id), status, wallet_profile_id, copy_strategy_code, scope_type, scope_value)
    WHERE ends_at IS NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_wallet_profile_active
    ON futuros_operaciones.user_copy_allocation (wallet_profile_id, id_user)
    WHERE ends_at IS NULL AND is_active = true AND execution_mode = 'LIVE';

CREATE INDEX IF NOT EXISTS ix_shadow_operation_wallet_profile_active
    ON futuros_operaciones.shadow_copy_operation (wallet_profile_id, id_order_origin, type_operation)
    WHERE wallet_profile_id IS NOT NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_event_wallet_profile_lookup
    ON futuros_operaciones.shadow_copy_operation_event (wallet_profile_id, id_order_origin, event_type, position_side, event_time)
    WHERE wallet_profile_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_wallet_profile_open
    ON futuros_operaciones.shadow_position_state (wallet_profile_id, parsymbol, position_side, status)
    WHERE wallet_profile_id IS NOT NULL AND status = 'OPEN';

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_wallet_profile_closed
    ON futuros_operaciones.shadow_position_state (wallet_profile_id, status, closed_at)
    WHERE wallet_profile_id IS NOT NULL AND status = 'CLOSED';
