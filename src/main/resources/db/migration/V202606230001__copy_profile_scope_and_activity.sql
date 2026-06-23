-- Completa la unidad wallet + estrategia + scope para LIVE y conserva activity separada para SHADOW.

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS scope_type varchar(32) NOT NULL DEFAULT 'strategy',
    ADD COLUMN IF NOT EXISTS scope_value varchar(160) NOT NULL DEFAULT 'default',
    ADD COLUMN IF NOT EXISTS strategy_key varchar(420),
    ADD COLUMN IF NOT EXISTS linked_shadow_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS promoted_from_shadow_at timestamptz,
    ADD COLUMN IF NOT EXISTS source_ranking_version varchar(80);

UPDATE futuros_operaciones.user_copy_allocation
SET scope_type = COALESCE(NULLIF(btrim(scope_type), ''), 'strategy'),
    scope_value = CASE
        WHEN scope_value IS NULL OR btrim(scope_value) = '' OR lower(btrim(scope_value)) = 'default'
            THEN COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL')
        ELSE btrim(scope_value)
    END,
    strategy_key = lower(wallet_id) || '|' || COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL') || '|' ||
        COALESCE(NULLIF(btrim(scope_type), ''), 'strategy') || '|' ||
        CASE
            WHEN scope_value IS NULL OR btrim(scope_value) = '' OR lower(btrim(scope_value)) = 'default'
                THEN COALESCE(NULLIF(btrim(copy_strategy_code), ''), 'MOVEMENT_ALL')
            ELSE btrim(scope_value)
        END
WHERE ends_at IS NULL;

DROP INDEX IF EXISTS futuros_operaciones.ux_user_copy_allocation_user_wallet_strategy_active;
DROP INDEX IF EXISTS futuros_operaciones.ux_user_copy_allocation_user_wallet_strategy_scope_active;

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_copy_allocation_user_wallet_strategy_scope_active
    ON futuros_operaciones.user_copy_allocation (
        id_user,
        lower(wallet_id),
        copy_strategy_code,
        scope_type,
        scope_value
    )
    WHERE ends_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_wallet_strategy_scope_active
    ON futuros_operaciones.user_copy_allocation (
        lower(wallet_id),
        copy_strategy_code,
        scope_type,
        scope_value,
        id_user
    )
    WHERE ends_at IS NULL AND is_active = true;

ALTER TABLE futuros_operaciones.shadow_copy_allocation
    ADD COLUMN IF NOT EXISTS wallet_last_activity_at timestamptz,
    ADD COLUMN IF NOT EXISTS wallet_last_opened_at timestamptz,
    ADD COLUMN IF NOT EXISTS wallet_last_closed_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_activity_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_opened_at timestamptz,
    ADD COLUMN IF NOT EXISTS strategy_last_closed_at timestamptz;
