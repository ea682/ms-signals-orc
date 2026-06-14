ALTER TABLE futuros_operaciones.user_copy_allocation
    ALTER COLUMN status TYPE varchar(40),
    ADD COLUMN IF NOT EXISTS execution_mode varchar(16) NOT NULL DEFAULT 'LIVE',
    ADD COLUMN IF NOT EXISTS status_reason varchar(160),
    ADD COLUMN IF NOT EXISTS status_updated_at timestamptz,
    ADD COLUMN IF NOT EXISTS status_cooldown_until timestamptz,
    ADD COLUMN IF NOT EXISTS leverage_override numeric(10, 2);

UPDATE futuros_operaciones.user_copy_allocation
SET execution_mode = 'LIVE'
WHERE execution_mode IS NULL OR btrim(execution_mode) = '';

UPDATE futuros_operaciones.user_copy_allocation
SET status_updated_at = COALESCE(status_updated_at, updated_at, now())
WHERE status_updated_at IS NULL;

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS chk_user_copy_allocation_execution_mode;
ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT chk_user_copy_allocation_execution_mode
    CHECK (execution_mode IN ('LIVE', 'SHADOW'));

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS chk_user_copy_allocation_copy_status;
ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT chk_user_copy_allocation_copy_status
    CHECK (lower(status) IN (
        'active',
        'exit_only',
        'paused',
        'paused_by_negative_pnl',
        'paused_by_stale_metric',
        'paused_by_risk',
        'disabled_manual',
        'closed'
    ));

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_status_guard
    ON futuros_operaciones.user_copy_allocation (status, status_cooldown_until)
    WHERE ends_at IS NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_shadow_active
    ON futuros_operaciones.user_copy_allocation (execution_mode, id_user, lower(wallet_id), copy_strategy_code)
    WHERE ends_at IS NULL AND is_active = true;

ALTER TABLE futuros_operaciones.copy_operation
    ADD COLUMN IF NOT EXISTS user_copy_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS copy_strategy_code varchar(64),
    ADD COLUMN IF NOT EXISTS execution_mode varchar(16) NOT NULL DEFAULT 'LIVE',
    ADD COLUMN IF NOT EXISTS is_shadow boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS shadow_status varchar(32);

UPDATE futuros_operaciones.copy_operation
SET execution_mode = CASE WHEN is_shadow THEN 'SHADOW' ELSE COALESCE(NULLIF(execution_mode, ''), 'LIVE') END;

ALTER TABLE futuros_operaciones.copy_operation
    DROP CONSTRAINT IF EXISTS chk_copy_operation_execution_mode;
ALTER TABLE futuros_operaciones.copy_operation
    ADD CONSTRAINT chk_copy_operation_execution_mode
    CHECK (execution_mode IN ('LIVE', 'SHADOW'));

CREATE INDEX IF NOT EXISTS ix_copy_operation_user_strategy_active
    ON futuros_operaciones.copy_operation (id_user, user_copy_allocation_id, copy_strategy_code, is_active)
    WHERE is_active = true;

CREATE INDEX IF NOT EXISTS ix_copy_operation_shadow_active
    ON futuros_operaciones.copy_operation (is_shadow, id_user, date_creation DESC)
    WHERE is_shadow = true;

ALTER TABLE futuros_operaciones.copy_operation_event
    ADD COLUMN IF NOT EXISTS user_copy_allocation_id bigint,
    ADD COLUMN IF NOT EXISTS copy_strategy_code varchar(64),
    ADD COLUMN IF NOT EXISTS execution_mode varchar(16) NOT NULL DEFAULT 'LIVE',
    ADD COLUMN IF NOT EXISTS is_shadow boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS decision varchar(40),
    ADD COLUMN IF NOT EXISTS decision_reason varchar(160),
    ADD COLUMN IF NOT EXISTS source_movement_key varchar(160);

UPDATE futuros_operaciones.copy_operation_event
SET execution_mode = CASE WHEN is_shadow THEN 'SHADOW' ELSE COALESCE(NULLIF(execution_mode, ''), 'LIVE') END;

ALTER TABLE futuros_operaciones.copy_operation_event
    DROP CONSTRAINT IF EXISTS chk_copy_operation_event_execution_mode;
ALTER TABLE futuros_operaciones.copy_operation_event
    ADD CONSTRAINT chk_copy_operation_event_execution_mode
    CHECK (execution_mode IN ('LIVE', 'SHADOW'));

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_allocation_time
    ON futuros_operaciones.copy_operation_event (user_copy_allocation_id, event_time DESC)
    WHERE user_copy_allocation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_shadow_time
    ON futuros_operaciones.copy_operation_event (is_shadow, event_time DESC)
    WHERE is_shadow = true;

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_strategy_time
    ON futuros_operaciones.copy_operation_event (copy_strategy_code, event_time DESC)
    WHERE copy_strategy_code IS NOT NULL;
