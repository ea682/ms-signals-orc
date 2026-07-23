ALTER TABLE futuros_operaciones.detail_user
    ADD COLUMN IF NOT EXISTS participate_in_micro_live BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS auto_follow_certified_live BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS continue_in_live_after_certification BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS futuros_operaciones.user_wallet_copy_preference (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    wallet_id VARCHAR(160) NOT NULL,
    wallet_blocked BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_wallet_copy_preference_user_wallet
    ON futuros_operaciones.user_wallet_copy_preference (user_id, lower(wallet_id));

CREATE INDEX IF NOT EXISTS ix_user_wallet_copy_preference_blocked
    ON futuros_operaciones.user_wallet_copy_preference (user_id, wallet_blocked)
    WHERE wallet_blocked = TRUE;

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS activation_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS live_certification_id UUID;

UPDATE futuros_operaciones.user_copy_allocation
SET activation_at = COALESCE(promoted_from_shadow_at, updated_at, now())
WHERE activation_at IS NULL;

ALTER TABLE futuros_operaciones.user_copy_allocation
    ALTER COLUMN activation_at SET DEFAULT now(),
    ALTER COLUMN activation_at SET NOT NULL;

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS fk_user_copy_allocation_live_certification;

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT fk_user_copy_allocation_live_certification
    FOREIGN KEY (live_certification_id)
    REFERENCES futuros_operaciones.strategy_live_certification(id)
    ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_live_certification
    ON futuros_operaciones.user_copy_allocation (live_certification_id, status, is_active)
    WHERE live_certification_id IS NOT NULL;

ALTER TABLE futuros_operaciones.live_allocation_activation_audit
    DROP CONSTRAINT IF EXISTS live_allocation_activation_mode_chk;

ALTER TABLE futuros_operaciones.live_allocation_activation_audit
    ADD CONSTRAINT live_allocation_activation_mode_chk CHECK (
        (prior_mode = 'MICRO_LIVE' AND next_mode = 'LIVE')
        OR (prior_mode = 'LIVE' AND next_mode = 'LIVE')
    );

DROP INDEX IF EXISTS futuros_operaciones.ux_user_copy_allocation_user_wallet_strategy_scope_active;

CREATE UNIQUE INDEX ux_user_copy_allocation_user_wallet_strategy_scope_mode_open
    ON futuros_operaciones.user_copy_allocation (
        id_user, lower(wallet_id), copy_strategy_code, scope_type, scope_value, execution_mode
    )
    WHERE ends_at IS NULL;
