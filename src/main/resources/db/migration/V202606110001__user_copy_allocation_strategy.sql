ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS copy_strategy_code varchar(64) NOT NULL DEFAULT 'MOVEMENT_ALL',
    ADD COLUMN IF NOT EXISTS copy_strategy_slug varchar(80),
    ADD COLUMN IF NOT EXISTS copy_strategy_label varchar(120),
    ADD COLUMN IF NOT EXISTS copy_mode varchar(80),
    ADD COLUMN IF NOT EXISTS strategy_source_endpoint varchar(180),
    ADD COLUMN IF NOT EXISTS rank_within_strategy integer,
    ADD COLUMN IF NOT EXISTS global_rank integer,
    ADD COLUMN IF NOT EXISTS strategy_score numeric(18, 6);

UPDATE futuros_operaciones.user_copy_allocation
SET copy_strategy_code = 'MOVEMENT_ALL'
WHERE copy_strategy_code IS NULL OR btrim(copy_strategy_code) = '';

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS uq_user_copy_allocation_user_wallet;

DROP INDEX IF EXISTS futuros_operaciones.ux_user_copy_allocation_user_wallet_strategy_active;
CREATE UNIQUE INDEX IF NOT EXISTS ux_user_copy_allocation_user_wallet_strategy_active
    ON futuros_operaciones.user_copy_allocation (id_user, lower(wallet_id), copy_strategy_code)
    WHERE ends_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_wallet_strategy_active
    ON futuros_operaciones.user_copy_allocation (lower(wallet_id), copy_strategy_code, id_user)
    WHERE ends_at IS NULL AND is_active = true;
