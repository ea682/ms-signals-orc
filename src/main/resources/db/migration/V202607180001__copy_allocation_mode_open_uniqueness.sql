-- The production baseline still carries an older coarse uniqueness index that
-- prevents a pending LIVE allocation from coexisting with the MICRO_LIVE row
-- whose history must be preserved until atomic activation.
DROP INDEX IF EXISTS futuros_operaciones.ux_user_copy_allocation_user_wallet_strategy_open;

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_copy_allocation_user_wallet_strategy_scope_mode_open
    ON futuros_operaciones.user_copy_allocation (
        id_user, lower(wallet_id), copy_strategy_code, scope_type, scope_value, execution_mode
    )
    WHERE ends_at IS NULL;
