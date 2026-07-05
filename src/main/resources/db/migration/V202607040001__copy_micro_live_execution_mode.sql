ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS chk_user_copy_allocation_execution_mode;
ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT chk_user_copy_allocation_execution_mode
    CHECK (execution_mode IN ('LIVE', 'SHADOW', 'MICRO_LIVE'));

ALTER TABLE futuros_operaciones.copy_operation
    DROP CONSTRAINT IF EXISTS chk_copy_operation_execution_mode;
ALTER TABLE futuros_operaciones.copy_operation
    ADD CONSTRAINT chk_copy_operation_execution_mode
    CHECK (execution_mode IN ('LIVE', 'SHADOW', 'MICRO_LIVE'));

ALTER TABLE futuros_operaciones.copy_operation_event
    DROP CONSTRAINT IF EXISTS chk_copy_operation_event_execution_mode;
ALTER TABLE futuros_operaciones.copy_operation_event
    ADD CONSTRAINT chk_copy_operation_event_execution_mode
    CHECK (execution_mode IN ('LIVE', 'SHADOW', 'MICRO_LIVE'));

DROP INDEX IF EXISTS futuros_operaciones.ix_user_copy_allocation_wallet_profile_active;
CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_wallet_profile_active
    ON futuros_operaciones.user_copy_allocation (wallet_profile_id, id_user)
    WHERE ends_at IS NULL
      AND is_active = true
      AND execution_mode IN ('LIVE', 'MICRO_LIVE');
