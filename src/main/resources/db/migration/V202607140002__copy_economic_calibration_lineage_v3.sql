-- Immutable lineage for economic evidence and exact SHADOW/MICRO_LIVE pairing.
-- Existing rows remain valid but stay non-calibratable when lineage is unknown.

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS metric_generation_id VARCHAR(80);

ALTER TABLE futuros_operaciones.shadow_copy_allocation
    ADD COLUMN IF NOT EXISTS metric_generation_id VARCHAR(80);

ALTER TABLE futuros_operaciones.shadow_copy_operation_event
    ADD COLUMN IF NOT EXISTS metric_generation_id VARCHAR(80);

ALTER TABLE futuros_operaciones.copy_dispatch_intent
    ADD COLUMN IF NOT EXISTS metric_generation_id VARCHAR(80),
    ADD COLUMN IF NOT EXISTS notional_band VARCHAR(32);

ALTER TABLE futuros_operaciones.copy_operation_event
    ADD COLUMN IF NOT EXISTS scope_type VARCHAR(32),
    ADD COLUMN IF NOT EXISTS scope_value VARCHAR(180),
    ADD COLUMN IF NOT EXISTS strategy_key VARCHAR(520),
    ADD COLUMN IF NOT EXISTS metric_generation_id VARCHAR(80),
    ADD COLUMN IF NOT EXISTS calibration_capital_usd NUMERIC(38, 12),
    ADD COLUMN IF NOT EXISTS target_leverage NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS calibration_target_notional_usd NUMERIC(38, 12),
    ADD COLUMN IF NOT EXISTS copy_action VARCHAR(32),
    ADD COLUMN IF NOT EXISTS notional_band VARCHAR(32);

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_calibration_lineage_v3
    ON futuros_operaciones.copy_operation_event
    (metric_generation_id, strategy_key, source_movement_key, execution_mode, copy_action, event_time DESC)
    WHERE metric_generation_id IS NOT NULL
      AND strategy_key IS NOT NULL
      AND source_movement_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_shadow_copy_allocation_generation_v3
    ON futuros_operaciones.shadow_copy_allocation (metric_generation_id, strategy_key)
    WHERE metric_generation_id IS NOT NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_generation_v3
    ON futuros_operaciones.user_copy_allocation (metric_generation_id, strategy_key, id_user)
    WHERE metric_generation_id IS NOT NULL AND is_active = true AND ends_at IS NULL;
