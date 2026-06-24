-- Refuerza trazabilidad y lookup rapido para el worker SHADOW asincrono.

ALTER TABLE futuros_operaciones.shadow_copy_operation_event
    ADD COLUMN IF NOT EXISTS shadow_position_id uuid;

CREATE INDEX IF NOT EXISTS ix_shadow_event_position_time
    ON futuros_operaciones.shadow_copy_operation_event (shadow_position_id, event_time DESC)
    WHERE shadow_position_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_shadow_operation_wallet_profile_position_active
    ON futuros_operaciones.shadow_copy_operation (wallet_profile_id, parsymbol, type_operation, date_creation DESC)
    WHERE wallet_profile_id IS NOT NULL AND is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_operation_allocation_position_active
    ON futuros_operaciones.shadow_copy_operation (shadow_allocation_id, parsymbol, type_operation, date_creation DESC)
    WHERE is_active = true;

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_wallet_profile_closed_position
    ON futuros_operaciones.shadow_position_state (wallet_profile_id, parsymbol, position_side, closed_at DESC)
    WHERE wallet_profile_id IS NOT NULL AND status = 'CLOSED';

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_allocation_closed_position
    ON futuros_operaciones.shadow_position_state (shadow_allocation_id, parsymbol, position_side, closed_at DESC)
    WHERE status = 'CLOSED';
