-- Refuerza que SHADOW se trate como wallet + estrategia/scope y que la idempotencia sea por perfil.
-- El mismo delta puede alimentar perfiles distintos, pero no debe duplicarse dentro del mismo perfil.

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

CREATE UNIQUE INDEX IF NOT EXISTS ux_shadow_copy_operation_event_profile_idempotency
    ON futuros_operaciones.shadow_copy_operation_event (
        shadow_allocation_id,
        id_order_origin,
        event_type,
        position_side,
        event_time
    );

CREATE INDEX IF NOT EXISTS ix_shadow_position_state_profile_open
    ON futuros_operaciones.shadow_position_state (
        shadow_allocation_id,
        parsymbol,
        position_side,
        status
    )
    WHERE status = 'OPEN';
