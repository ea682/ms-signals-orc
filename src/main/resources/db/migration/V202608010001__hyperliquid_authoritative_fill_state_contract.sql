ALTER TABLE futuros_operaciones.operation_movement_event
    ADD COLUMN IF NOT EXISTS source_previous_position_quantity numeric(38, 18),
    ADD COLUMN IF NOT EXISTS source_resulting_position_quantity numeric(38, 18),
    ADD COLUMN IF NOT EXISTS source_execution_quantity numeric(38, 18),
    ADD COLUMN IF NOT EXISTS source_signed_execution_quantity numeric(38, 18),
    ADD COLUMN IF NOT EXISTS source_delivery_mode varchar(40),
    ADD COLUMN IF NOT EXISTS source_recovered_at timestamptz,
    ADD COLUMN IF NOT EXISTS economic_basis_status varchar(40),
    ADD COLUMN IF NOT EXISTS metric_eligible boolean;

COMMENT ON COLUMN futuros_operaciones.operation_movement_event.source_previous_position_quantity IS
    'Authoritative signed position quantity immediately before an individual Hyperliquid USER_FILL.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.source_resulting_position_quantity IS
    'Authoritative signed position quantity immediately after an individual Hyperliquid USER_FILL.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.source_execution_quantity IS
    'Absolute authoritative execution quantity for the individual Hyperliquid USER_FILL.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.source_signed_execution_quantity IS
    'Signed authoritative execution quantity for the individual Hyperliquid USER_FILL.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.source_delivery_mode IS
    'Source delivery semantics: LIVE_USER_FILL, GAP_RECOVERY or HISTORICAL_REPLAY.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.economic_basis_status IS
    'Signals validation result for the authoritative economic state contract.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.metric_eligible IS
    'Durable decision indicating whether this movement may feed wallet metrics.';
