CREATE INDEX IF NOT EXISTS ix_operation_movement_event_economic_order
    ON futuros_operaciones.operation_movement_event
       (position_key, event_time, source_sequence, movement_key);

COMMENT ON INDEX futuros_operaciones.ix_operation_movement_event_economic_order IS
    'Deterministic predecessor lookup for equal-timestamp source events.';
