-- Normalized/auditable fields for Hyperliquid movement ledger.
-- Keep raw notional/margin untouched and store canonical close-level values separately.

ALTER TABLE futuros_operaciones.operation_movement_event
    ADD COLUMN IF NOT EXISTS raw_notional_usd numeric(38,18),
    ADD COLUMN IF NOT EXISTS position_notional_usd numeric(38,18),
    ADD COLUMN IF NOT EXISTS closed_notional_usd numeric(38,18),
    ADD COLUMN IF NOT EXISTS closed_margin_used_usd numeric(38,18),
    ADD COLUMN IF NOT EXISTS effective_close_qty numeric(38,18),
    ADD COLUMN IF NOT EXISTS effective_entry_price numeric(38,18),
    ADD COLUMN IF NOT EXISTS effective_exit_price numeric(38,18),
    ADD COLUMN IF NOT EXISTS effective_realized_pnl_usd numeric(38,18),
    ADD COLUMN IF NOT EXISTS normalization_status varchar(40),
    ADD COLUMN IF NOT EXISTS normalization_reason varchar(180);

COMMENT ON COLUMN futuros_operaciones.operation_movement_event.notional_usd IS
    'Raw/legacy notional received from source or old pipeline. Do not assume this is closed trade notional.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.raw_notional_usd IS
    'Notional value as received from the source/payload for audit. May mean position or snapshot notional.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.position_notional_usd IS
    'Open position notional after the movement when available.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.closed_notional_usd IS
    'Canonical close-level notional for REDUCE/CLOSE/FLIP: abs(effective_close_qty) * effective_exit_price.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.closed_margin_used_usd IS
    'Canonical close-level margin: closed_notional_usd / leverage when leverage is known.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.effective_close_qty IS
    'Quantity actually closed/reduced by this movement. Null for non-closing movements.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.effective_entry_price IS
    'Entry price used for canonical movement math.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.effective_exit_price IS
    'Exit/trade price used for canonical movement math.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.effective_realized_pnl_usd IS
    'Canonical realized PnL recovered from entry/exit/qty when raw realized_pnl_usd is absent.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.normalization_status IS
    'Normalization result: NOT_CLOSING, RECOVERED, PARTIAL_RECOVERY, UNRECOVERABLE, or source-provided status.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.normalization_reason IS
    'Short reason explaining how canonical fields were derived.';

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_closed_notional_time
    ON futuros_operaciones.operation_movement_event (event_time DESC, closed_notional_usd)
    WHERE closed_notional_usd IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_normalization_status_time
    ON futuros_operaciones.operation_movement_event (normalization_status, event_time DESC)
    WHERE normalization_status IS NOT NULL;
