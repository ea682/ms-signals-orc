-- Forward-only economic lineage for Hyperliquid fills, funding and position deltas.
-- Legacy columns and raw payloads remain untouched.
ALTER TABLE futuros_operaciones.operation_movement_event
    ADD COLUMN IF NOT EXISTS economic_event_kind varchar(40),
    ADD COLUMN IF NOT EXISTS economic_event_version integer,
    ADD COLUMN IF NOT EXISTS source_event_id varchar(600),
    ADD COLUMN IF NOT EXISTS source_sequence bigint,
    ADD COLUMN IF NOT EXISTS source_fee_usd numeric(38,18),
    ADD COLUMN IF NOT EXISTS funding_pnl_usd numeric(38,18),
    ADD COLUMN IF NOT EXISTS execution_price_basis varchar(80),
    ADD COLUMN IF NOT EXISTS notional_basis varchar(80),
    ADD COLUMN IF NOT EXISTS lifecycle_quality_flags text[],
    ADD COLUMN IF NOT EXISTS source_estimated boolean;

ALTER TABLE futuros_operaciones.operation_movement_event
    DROP CONSTRAINT IF EXISTS operation_movement_event_economic_version_chk;

ALTER TABLE futuros_operaciones.operation_movement_event
    ADD CONSTRAINT operation_movement_event_economic_version_chk
    CHECK (economic_event_version IS NULL OR economic_event_version > 0) NOT VALID;

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_source_economic_id
    ON futuros_operaciones.operation_movement_event (source, economic_event_kind, source_event_id)
    WHERE source_event_id IS NOT NULL;

COMMENT ON COLUMN futuros_operaciones.operation_movement_event.economic_event_kind IS
    'Versioned semantic unit such as USER_FILL, USER_FUNDING or POSITION_DELTA.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.source_event_id IS
    'Stable source identity. Transport timestamps and replica identity must not participate.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.source_fee_usd IS
    'Source fee kept separate from trading realized PnL and copy fee scenarios.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.funding_pnl_usd IS
    'Source funding kept separate from fill/trading PnL.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.notional_basis IS
    'Declares whether a value is executed notional or a position snapshot.';
