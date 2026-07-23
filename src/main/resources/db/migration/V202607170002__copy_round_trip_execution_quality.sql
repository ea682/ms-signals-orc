CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_round_trip_execution_quality (
    id UUID PRIMARY KEY,
    allocation_id BIGINT NOT NULL REFERENCES futuros_operaciones.user_copy_allocation(id) ON DELETE RESTRICT,
    economic_cycle_id UUID NOT NULL,
    execution_mode VARCHAR(16) NOT NULL,
    position_side VARCHAR(8) NOT NULL,
    status VARCHAR(16) NOT NULL,
    incomplete_reason VARCHAR(96),
    origin_open_price NUMERIC(38, 18),
    origin_close_price NUMERIC(38, 18),
    copy_open_price NUMERIC(38, 18),
    copy_close_price NUMERIC(38, 18),
    origin_return NUMERIC(30, 16),
    copy_return NUMERIC(30, 16),
    execution_drag_bps NUMERIC(24, 8),
    fees_usd NUMERIC(38, 12) NOT NULL DEFAULT 0,
    funding_usd NUMERIC(38, 12) NOT NULL DEFAULT 0,
    latency_ms BIGINT,
    net_tracking_error_bps NUMERIC(24, 8),
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT copy_round_trip_execution_quality_status_chk
        CHECK (status IN ('COMPLETE', 'INCOMPLETE')),
    CONSTRAINT copy_round_trip_execution_quality_side_chk
        CHECK (position_side IN ('LONG', 'SHORT', 'UNKNOWN')),
    CONSTRAINT copy_round_trip_execution_quality_incomplete_chk
        CHECK ((status = 'COMPLETE' AND incomplete_reason IS NULL)
            OR (status = 'INCOMPLETE' AND incomplete_reason IS NOT NULL)),
    UNIQUE (allocation_id, economic_cycle_id)
);

CREATE INDEX IF NOT EXISTS ix_copy_round_trip_quality_complete
    ON futuros_operaciones.copy_round_trip_execution_quality
    (allocation_id, calculated_at DESC)
    WHERE status = 'COMPLETE';
