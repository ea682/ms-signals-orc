CREATE TABLE IF NOT EXISTS futuros_operaciones.shadow_event_dead_letter (
    idempotency_key varchar(600) PRIMARY KEY,
    source_event_id varchar(600),
    position_key varchar(600),
    wallet_id varchar(180),
    symbol varchar(40),
    position_side varchar(12),
    delta_type varchar(40),
    payload_json jsonb NOT NULL,
    error_code varchar(80) NOT NULL,
    error_detail varchar(1000),
    attempt_count integer NOT NULL DEFAULT 1,
    status varchar(24) NOT NULL DEFAULT 'RECOVERABLE',
    first_failed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_failed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    CONSTRAINT chk_shadow_event_dead_letter_attempts CHECK (attempt_count > 0),
    CONSTRAINT chk_shadow_event_dead_letter_status CHECK (
        status IN ('RECOVERABLE', 'REPLAYING', 'RESOLVED', 'DISCARDED')
    )
);

CREATE INDEX IF NOT EXISTS ix_shadow_event_dead_letter_recovery
    ON futuros_operaciones.shadow_event_dead_letter(status, last_failed_at, idempotency_key);

COMMENT ON TABLE futuros_operaciones.shadow_event_dead_letter IS
    'Eventos SHADOW agotados despues de retries PostgreSQL acotados; nunca autoriza dispatch Binance.';
