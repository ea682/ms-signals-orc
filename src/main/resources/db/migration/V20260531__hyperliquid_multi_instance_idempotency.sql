-- Multi-instance safety for Hyperliquid copy trading.
-- This migration adds a DB-backed idempotency guard before copy dispatch and a unique active-copy guard.
-- It is safe to run under Flyway transactions.

CREATE TABLE IF NOT EXISTS futuros_operaciones.hyperliquid_direct_ingest_dedupe (
    idempotency_key varchar(600) NOT NULL,
    dedupe_key varchar(600) NOT NULL,
    position_key varchar(300) NULL,
    wallet varchar(180) NULL,
    symbol varchar(40) NULL,
    side varchar(20) NULL,
    delta_type varchar(30) NULL,
    source_ts_ms bigint NULL,
    status varchar(30) NOT NULL DEFAULT 'PROCESSING',
    attempt_count integer NOT NULL DEFAULT 1,
    duplicate_count integer NOT NULL DEFAULT 0,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz NULL,
    processed_at timestamptz NULL,
    failed_at timestamptz NULL,
    last_reason_code varchar(120) NULL,
    last_error_class varchar(180) NULL,
    last_error_message varchar(1000) NULL,
    CONSTRAINT hyperliquid_direct_ingest_dedupe_pkey PRIMARY KEY (idempotency_key),
    CONSTRAINT chk_hyperliquid_direct_ingest_dedupe_status CHECK (status IN ('PROCESSING', 'PROCESSED', 'FAILED', 'REJECTED'))
);

CREATE INDEX IF NOT EXISTS ix_hyperliquid_direct_ingest_dedupe_position
    ON futuros_operaciones.hyperliquid_direct_ingest_dedupe (position_key, source_ts_ms DESC)
    WHERE position_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_hyperliquid_direct_ingest_dedupe_wallet_symbol_time
    ON futuros_operaciones.hyperliquid_direct_ingest_dedupe (wallet, symbol, side, source_ts_ms DESC)
    WHERE wallet IS NOT NULL AND symbol IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_hyperliquid_direct_ingest_dedupe_status_lease
    ON futuros_operaciones.hyperliquid_direct_ingest_dedupe (status, lease_until)
    WHERE status = 'PROCESSING';

COMMENT ON TABLE futuros_operaciones.hyperliquid_direct_ingest_dedupe IS
    'Distributed idempotency guard for Hyperliquid direct ingest. Prevents duplicate copy dispatch when multiple sentinels/signals instances receive the same trade.';

COMMENT ON COLUMN futuros_operaciones.hyperliquid_direct_ingest_dedupe.lease_until IS
    'Processing lease. If a replica dies while PROCESSING, another duplicate can reacquire after this timestamp.';

-- Prevent two active copy_operation rows for the same origin/user/side when multiple signals replicas race.
-- Partial index allows a later new active lifecycle after the old copy has been closed.
CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_origin_user_type
    ON futuros_operaciones.copy_operation (id_order_origin, id_user, type_operation)
    WHERE is_active = true;

-- Extra idempotency guard for exact copied order identifiers when present.
-- Binance order ids can be unique per account/user, not necessarily globally across all users.
CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_user_order_id
    ON futuros_operaciones.copy_operation (id_user, id_orden)
    WHERE id_orden IS NOT NULL;
