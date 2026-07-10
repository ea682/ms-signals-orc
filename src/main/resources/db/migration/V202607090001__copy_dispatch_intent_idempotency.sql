-- Durable idempotency boundary for real MICRO_LIVE/LIVE Binance dispatches.
-- Additive migration: the new table is empty at creation, so index lock time is short.
CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_dispatch_intent (
    id uuid PRIMARY KEY,
    idempotency_key varchar(64) NOT NULL,
    id_user varchar(80) NOT NULL,
    user_copy_allocation_id bigint,
    execution_mode varchar(16) NOT NULL,
    wallet_id varchar(180),
    strategy_code varchar(64),
    scope_type varchar(32),
    scope_value varchar(180),
    source_event_id varchar(600) NOT NULL,
    id_order_origin varchar(120),
    source_event_type varchar(40),
    copy_intent varchar(40) NOT NULL,
    symbol varchar(40) NOT NULL,
    side varchar(12),
    position_side varchar(12),
    reduce_only boolean NOT NULL DEFAULT false,
    requested_qty numeric(38, 18),
    requested_margin_usd numeric(38, 12) NOT NULL DEFAULT 0,
    requested_notional_usd numeric(38, 12) NOT NULL DEFAULT 0,
    reference_price numeric(38, 18),
    requested_leverage integer,
    reserved_position_count smallint NOT NULL DEFAULT 0,
    reservation_status varchar(24) NOT NULL DEFAULT 'UNRESERVED',
    client_order_id varchar(36) NOT NULL,
    binance_order_id bigint,
    binance_status varchar(32),
    executed_qty numeric(38, 18),
    persisted_executed_qty numeric(38, 18) NOT NULL DEFAULT 0,
    average_price numeric(38, 18),
    cumulative_quote_qty numeric(38, 18),
    average_price_status varchar(32) NOT NULL DEFAULT 'NOT_AVAILABLE',
    status varchar(32) NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    reconciliation_attempts integer NOT NULL DEFAULT 0,
    request_hash varchar(64) NOT NULL,
    response_snapshot text,
    last_error_code varchar(80),
    last_error_detail varchar(1000),
    copy_operation_id uuid,
    copy_operation_event_id uuid,
    claimed_by varchar(128),
    created_at timestamp with time zone NOT NULL,
    claimed_at timestamp with time zone,
    sent_at timestamp with time zone,
    acknowledged_at timestamp with time zone,
    filled_at timestamp with time zone,
    persisted_at timestamp with time zone,
    next_reconciliation_at timestamp with time zone,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT ux_copy_dispatch_intent_idempotency UNIQUE (idempotency_key),
    CONSTRAINT chk_copy_dispatch_execution_mode CHECK (execution_mode IN ('MICRO_LIVE', 'LIVE')),
    CONSTRAINT chk_copy_dispatch_reservation_status CHECK (
        reservation_status IN ('UNRESERVED', 'PENDING', 'CONFIRMED', 'RELEASED')
    ),
    CONSTRAINT chk_copy_dispatch_status CHECK (
        status IN (
            'CREATED', 'CLAIMED', 'DISPATCHING', 'ACKNOWLEDGED', 'NEW',
            'PARTIALLY_FILLED', 'FILLED', 'RECONCILING', 'PERSISTENCE_PENDING',
            'PERSISTED', 'REJECTED', 'FAILED_FINAL', 'CANCELLED'
        )
    )
);

-- Keeps local/dev schemas safe when the table was pre-created while iterating the
-- unreleased migration.
ALTER TABLE futuros_operaciones.copy_dispatch_intent
    ADD COLUMN IF NOT EXISTS persisted_executed_qty numeric(38, 18) NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS ix_copy_dispatch_intent_reconciliation
    ON futuros_operaciones.copy_dispatch_intent (status, next_reconciliation_at, updated_at);

CREATE INDEX IF NOT EXISTS ix_copy_dispatch_intent_allocation_budget
    ON futuros_operaciones.copy_dispatch_intent (
        id_user, user_copy_allocation_id, execution_mode, reservation_status
    );

CREATE INDEX IF NOT EXISTS ix_copy_dispatch_intent_client_order
    ON futuros_operaciones.copy_dispatch_intent (client_order_id);

CREATE INDEX IF NOT EXISTS ix_copy_dispatch_intent_source_event
    ON futuros_operaciones.copy_dispatch_intent (source_event_id, user_copy_allocation_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_dispatch_intent_user_binance_order
    ON futuros_operaciones.copy_dispatch_intent (id_user, binance_order_id)
    WHERE binance_order_id IS NOT NULL;

ALTER TABLE futuros_operaciones.copy_operation
    ADD COLUMN IF NOT EXISTS dispatch_intent_id uuid,
    ADD COLUMN IF NOT EXISTS source_event_id varchar(600),
    ADD COLUMN IF NOT EXISTS client_order_id varchar(36),
    ADD COLUMN IF NOT EXISTS price_status varchar(32);

ALTER TABLE futuros_operaciones.copy_operation_event
    ADD COLUMN IF NOT EXISTS dispatch_intent_id uuid,
    ADD COLUMN IF NOT EXISTS price_status varchar(32);

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_dispatch_intent
    ON futuros_operaciones.copy_operation (dispatch_intent_id)
    WHERE dispatch_intent_id IS NOT NULL;

-- Multiple lifecycle events per intent are intentional, so this index is not unique.
CREATE INDEX IF NOT EXISTS ix_copy_operation_event_dispatch_intent
    ON futuros_operaciones.copy_operation_event (dispatch_intent_id)
    WHERE dispatch_intent_id IS NOT NULL;

-- Durable intents can emit multiple partial-fill progress events. Preserve legacy
-- idempotency for rows without an intent and use cumulative progress for new rows.
DROP INDEX IF EXISTS futuros_operaciones.ux_copy_operation_event_client_order_id;
CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_event_dispatch_progress
    ON futuros_operaciones.copy_operation_event (
        dispatch_intent_id, event_type, COALESCE(qty_executed, 0), COALESCE(resulting_qty, 0)
    ) WHERE dispatch_intent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_copy_operation_event_client_order_id
    ON futuros_operaciones.copy_operation_event (client_order_id) WHERE client_order_id IS NOT NULL;

-- Active uniqueness follows the exact allocation. This preserves intentional
-- MOVEMENT_ALL + SHORT_ONLY/LONG_ONLY copies and different strategy scopes.
DROP INDEX IF EXISTS futuros_operaciones.ux_copy_operation_origin_user_type_strategy_active;
DROP INDEX IF EXISTS futuros_operaciones.ux_copy_operation_allocation_origin_type_active;

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_allocation_origin_type_active
    ON futuros_operaciones.copy_operation (
        user_copy_allocation_id, id_order_origin, type_operation
    )
    WHERE is_active = true AND user_copy_allocation_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_legacy_origin_user_type_strategy_active
    ON futuros_operaciones.copy_operation (
        id_order_origin, id_user, type_operation,
        COALESCE(copy_strategy_code, 'MOVEMENT_ALL')
    )
    WHERE is_active = true AND user_copy_allocation_id IS NULL;

COMMENT ON TABLE futuros_operaciones.copy_dispatch_intent IS
    'Durable pre-Binance copy intent, idempotency claim and pending budget reservation.';
COMMENT ON COLUMN futuros_operaciones.copy_dispatch_intent.idempotency_key IS
    'SHA-256 over user, exact allocation, execution mode, strategy scope, immutable source event and copy intent.';
COMMENT ON COLUMN futuros_operaciones.copy_dispatch_intent.reservation_status IS
    'PENDING remains charged during ambiguous/persistence-pending outcomes; CONFIRMED is represented by active copy_operation.';

-- Rollback (manual, only after all non-terminal intents are resolved):
-- DROP INDEX ...; ALTER TABLE ... DROP COLUMN ...; DROP TABLE ...;
-- Application rollback is safe while retaining this additive schema.
