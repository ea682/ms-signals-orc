-- Economic execution ledger V3. A copy_operation is one economic cycle only:
-- OPEN -> zero or more adjustments -> CLOSE. A later reopen inserts another
-- copy_operation and therefore another copy_economic_cycle.

CREATE SEQUENCE IF NOT EXISTS futuros_operaciones.copy_economic_cycle_sequence;

CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_economic_cycle (
    cycle_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    copy_operation_id uuid NOT NULL,
    cycle_sequence bigint NOT NULL DEFAULT nextval('futuros_operaciones.copy_economic_cycle_sequence'),
    id_user varchar(120) NOT NULL,
    id_wallet_origin varchar(180) NOT NULL,
    user_copy_allocation_id bigint NULL,
    copy_strategy_code varchar(64) NULL,
    parsymbol varchar(40) NOT NULL,
    position_side varchar(16) NOT NULL,
    execution_mode varchar(16) NOT NULL,
    source_first_event_id varchar(600) NULL,
    source_last_event_id varchar(600) NULL,
    opened_at timestamptz NOT NULL,
    closed_at timestamptz NULL,
    cycle_status varchar(32) NOT NULL DEFAULT 'OPEN',
    economic_data_status varchar(32) NOT NULL DEFAULT 'PENDING_RECONCILIATION',
    gross_realized_pnl numeric(38, 12) NULL,
    net_realized_pnl numeric(38, 12) NULL,
    unrealized_pnl numeric(38, 12) NULL,
    entry_fee numeric(38, 12) NULL,
    exit_fee numeric(38, 12) NULL,
    total_fees numeric(38, 12) NULL,
    funding_paid numeric(38, 12) NULL,
    funding_received numeric(38, 12) NULL,
    net_funding numeric(38, 12) NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fk_copy_economic_cycle_operation
        FOREIGN KEY (copy_operation_id)
        REFERENCES futuros_operaciones.copy_operation (id_operation),
    CONSTRAINT ux_copy_economic_cycle_operation UNIQUE (copy_operation_id),
    CONSTRAINT chk_copy_economic_cycle_status
        CHECK (cycle_status IN ('OPEN', 'CLOSING', 'CLOSED', 'RECONCILING', 'UNAVAILABLE')),
    CONSTRAINT chk_copy_economic_cycle_data_status
        CHECK (economic_data_status IN ('KNOWN', 'PENDING_RECONCILIATION', 'UNAVAILABLE'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_economic_cycle_sequence
    ON futuros_operaciones.copy_economic_cycle (cycle_sequence);

CREATE INDEX IF NOT EXISTS ix_copy_economic_cycle_scope_opened
    ON futuros_operaciones.copy_economic_cycle (
        lower(id_wallet_origin), copy_strategy_code, user_copy_allocation_id,
        parsymbol, position_side, opened_at DESC
    );

ALTER TABLE futuros_operaciones.copy_operation
    ADD COLUMN IF NOT EXISTS economic_cycle_id uuid NULL,
    ADD COLUMN IF NOT EXISTS cycle_sequence bigint NULL,
    ADD COLUMN IF NOT EXISTS economic_data_status varchar(32) NOT NULL DEFAULT 'PENDING_RECONCILIATION';

ALTER TABLE futuros_operaciones.copy_operation
    DROP CONSTRAINT IF EXISTS chk_copy_operation_economic_data_status;
ALTER TABLE futuros_operaciones.copy_operation
    ADD CONSTRAINT chk_copy_operation_economic_data_status
    CHECK (economic_data_status IN ('KNOWN', 'PENDING_RECONCILIATION', 'UNAVAILABLE'));

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_economic_cycle
    ON futuros_operaciones.copy_operation (economic_cycle_id)
    WHERE economic_cycle_id IS NOT NULL;

ALTER TABLE futuros_operaciones.copy_operation_event
    ADD COLUMN IF NOT EXISTS economic_cycle_id uuid NULL,
    ADD COLUMN IF NOT EXISTS execution_intent_id uuid NULL,
    ADD COLUMN IF NOT EXISTS trade_ids jsonb NULL,
    ADD COLUMN IF NOT EXISTS requested_quantity numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS executed_quantity numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS average_fill_price numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS individual_fills jsonb NULL,
    ADD COLUMN IF NOT EXISTS entry_price numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS exit_price numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS entry_fee numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS exit_fee numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS total_fees numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS funding_paid numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS funding_received numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS net_funding numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS gross_realized_pnl numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS net_realized_pnl numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS unrealized_pnl numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS expected_price numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS actual_price numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS slippage_bps numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS slippage_usd numeric(38, 12) NULL,
    ADD COLUMN IF NOT EXISTS submitted_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS accepted_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS filled_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS persisted_at timestamptz NULL,
    ADD COLUMN IF NOT EXISTS source_to_submit_latency_ms bigint NULL,
    ADD COLUMN IF NOT EXISTS submit_to_fill_latency_ms bigint NULL,
    ADD COLUMN IF NOT EXISTS end_to_end_latency_ms bigint NULL,
    ADD COLUMN IF NOT EXISTS economic_data_status varchar(32) NOT NULL DEFAULT 'PENDING_RECONCILIATION',
    ADD COLUMN IF NOT EXISTS strategy_version varchar(64) NULL,
    ADD COLUMN IF NOT EXISTS sizing_policy_version varchar(64) NULL,
    ADD COLUMN IF NOT EXISTS symbol_mapping_version varchar(64) NULL,
    ADD COLUMN IF NOT EXISTS fee_model_version varchar(64) NULL,
    ADD COLUMN IF NOT EXISTS funding_model_version varchar(64) NULL,
    ADD COLUMN IF NOT EXISTS slippage_model_version varchar(64) NULL,
    ADD COLUMN IF NOT EXISTS liquidity_model_version varchar(64) NULL;

UPDATE futuros_operaciones.copy_operation_event
SET execution_intent_id = dispatch_intent_id
WHERE execution_intent_id IS NULL AND dispatch_intent_id IS NOT NULL;

ALTER TABLE futuros_operaciones.copy_operation_event
    DROP CONSTRAINT IF EXISTS chk_copy_operation_event_economic_data_status;
ALTER TABLE futuros_operaciones.copy_operation_event
    ADD CONSTRAINT chk_copy_operation_event_economic_data_status
    CHECK (economic_data_status IN ('KNOWN', 'PENDING_RECONCILIATION', 'UNAVAILABLE'));

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_economic_cycle_time
    ON futuros_operaciones.copy_operation_event (economic_cycle_id, event_time)
    WHERE economic_cycle_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_economic_reconciliation
    ON futuros_operaciones.copy_operation_event (economic_data_status, event_time)
    WHERE economic_data_status = 'PENDING_RECONCILIATION';

COMMENT ON TABLE futuros_operaciones.copy_economic_cycle IS
    'One immutable identity per OPEN-to-CLOSE economic cycle; reopens never reuse copy_operation_id.';
COMMENT ON COLUMN futuros_operaciones.copy_operation_event.economic_data_status IS
    'KNOWN only when authoritative exchange facts are complete. Unknown fee, funding, fill or slippage values stay NULL.';

