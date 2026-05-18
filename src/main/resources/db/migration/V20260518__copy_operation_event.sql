-- Historial auditable de movimientos reales enviados a Binance para una copy_operation.
-- Mantiene separado el estado agregado (copy_operation) del ledger de fills (copy_operation_event).
-- Ejecutar en el esquema futuros_operaciones.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_operation_event (
    id_event uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    id_operation uuid NULL,
    id_order_origin varchar(120) NOT NULL,
    id_user varchar(120) NOT NULL,
    id_wallet_origin varchar(180) NOT NULL,
    parsymbol varchar(40) NOT NULL,
    type_operation varchar(20) NOT NULL,
    event_type varchar(30) NOT NULL,
    copy_intent varchar(30) NULL,
    binance_order_id varchar(80) NULL,
    client_order_id varchar(36) NULL,
    side varchar(16) NULL,
    position_side varchar(16) NULL,
    qty_requested numeric(38, 12) NULL,
    qty_executed numeric(38, 12) NULL,
    price numeric(38, 12) NULL,
    notional_usd numeric(38, 12) NULL,
    previous_qty numeric(38, 12) NULL,
    resulting_qty numeric(38, 12) NULL,
    realized_pnl_usd numeric(38, 12) NULL,
    fee_usd numeric(38, 12) NULL,
    trace_id varchar(128) NULL,
    source varchar(80) NULL,
    reason_code varchar(120) NULL,
    event_time timestamptz NOT NULL,
    date_creation timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_copy_operation_event_type CHECK (event_type IN ('OPEN', 'REOPEN', 'INCREASE', 'REDUCE', 'CLOSE', 'PANIC_CLOSE', 'RECONCILE')),
    CONSTRAINT chk_copy_operation_event_qty_non_negative CHECK (
        (qty_requested IS NULL OR qty_requested >= 0) AND
        (qty_executed IS NULL OR qty_executed >= 0) AND
        (previous_qty IS NULL OR previous_qty >= 0) AND
        (resulting_qty IS NULL OR resulting_qty >= 0)
    )
);

-- Idempotencia de órdenes Binance. Permite NULL porque no todos los eventos históricos antiguos tendrán clientOrderId.
CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_operation_event_client_order_id
    ON futuros_operaciones.copy_operation_event (client_order_id)
    WHERE client_order_id IS NOT NULL;

-- Consultas de historial por operación copiada/origen.
CREATE INDEX IF NOT EXISTS ix_copy_operation_event_origin_user_time
    ON futuros_operaciones.copy_operation_event (id_order_origin, id_user, event_time DESC);

-- Consultas de historial y PnL por usuario/símbolo.
CREATE INDEX IF NOT EXISTS ix_copy_operation_event_user_symbol_time
    ON futuros_operaciones.copy_operation_event (id_user, parsymbol, event_time DESC);

-- Seguimiento completo de flujo en Loki/DB por traceId.
CREATE INDEX IF NOT EXISTS ix_copy_operation_event_trace_id
    ON futuros_operaciones.copy_operation_event (trace_id)
    WHERE trace_id IS NOT NULL;

-- Reportes por fecha.
CREATE INDEX IF NOT EXISTS ix_copy_operation_event_time
    ON futuros_operaciones.copy_operation_event (event_time DESC);

-- Conciliación con Binance.
CREATE INDEX IF NOT EXISTS ix_copy_operation_event_binance_order_id
    ON futuros_operaciones.copy_operation_event (binance_order_id)
    WHERE binance_order_id IS NOT NULL;

-- Consultas directas por copy_operation agregada.
CREATE INDEX IF NOT EXISTS ix_copy_operation_event_operation_time
    ON futuros_operaciones.copy_operation_event (id_operation, event_time DESC)
    WHERE id_operation IS NOT NULL;
