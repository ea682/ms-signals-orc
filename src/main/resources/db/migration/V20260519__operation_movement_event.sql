-- Ledger auditable de TODOS los movimientos de operaciones originales.
-- Objetivo: que metricas y consultas de rentabilidad no dependan solo de copy_operation_event.
-- Se graba asincronicamente desde el hot path; si esta cola falla, no bloquea el copiado.

-- PostgreSQL 16 trae gen_random_uuid en pg_catalog; no se requiere pgcrypto.

CREATE TABLE IF NOT EXISTS futuros_operaciones.operation_movement_event (
    id_event uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    id_order_origin uuid NOT NULL,
    movement_key varchar(600) NOT NULL,
    idempotency_key varchar(600) NULL,
    position_key varchar(300) NOT NULL,
    id_wallet_origin varchar(180) NOT NULL,
    parsymbol varchar(40) NOT NULL,
    type_operation varchar(20) NOT NULL,
    event_type varchar(30) NOT NULL,
    delta_type varchar(30) NOT NULL,
    source_event_type varchar(30) NULL,
    status varchar(30) NULL,
    size_qty numeric(38,18) NULL,
    signed_size_qty numeric(38,18) NULL,
    previous_size_qty numeric(38,18) NULL,
    resulting_size_qty numeric(38,18) NULL,
    delta_size_qty numeric(38,18) NULL,
    notional_usd numeric(38,18) NULL,
    margin_used_usd numeric(38,18) NULL,
    entry_price numeric(38,18) NULL,
    mark_price numeric(38,18) NULL,
    exit_price numeric(38,18) NULL,
    realized_pnl_usd numeric(38,18) NULL,
    leverage numeric(38,18) NULL,
    wallet_version bigint NULL,
    snapshot_version bigint NULL,
    source_ts timestamptz NULL,
    detected_at timestamptz NULL,
    published_at timestamptz NULL,
    event_time timestamptz NOT NULL,
    trace_id varchar(128) NULL,
    source varchar(80) NULL,
    reason_code varchar(120) NULL,
    copy_eligible_users integer NULL,
    copy_submitted_tasks integer NULL,
    copy_business_skipped integer NULL,
    copy_fallback_jobs integer NULL,
    copy_fallback_used boolean NULL,
    raw jsonb NULL,
    date_creation timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_operation_movement_event_type CHECK (event_type IN ('OPEN', 'INCREASE', 'REDUCE', 'CLOSE', 'FLIP', 'UPDATE', 'NO_CHANGE', 'UNKNOWN')),
    CONSTRAINT chk_operation_movement_qty_non_negative CHECK (
        (size_qty IS NULL OR size_qty >= 0) AND
        (previous_size_qty IS NULL OR previous_size_qty >= 0) AND
        (resulting_size_qty IS NULL OR resulting_size_qty >= 0)
    )
);

-- Idempotencia: un movimiento de origen no puede duplicarse aunque llegue por hot path y fallback.
CREATE UNIQUE INDEX IF NOT EXISTS ux_operation_movement_event_movement_key
    ON futuros_operaciones.operation_movement_event (movement_key);

-- Timeline completo de una operacion original.
CREATE INDEX IF NOT EXISTS ix_operation_movement_event_origin_time
    ON futuros_operaciones.operation_movement_event (id_order_origin, event_time DESC);

-- Timeline por wallet/simbolo/side, clave para reconstruir ajustes, resize, flip y cierres.
CREATE INDEX IF NOT EXISTS ix_operation_movement_event_wallet_symbol_side_time
    ON futuros_operaciones.operation_movement_event (id_wallet_origin, parsymbol, type_operation, event_time DESC);

-- Reconciliacion por posicion tecnica de Hyperliquid/fallback.
CREATE INDEX IF NOT EXISTS ix_operation_movement_event_position_time
    ON futuros_operaciones.operation_movement_event (position_key, event_time DESC, date_creation DESC);

-- Busqueda por traceId compartido con Loki.
CREATE INDEX IF NOT EXISTS ix_operation_movement_event_trace_id
    ON futuros_operaciones.operation_movement_event (trace_id)
    WHERE trace_id IS NOT NULL;

-- Reportes y paneles por fecha/tipo.
CREATE INDEX IF NOT EXISTS ix_operation_movement_event_type_time
    ON futuros_operaciones.operation_movement_event (event_type, delta_type, event_time DESC);

CREATE INDEX IF NOT EXISTS ix_operation_movement_event_reason_time
    ON futuros_operaciones.operation_movement_event (reason_code, event_time DESC)
    WHERE reason_code IS NOT NULL;

COMMENT ON TABLE futuros_operaciones.operation_movement_event IS 'Ledger de movimientos de operaciones originales; complementa copy_operation_event y evita perder trazabilidad de ajustes/resize/flip/cierres no copiados.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.movement_key IS 'Llave idempotente del movimiento original; usa idempotency_key de Hyperliquid cuando existe y fallback deterministico cuando no.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.realized_pnl_usd IS 'PnL realizado inferido para cierres/reducciones cuando hay precio suficiente; el raw jsonb conserva el payload original para auditoria.';
COMMENT ON COLUMN futuros_operaciones.operation_movement_event.copy_submitted_tasks IS 'Cuantas tareas de copia se enviaron para este movimiento; permite saber si un movimiento original tuvo impacto en Binance.';

-- Vista simple para comparar rentabilidad original vs copia por dia/simbolo.
-- Nota: copy_operation_event.realized_pnl_usd refleja fills reales de Binance; operation_movement_event.realized_pnl_usd puede ser inferido si el exchange no mando fill exacto.
CREATE OR REPLACE VIEW futuros_operaciones.operation_copy_pnl_daily_v AS
SELECT
    day,
    id_wallet_origin,
    parsymbol,
    type_operation,
    sum(origin_realized_pnl_usd) AS origin_realized_pnl_usd,
    sum(copy_realized_pnl_usd) AS copy_realized_pnl_usd,
    sum(copy_fee_usd) AS copy_fee_usd,
    sum(copy_realized_pnl_usd) - sum(copy_fee_usd) AS copy_net_pnl_usd
FROM (
    SELECT
        date_trunc('day', ome.event_time) AS day,
        ome.id_wallet_origin,
        ome.parsymbol,
        ome.type_operation,
        COALESCE(ome.realized_pnl_usd, 0) AS origin_realized_pnl_usd,
        0::numeric AS copy_realized_pnl_usd,
        0::numeric AS copy_fee_usd
    FROM futuros_operaciones.operation_movement_event ome

    UNION ALL

    SELECT
        date_trunc('day', coe.event_time) AS day,
        coe.id_wallet_origin,
        coe.parsymbol,
        coe.type_operation,
        0::numeric AS origin_realized_pnl_usd,
        COALESCE(coe.realized_pnl_usd, 0) AS copy_realized_pnl_usd,
        COALESCE(coe.fee_usd, 0) AS copy_fee_usd
    FROM futuros_operaciones.copy_operation_event coe
) x
GROUP BY day, id_wallet_origin, parsymbol, type_operation;
