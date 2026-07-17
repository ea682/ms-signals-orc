CREATE TABLE IF NOT EXISTS copy_simulation_job_v3 (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(768) NOT NULL UNIQUE,
    input_hash CHAR(64) NOT NULL,
    source_event_id VARCHAR(180) NOT NULL,
    source_snapshot_version BIGINT NOT NULL,
    allocation_id BIGINT,
    user_id VARCHAR(80) NOT NULL,
    wallet_id VARCHAR(160) NOT NULL,
    strategy_code VARCHAR(80) NOT NULL,
    strategy_version VARCHAR(80) NOT NULL,
    scope_type VARCHAR(40) NOT NULL,
    scope_value VARCHAR(180) NOT NULL,
    sizing_policy_version VARCHAR(80) NOT NULL,
    symbol_mapping_version VARCHAR(80) NOT NULL,
    execution_mode VARCHAR(20) NOT NULL,
    input_snapshot JSONB NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
    resume_cursor INTEGER NOT NULL DEFAULT 0,
    pause_requested BOOLEAN NOT NULL DEFAULT FALSE,
    attempt INTEGER NOT NULL DEFAULT 0,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(160),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT copy_simulation_job_v3_mode_chk CHECK (execution_mode = 'MICRO_LIVE'),
    CONSTRAINT copy_simulation_job_v3_status_chk CHECK (
        status IN ('PENDING', 'RUNNING', 'PAUSED', 'COMPLETED', 'FAILED')
    ),
    CONSTRAINT copy_simulation_job_v3_cursor_chk CHECK (resume_cursor BETWEEN 0 AND 40)
);

CREATE INDEX IF NOT EXISTS ix_copy_simulation_job_v3_claim
    ON copy_simulation_job_v3 (next_run_at, created_at, id)
    WHERE status = 'PENDING' AND pause_requested = FALSE;

CREATE INDEX IF NOT EXISTS ix_copy_simulation_job_v3_identity
    ON copy_simulation_job_v3 (
        lower(wallet_id), strategy_code, scope_type, scope_value,
        source_event_id, source_snapshot_version
    );

CREATE TABLE IF NOT EXISTS copy_capital_leverage_simulation_v3 (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES copy_simulation_job_v3(id) ON DELETE RESTRICT,
    scenario_index INTEGER NOT NULL,
    capital_usd NUMERIC(38, 8) NOT NULL,
    target_leverage NUMERIC(12, 4) NOT NULL,
    target_notional_usd NUMERIC(38, 12) NOT NULL,
    target_margin_usd NUMERIC(38, 12) NOT NULL,
    positions_copied INTEGER NOT NULL,
    positions_omitted INTEGER NOT NULL,
    movement_coverage NUMERIC(20, 12) NOT NULL,
    notional_coverage NUMERIC(20, 12) NOT NULL,
    exposure_coverage NUMERIC(20, 12) NOT NULL,
    rounding_loss_usd NUMERIC(38, 12) NOT NULL,
    min_notional_skips INTEGER NOT NULL,
    fees_usd NUMERIC(38, 12),
    funding_usd NUMERIC(38, 12),
    slippage_usd NUMERIC(38, 12),
    gross_pnl_usd NUMERIC(38, 12),
    net_pnl_usd NUMERIC(38, 12),
    drawdown_pct NUMERIC(20, 12),
    profit_factor NUMERIC(20, 12),
    liquidation_risk NUMERIC(20, 12),
    modeled_economics_status VARCHAR(32) NOT NULL,
    target_portfolio JSONB NOT NULL,
    simulation_only BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT copy_capital_leverage_simulation_v3_index_chk CHECK (scenario_index BETWEEN 0 AND 39),
    CONSTRAINT copy_capital_leverage_simulation_v3_only_chk CHECK (simulation_only),
    UNIQUE (job_id, capital_usd, target_leverage),
    UNIQUE (job_id, scenario_index)
);

CREATE INDEX IF NOT EXISTS ix_copy_capital_leverage_simulation_v3_band
    ON copy_capital_leverage_simulation_v3 (capital_usd, target_leverage, job_id);

CREATE TABLE IF NOT EXISTS copy_liquidity_job_v3 (
    id UUID PRIMARY KEY,
    capital_scenario_id BIGINT NOT NULL
        REFERENCES copy_capital_leverage_simulation_v3(id) ON DELETE CASCADE,
    symbol VARCHAR(40) NOT NULL,
    position_side VARCHAR(8) NOT NULL,
    requested_notional_usd NUMERIC(38, 12) NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
    attempt INTEGER NOT NULL DEFAULT 0,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(160),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT copy_liquidity_job_v3_side_chk CHECK (position_side IN ('LONG', 'SHORT')),
    CONSTRAINT copy_liquidity_job_v3_status_chk CHECK (
        status IN ('PENDING', 'RUNNING', 'NO_BOOK', 'COMPLETED', 'FAILED')
    ),
    CONSTRAINT copy_liquidity_job_v3_notional_chk CHECK (requested_notional_usd > 0),
    UNIQUE (capital_scenario_id, symbol, position_side)
);

CREATE INDEX IF NOT EXISTS ix_copy_liquidity_job_v3_claim
    ON copy_liquidity_job_v3 (next_run_at, created_at, id)
    WHERE status IN ('PENDING', 'NO_BOOK');

-- Optional per-user limit. NULL means no position-count limit.
ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS user_max_concurrent_positions INTEGER;

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS user_copy_allocation_max_positions_chk;

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT user_copy_allocation_max_positions_chk
    CHECK (user_max_concurrent_positions IS NULL OR user_max_concurrent_positions > 0);

CREATE TABLE IF NOT EXISTS copy_order_book_snapshot_v3 (
    id UUID PRIMARY KEY,
    symbol VARCHAR(40) NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    source VARCHAR(80) NOT NULL,
    sequence_number BIGINT,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL,
    model_version VARCHAR(80) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (exchange, symbol, captured_at, model_version)
);

CREATE TABLE IF NOT EXISTS copy_liquidity_simulation_v3 (
    id BIGSERIAL PRIMARY KEY,
    liquidity_job_id UUID NOT NULL REFERENCES copy_liquidity_job_v3(id) ON DELETE CASCADE,
    capital_scenario_id BIGINT NOT NULL
        REFERENCES copy_capital_leverage_simulation_v3(id) ON DELETE CASCADE,
    order_book_snapshot_id UUID REFERENCES copy_order_book_snapshot_v3(id) ON DELETE RESTRICT,
    execution_strategy VARCHAR(32) NOT NULL,
    status VARCHAR(40) NOT NULL,
    requested_notional_usd NUMERIC(38, 12) NOT NULL,
    filled_notional_usd NUMERIC(38, 12) NOT NULL,
    vwap NUMERIC(38, 18),
    expected_slippage_bps NUMERIC(20, 12),
    depth_consumed_pct NUMERIC(20, 12),
    fill_percentage NUMERIC(20, 12),
    unfilled_notional_usd NUMERIC(38, 12),
    estimated_execution_ms BIGINT,
    market_participation_pct NUMERIC(20, 12),
    estimated_fees_usd NUMERIC(38, 12),
    estimated_funding_usd NUMERIC(38, 12),
    adverse_selection_bps NUMERIC(20, 12),
    source_closed_before_completion BOOLEAN NOT NULL DEFAULT FALSE,
    evidence_level VARCHAR(40) NOT NULL DEFAULT 'SIMULATED',
    model_version VARCHAR(80) NOT NULL,
    assumptions JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT copy_liquidity_simulation_v3_strategy_chk CHECK (
        execution_strategy IN ('SINGLE_MARKET', 'FRAGMENTED', 'TWAP', 'PARTICIPATION_CAP')
    ),
    CONSTRAINT copy_liquidity_simulation_v3_status_chk CHECK (
        status IN ('NO_BOOK', 'INSUFFICIENT_DEPTH', 'ESTIMATED', 'PARTIALLY_REAL_VALIDATED')
    ),
    CONSTRAINT copy_liquidity_simulation_v3_real_evidence_chk CHECK (
        evidence_level <> 'REAL_VALIDATED'
    ),
    UNIQUE (liquidity_job_id, execution_strategy, order_book_snapshot_id)
);

CREATE TABLE IF NOT EXISTS copy_micro_live_calibration_v3 (
    id UUID PRIMARY KEY,
    calibration_key CHAR(64) NOT NULL UNIQUE,
    wallet_id VARCHAR(160) NOT NULL,
    strategy_code VARCHAR(80) NOT NULL,
    strategy_version VARCHAR(80) NOT NULL,
    scope_type VARCHAR(40) NOT NULL,
    scope_value VARCHAR(180) NOT NULL,
    event_set_hash CHAR(64),
    window_started_at TIMESTAMPTZ NOT NULL,
    window_ended_at TIMESTAMPTZ NOT NULL,
    capital_usd NUMERIC(38, 8) NOT NULL,
    target_leverage NUMERIC(12, 4) NOT NULL,
    sizing_policy_version VARCHAR(80) NOT NULL,
    symbol_mapping_version VARCHAR(80) NOT NULL,
    fee_model_version VARCHAR(80) NOT NULL,
    funding_model_version VARCHAR(80) NOT NULL,
    slippage_model_version VARCHAR(80) NOT NULL,
    liquidity_model_version VARCHAR(80) NOT NULL,
    sample_count INTEGER NOT NULL,
    executable_shadow_net_pnl_usd NUMERIC(38, 12),
    micro_live_net_pnl_usd NUMERIC(38, 12),
    pnl_capture_ratio NUMERIC(20, 12),
    fill_error_bps NUMERIC(20, 12),
    fee_error_usd NUMERIC(38, 12),
    slippage_error_bps NUMERIC(20, 12),
    latency_error_ms BIGINT,
    pnl_error_usd NUMERIC(38, 12),
    status VARCHAR(40) NOT NULL,
    confidence_level VARCHAR(24) NOT NULL,
    mismatch_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT copy_micro_live_calibration_v3_band_chk CHECK (capital_usd = 100 AND target_leverage = 5),
    UNIQUE (
        wallet_id, strategy_code, strategy_version, scope_type, scope_value,
        event_set_hash, window_started_at, window_ended_at, capital_usd,
        target_leverage, sizing_policy_version, symbol_mapping_version,
        fee_model_version, funding_model_version, slippage_model_version,
        liquidity_model_version
    )
);

CREATE TABLE IF NOT EXISTS strategy_live_certification (
    id UUID PRIMARY KEY,
    creation_key VARCHAR(240) NOT NULL UNIQUE,
    wallet_id VARCHAR(160) NOT NULL,
    strategy_code VARCHAR(80) NOT NULL,
    strategy_version VARCHAR(80) NOT NULL,
    scope_type VARCHAR(40) NOT NULL,
    scope_value VARCHAR(180) NOT NULL,
    capital_band_min NUMERIC(38, 8) NOT NULL,
    capital_band_max NUMERIC(38, 8) NOT NULL,
    target_leverage NUMERIC(12, 4) NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    quote_asset VARCHAR(16) NOT NULL,
    sizing_policy_version VARCHAR(80) NOT NULL,
    symbol_mapping_version VARCHAR(80) NOT NULL,
    fee_model_version VARCHAR(80) NOT NULL,
    funding_model_version VARCHAR(80) NOT NULL,
    slippage_model_version VARCHAR(80) NOT NULL,
    liquidity_model_version VARCHAR(80) NOT NULL,
    evidence_level VARCHAR(40) NOT NULL,
    certification_status VARCHAR(48) NOT NULL,
    evidence_snapshot JSONB NOT NULL,
    automatic_promotion_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 0,
    created_by VARCHAR(160) NOT NULL,
    creation_reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_live_certification_band_chk CHECK (
        capital_band_min >= 0 AND capital_band_max >= capital_band_min
    ),
    CONSTRAINT strategy_live_certification_evidence_chk CHECK (
        evidence_level IN ('SIMULATED', 'MICRO_LIVE_CALIBRATED', 'PARTIALLY_REAL_VALIDATED', 'REAL_VALIDATED')
    ),
    CONSTRAINT strategy_live_certification_status_chk CHECK (
        certification_status IN (
            'SOURCE_SHADOW_VALIDATING', 'EXECUTABLE_SHADOW_VALIDATING',
            'MICRO_LIVE_VALIDATING', 'LIVE_APPROVED', 'LIVE_DEGRADED',
            'SUSPENDED', 'REVOKED'
        )
    ),
    UNIQUE (
        wallet_id, strategy_code, strategy_version, scope_type, scope_value,
        capital_band_min, capital_band_max, target_leverage, exchange, quote_asset,
        sizing_policy_version, symbol_mapping_version, fee_model_version,
        funding_model_version, slippage_model_version, liquidity_model_version
    )
);

CREATE TABLE IF NOT EXISTS strategy_live_certification_audit (
    id UUID PRIMARY KEY,
    certification_id UUID NOT NULL REFERENCES strategy_live_certification(id) ON DELETE RESTRICT,
    transition_key VARCHAR(240) NOT NULL UNIQUE,
    prior_status VARCHAR(48) NOT NULL,
    next_status VARCHAR(48) NOT NULL,
    prior_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    actor VARCHAR(160) NOT NULL,
    reason TEXT NOT NULL,
    evidence_snapshot JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_live_certification_adoption (
    id UUID PRIMARY KEY,
    certification_id UUID NOT NULL REFERENCES strategy_live_certification(id) ON DELETE RESTRICT,
    user_id UUID NOT NULL,
    allocation_id BIGINT NOT NULL,
    validation_status VARCHAR(40) NOT NULL,
    balance_usd NUMERIC(38, 8),
    assigned_capital_usd NUMERIC(38, 8),
    target_leverage NUMERIC(12, 4),
    quote_asset VARCHAR(16),
    observed_margin_mode VARCHAR(24),
    required_margin_mode VARCHAR(24),
    balance_valid BOOLEAN NOT NULL,
    capital_band_valid BOOLEAN NOT NULL,
    leverage_valid BOOLEAN NOT NULL,
    quote_asset_valid BOOLEAN NOT NULL,
    margin_mode_valid BOOLEAN NOT NULL,
    api_permissions_valid BOOLEAN NOT NULL,
    manual_positions_valid BOOLEAN NOT NULL,
    risk_policy_valid BOOLEAN NOT NULL,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    observed_at TIMESTAMPTZ NOT NULL,
    validated_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT user_live_certification_adoption_status_chk CHECK (
        validation_status IN ('VALID', 'REJECTED')
    ),
    CONSTRAINT user_live_certification_adoption_window_chk CHECK (
        expires_at > validated_at
    ),
    UNIQUE (certification_id, user_id, allocation_id)
);

CREATE TABLE IF NOT EXISTS live_allocation_activation_audit (
    id UUID PRIMARY KEY,
    activation_key VARCHAR(240) NOT NULL UNIQUE,
    allocation_id BIGINT NOT NULL,
    certification_id UUID NOT NULL REFERENCES strategy_live_certification(id) ON DELETE RESTRICT,
    user_id UUID NOT NULL,
    prior_mode VARCHAR(16) NOT NULL,
    next_mode VARCHAR(16) NOT NULL,
    actor VARCHAR(160) NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT live_allocation_activation_mode_chk CHECK (
        prior_mode = 'MICRO_LIVE' AND next_mode = 'LIVE'
    )
);
