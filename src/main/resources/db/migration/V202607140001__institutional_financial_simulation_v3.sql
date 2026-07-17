-- Expands the canonical cold simulation from 40 to 44 scenarios and makes
-- strategy/generation/economic evidence explicit. No real execution is enabled.

ALTER TABLE copy_simulation_job_v3
    ADD COLUMN IF NOT EXISTS strategy_key VARCHAR(520),
    ADD COLUMN IF NOT EXISTS generation_id VARCHAR(120),
    ADD COLUMN IF NOT EXISTS generation_status VARCHAR(24),
    ADD COLUMN IF NOT EXISTS generation_reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE copy_simulation_job_v3
SET strategy_key = lower(btrim(wallet_id)) || '|' || upper(btrim(strategy_code)) || '|'
        || upper(btrim(scope_type)) || '|' || upper(btrim(scope_value))
WHERE strategy_key IS NULL;

UPDATE copy_simulation_job_v3
SET generation_id = 'LEGACY_UNKNOWN',
    generation_status = 'UNKNOWN',
    generation_reason_codes = '["LEGACY_SIMULATION_GENERATION_UNKNOWN"]'::jsonb
WHERE generation_id IS NULL OR generation_status IS NULL;

ALTER TABLE copy_simulation_job_v3
    ALTER COLUMN strategy_key SET NOT NULL,
    ALTER COLUMN generation_id SET NOT NULL,
    ALTER COLUMN generation_status SET NOT NULL;

ALTER TABLE copy_simulation_job_v3
    DROP CONSTRAINT IF EXISTS copy_simulation_job_v3_mode_chk;
ALTER TABLE copy_simulation_job_v3
    ADD CONSTRAINT copy_simulation_job_v3_mode_chk
        CHECK (execution_mode IN ('SHADOW', 'MICRO_LIVE', 'LIVE'));

ALTER TABLE copy_simulation_job_v3
    DROP CONSTRAINT IF EXISTS copy_simulation_job_v3_cursor_chk;
ALTER TABLE copy_simulation_job_v3
    ADD CONSTRAINT copy_simulation_job_v3_cursor_chk CHECK (resume_cursor BETWEEN 0 AND 44);

ALTER TABLE copy_simulation_job_v3
    DROP CONSTRAINT IF EXISTS copy_simulation_job_v3_generation_status_chk;
ALTER TABLE copy_simulation_job_v3
    ADD CONSTRAINT copy_simulation_job_v3_generation_status_chk
        CHECK (generation_status IN ('KNOWN', 'UNKNOWN'));

ALTER TABLE copy_simulation_job_v3
    DROP CONSTRAINT IF EXISTS copy_simulation_job_v3_strategy_key_chk;
ALTER TABLE copy_simulation_job_v3
    ADD CONSTRAINT copy_simulation_job_v3_strategy_key_chk CHECK (
        strategy_key = lower(btrim(wallet_id)) || '|' || upper(btrim(strategy_code)) || '|'
            || upper(btrim(scope_type)) || '|' || upper(btrim(scope_value))
    );

CREATE INDEX IF NOT EXISTS ix_copy_simulation_job_v3_generation_strategy
    ON copy_simulation_job_v3 (generation_id, strategy_key, status, completed_at DESC);

ALTER TABLE copy_capital_leverage_simulation_v3
    DROP CONSTRAINT IF EXISTS copy_capital_leverage_simulation_v3_index_chk;
ALTER TABLE copy_capital_leverage_simulation_v3
    ADD CONSTRAINT copy_capital_leverage_simulation_v3_index_chk
        CHECK (scenario_index BETWEEN 0 AND 43);

ALTER TABLE copy_capital_leverage_simulation_v3
    ALTER COLUMN modeled_economics_status TYPE VARCHAR(64),
    ADD COLUMN IF NOT EXISTS economic_evidence JSONB NOT NULL DEFAULT
        '{"status":"UNKNOWN","reasonCodes":["LEGACY_SCENARIO_ECONOMIC_EVIDENCE_UNKNOWN"]}'::jsonb,
    ADD COLUMN IF NOT EXISTS calculator_version VARCHAR(80) NOT NULL DEFAULT 'copy-target-core-v3',
    ADD COLUMN IF NOT EXISTS policy_version VARCHAR(80) NOT NULL DEFAULT 'institutional-simulation-v3',
    ADD COLUMN IF NOT EXISTS field_availability JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE copy_capital_leverage_simulation_v3
SET modeled_economics_status = 'UNKNOWN_HISTORICAL_EXECUTION_EVIDENCE'
WHERE modeled_economics_status = 'NOT_CALCULATED';

COMMENT ON COLUMN copy_capital_leverage_simulation_v3.economic_evidence IS
    'Turnover, expectancy, fill, latency grid, venue basis, capacities, liquidation and confidence with explicit UNKNOWN reasons.';
