ALTER TABLE futuros_operaciones.user_copy_allocation
    ALTER COLUMN allocation_pct DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS sizing_mode varchar(32),
    ADD COLUMN IF NOT EXISTS allocation_pct_source varchar(80),
    ADD COLUMN IF NOT EXISTS allocation_pct_source_id uuid,
    ADD COLUMN IF NOT EXISTS allocation_pct_calculated_at timestamptz,
    ADD COLUMN IF NOT EXISTS allocation_pct_valid_until timestamptz,
    ADD COLUMN IF NOT EXISTS wallet_total_allocation_pct numeric(9, 6);

ALTER TABLE futuros_operaciones.user_wallet_copy_plan
    ALTER COLUMN allocation_pct DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS sizing_mode varchar(32),
    ADD COLUMN IF NOT EXISTS allocation_pct_source varchar(80),
    ADD COLUMN IF NOT EXISTS allocation_pct_source_id uuid,
    ADD COLUMN IF NOT EXISTS allocation_pct_calculated_at timestamptz,
    ADD COLUMN IF NOT EXISTS allocation_pct_valid_until timestamptz,
    ADD COLUMN IF NOT EXISTS wallet_total_allocation_pct numeric(9, 6);

CREATE TABLE IF NOT EXISTS futuros_operaciones.live_allocation_distribution_run (
    distribution_id uuid PRIMARY KEY,
    id_user uuid NOT NULL,
    source varchar(80) NOT NULL,
    status varchar(24) NOT NULL,
    reason_code varchar(80),
    user_total_allocation_pct numeric(9, 6) NOT NULL,
    calculated_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_live_allocation_distribution_run_status
        CHECK (status IN ('STAGED', 'COMPLETED', 'FAILED')),
    CONSTRAINT chk_live_allocation_distribution_run_total
        CHECK (user_total_allocation_pct >= 0 AND user_total_allocation_pct <= 1),
    CONSTRAINT chk_live_allocation_distribution_run_validity
        CHECK (valid_until > calculated_at)
);

CREATE TABLE IF NOT EXISTS futuros_operaciones.live_allocation_distribution_detail (
    id bigserial PRIMARY KEY,
    distribution_id uuid NOT NULL
        REFERENCES futuros_operaciones.live_allocation_distribution_run(distribution_id) ON DELETE CASCADE,
    id_user uuid NOT NULL,
    wallet_lc varchar(128) NOT NULL,
    strategy_code varchar(64) NOT NULL,
    scope_type varchar(32) NOT NULL,
    scope_value varchar(160) NOT NULL,
    strategy_allocation_pct numeric(9, 6) NOT NULL,
    wallet_total_allocation_pct numeric(9, 6) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_live_allocation_distribution_profile
        UNIQUE (distribution_id, id_user, wallet_lc, strategy_code, scope_type, scope_value),
    CONSTRAINT chk_live_allocation_distribution_detail_pct
        CHECK (
            strategy_allocation_pct > 0
            AND strategy_allocation_pct <= 1
            AND wallet_total_allocation_pct > 0
            AND wallet_total_allocation_pct <= 1
            AND strategy_allocation_pct <= wallet_total_allocation_pct
        )
);

CREATE INDEX IF NOT EXISTS ix_live_allocation_distribution_run_latest
    ON futuros_operaciones.live_allocation_distribution_run (id_user, calculated_at DESC, distribution_id DESC)
    WHERE status = 'COMPLETED';

CREATE INDEX IF NOT EXISTS ix_live_allocation_distribution_detail_exact
    ON futuros_operaciones.live_allocation_distribution_detail (
        distribution_id, id_user, wallet_lc, strategy_code, scope_type, scope_value
    );

-- Fail closed only for LIVE rows whose provenance matches the old MICRO sentinel.
UPDATE futuros_operaciones.user_copy_allocation live
SET status = 'paused_by_risk',
    status_reason = 'LEGACY_LIVE_ALLOCATION_PCT_INVALID',
    status_updated_at = now(),
    updated_at = now(),
    sizing_mode = 'PERCENTAGE',
    allocation_pct_source = 'LEGACY_MICRO_LIVE_SENTINEL'
WHERE coalesce(live.execution_mode, 'LIVE') = 'LIVE'
  AND live.is_active = true
  AND live.ends_at IS NULL
  AND lower(live.status) = 'active'
  AND live.allocation_pct = 0.000001
  AND coalesce(live.allocation_pct_source, '') NOT IN ('SIGNALS_CURRENT_LIVE_DISTRIBUTION')
  AND (
      EXISTS (
          SELECT 1
          FROM futuros_operaciones.shadow_copy_allocation shadow
          WHERE shadow.id = live.linked_shadow_allocation_id
            AND coalesce(shadow.allocation_pct, 0) <= 0
            AND shadow.target_live_allocation_pct IS NULL
      )
      OR EXISTS (
          SELECT 1
          FROM futuros_operaciones.user_wallet_copy_plan plan
          WHERE plan.id_user = live.id_user
            AND plan.wallet_lc = lower(live.wallet_id)
            AND plan.allocation_pct = 0.000001
            AND plan.allocated_capital_usd = 100
      )
  );

UPDATE futuros_operaciones.user_copy_allocation live
SET status = 'paused_by_risk',
    status_reason = 'LEGACY_LIVE_ALLOCATION_PCT_INVALID',
    status_updated_at = now(),
    updated_at = now()
WHERE coalesce(live.execution_mode, 'LIVE') = 'LIVE'
  AND live.is_active = true
  AND live.ends_at IS NULL
  AND lower(live.status) = 'active'
  AND (live.allocation_pct IS NULL OR live.allocation_pct <= 0);

-- Convert only MICRO_LIVE rows proven to come from the historical fallback.
WITH safe_micro_sentinel AS (
    SELECT micro.id
    FROM futuros_operaciones.user_copy_allocation micro
    JOIN futuros_operaciones.shadow_copy_allocation shadow
      ON shadow.id = micro.linked_shadow_allocation_id
    WHERE micro.execution_mode = 'MICRO_LIVE'
      AND micro.allocation_pct = 0.000001
      AND coalesce(shadow.allocation_pct, 0) <= 0
      AND shadow.target_live_allocation_pct IS NULL
      AND micro.promoted_from_shadow_at IS NOT NULL
)
UPDATE futuros_operaciones.user_copy_allocation micro
SET allocation_pct = NULL,
    sizing_mode = 'FIXED_CAPITAL',
    allocation_pct_source = 'FIXED_MICRO_BUDGET',
    allocation_pct_source_id = NULL,
    allocation_pct_calculated_at = NULL,
    allocation_pct_valid_until = NULL,
    wallet_total_allocation_pct = NULL,
    updated_at = now()
FROM safe_micro_sentinel safe
WHERE micro.id = safe.id;

-- Migrate the corresponding wallet plan only when no active LIVE allocation owns it.
UPDATE futuros_operaciones.user_wallet_copy_plan plan
SET allocation_pct = NULL,
    sizing_mode = 'FIXED_CAPITAL',
    allocation_pct_source = 'FIXED_MICRO_BUDGET',
    allocation_pct_source_id = NULL,
    allocation_pct_calculated_at = NULL,
    allocation_pct_valid_until = NULL,
    wallet_total_allocation_pct = NULL,
    allocated_capital_usd = 100,
    updated_at = now()
WHERE plan.allocation_pct = 0.000001
  AND plan.allocated_capital_usd = 100
  AND EXISTS (
      SELECT 1
      FROM futuros_operaciones.user_copy_allocation micro
      WHERE micro.id_user = plan.id_user
        AND lower(micro.wallet_id) = plan.wallet_lc
        AND micro.execution_mode = 'MICRO_LIVE'
        AND micro.allocation_pct IS NULL
        AND micro.sizing_mode = 'FIXED_CAPITAL'
        AND micro.allocation_pct_source = 'FIXED_MICRO_BUDGET'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM futuros_operaciones.user_copy_allocation live
      WHERE live.id_user = plan.id_user
        AND lower(live.wallet_id) = plan.wallet_lc
        AND live.execution_mode = 'LIVE'
        AND live.is_active = true
        AND live.ends_at IS NULL
        AND lower(live.status) = 'active'
  );

UPDATE futuros_operaciones.user_copy_allocation
SET sizing_mode = 'FIXED_CAPITAL',
    allocation_pct_source = CASE
        WHEN allocation_pct IS NULL THEN 'FIXED_MICRO_BUDGET'
        ELSE 'LEGACY_MICRO_PCT_IGNORED'
    END
WHERE execution_mode = 'MICRO_LIVE'
  AND (sizing_mode IS NULL OR allocation_pct_source IS NULL);

UPDATE futuros_operaciones.user_copy_allocation
SET sizing_mode = 'PERCENTAGE',
    allocation_pct_source = coalesce(allocation_pct_source, 'LEGACY_ECONOMIC_ALLOCATION')
WHERE coalesce(execution_mode, 'LIVE') = 'LIVE'
  AND allocation_pct IS NOT NULL
  AND allocation_pct > 0
  AND (sizing_mode IS NULL OR allocation_pct_source IS NULL);

UPDATE futuros_operaciones.user_copy_allocation
SET sizing_mode = coalesce(sizing_mode, 'SIMULATION'),
    allocation_pct_source = coalesce(allocation_pct_source, 'SHADOW_SIMULATION')
WHERE execution_mode = 'SHADOW';

UPDATE futuros_operaciones.user_wallet_copy_plan
SET sizing_mode = CASE
        WHEN sizing_mode IS NOT NULL THEN sizing_mode
        WHEN allocation_pct IS NULL THEN 'NOT_CALCULATED'
        WHEN allocation_pct > 0 THEN 'PERCENTAGE'
        ELSE 'NOT_CALCULATED'
    END,
    allocation_pct_source = CASE
        WHEN allocation_pct_source IS NOT NULL THEN allocation_pct_source
        WHEN allocation_pct > 0 THEN 'LEGACY_PLAN_ECONOMIC_ALLOCATION'
        ELSE 'NOT_CALCULATED'
    END;

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS chk_user_copy_allocation_pct_contract,
    DROP CONSTRAINT IF EXISTS chk_user_copy_allocation_sizing_mode,
    DROP CONSTRAINT IF EXISTS chk_user_copy_allocation_micro_pct_contract,
    DROP CONSTRAINT IF EXISTS chk_user_copy_allocation_active_live_pct_contract;

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT chk_user_copy_allocation_pct_contract
        CHECK (
            (allocation_pct IS NULL OR (allocation_pct >= 0 AND allocation_pct <= 1))
            AND (wallet_total_allocation_pct IS NULL
                OR (wallet_total_allocation_pct > 0 AND wallet_total_allocation_pct <= 1))
            AND (allocation_pct IS NULL OR wallet_total_allocation_pct IS NULL
                OR allocation_pct <= wallet_total_allocation_pct)
        ),
    ADD CONSTRAINT chk_user_copy_allocation_sizing_mode
        CHECK (sizing_mode IS NULL OR sizing_mode IN ('SIMULATION', 'FIXED_CAPITAL', 'PERCENTAGE')),
    ADD CONSTRAINT chk_user_copy_allocation_micro_pct_contract
        CHECK (
            execution_mode <> 'MICRO_LIVE'
            OR (
                sizing_mode = 'FIXED_CAPITAL'
                AND (
                    (allocation_pct IS NULL AND allocation_pct_source = 'FIXED_MICRO_BUDGET')
                    OR (allocation_pct IS NOT NULL AND allocation_pct_source = 'LEGACY_MICRO_PCT_IGNORED')
                )
            )
        ),
    ADD CONSTRAINT chk_user_copy_allocation_active_live_pct_contract
        CHECK (
            coalesce(execution_mode, 'LIVE') <> 'LIVE'
            OR is_active = false
            OR ends_at IS NOT NULL
            OR lower(status) <> 'active'
            OR (
                allocation_pct > 0
                AND allocation_pct <= 1
                AND sizing_mode = 'PERCENTAGE'
                AND allocation_pct_source IS NOT NULL
                AND allocation_pct_source NOT IN (
                    'FIXED_MICRO_BUDGET',
                    'LEGACY_MICRO_LIVE_SENTINEL',
                    'LEGACY_MICRO_PCT_IGNORED'
                )
                AND (
                    allocation_pct_source <> 'SIGNALS_CURRENT_LIVE_DISTRIBUTION'
                    OR (
                        allocation_pct_source_id IS NOT NULL
                        AND allocation_pct_calculated_at IS NOT NULL
                        AND allocation_pct_valid_until > allocation_pct_calculated_at
                        AND wallet_total_allocation_pct IS NOT NULL
                        AND allocation_pct <= wallet_total_allocation_pct
                    )
                )
            )
        );

ALTER TABLE futuros_operaciones.user_wallet_copy_plan
    DROP CONSTRAINT IF EXISTS chk_user_wallet_copy_plan_pct,
    DROP CONSTRAINT IF EXISTS chk_user_wallet_copy_plan_pct_contract,
    DROP CONSTRAINT IF EXISTS chk_user_wallet_copy_plan_sizing_contract;

ALTER TABLE futuros_operaciones.user_wallet_copy_plan
    ADD CONSTRAINT chk_user_wallet_copy_plan_pct_contract
        CHECK (
            (allocation_pct IS NULL OR (allocation_pct >= 0 AND allocation_pct <= 1))
            AND (wallet_total_allocation_pct IS NULL
                OR (wallet_total_allocation_pct > 0 AND wallet_total_allocation_pct <= 1))
        ),
    ADD CONSTRAINT chk_user_wallet_copy_plan_sizing_contract
        CHECK (
            sizing_mode IN ('FIXED_CAPITAL', 'PERCENTAGE', 'NOT_CALCULATED')
            AND (sizing_mode <> 'FIXED_CAPITAL' OR allocation_pct IS NULL)
            AND (sizing_mode <> 'PERCENTAGE' OR allocation_pct > 0)
        );
