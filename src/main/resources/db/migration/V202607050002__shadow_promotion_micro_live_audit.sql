CREATE TABLE IF NOT EXISTS futuros_operaciones.user_wallet_copy_plan (
  id bigserial PRIMARY KEY,
  id_user uuid NOT NULL,
  wallet_lc text NOT NULL,
  allocation_pct numeric(9,6) NOT NULL,
  score int,
  status text NOT NULL DEFAULT 'ACTIVE',
  is_active boolean NOT NULL DEFAULT true,
  allocation_run_id uuid,
  metric_version int NOT NULL DEFAULT 1,
  max_wallet int,
  user_capital_usd numeric(18,8),
  allocated_capital_usd numeric(18,8),
  copy_min_notional_mode text NOT NULL DEFAULT 'INHERIT',
  copy_min_notional_max_usdt numeric(18,8),
  copy_min_notional_min_score int,
  copy_min_notional_min_history_days int,
  copy_min_notional_min_operations int,
  reason jsonb NOT NULL DEFAULT '{}'::jsonb,
  synced_to_runtime boolean NOT NULL DEFAULT false,
  runtime_synced_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_user_wallet_copy_plan_user_wallet UNIQUE (id_user, wallet_lc),
  CONSTRAINT chk_user_wallet_copy_plan_status CHECK (status IN ('ACTIVE', 'PAUSED', 'CLOSED')),
  CONSTRAINT chk_user_wallet_copy_plan_pct CHECK (allocation_pct >= 0 AND allocation_pct <= 1)
);

ALTER TABLE futuros_operaciones.user_wallet_copy_plan
    ADD COLUMN IF NOT EXISTS allocation_run_id uuid,
    ADD COLUMN IF NOT EXISTS metric_version int NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS max_wallet int,
    ADD COLUMN IF NOT EXISTS user_capital_usd numeric(18,8),
    ADD COLUMN IF NOT EXISTS allocated_capital_usd numeric(18,8),
    ADD COLUMN IF NOT EXISTS copy_min_notional_mode text NOT NULL DEFAULT 'INHERIT',
    ADD COLUMN IF NOT EXISTS copy_min_notional_max_usdt numeric(18,8),
    ADD COLUMN IF NOT EXISTS copy_min_notional_min_score int,
    ADD COLUMN IF NOT EXISTS copy_min_notional_min_history_days int,
    ADD COLUMN IF NOT EXISTS copy_min_notional_min_operations int,
    ADD COLUMN IF NOT EXISTS reason jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS synced_to_runtime boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS runtime_synced_at timestamptz,
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS ix_user_wallet_copy_plan_wallet_active_users
ON futuros_operaciones.user_wallet_copy_plan (wallet_lc, id_user)
WHERE is_active = true AND status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS ix_user_wallet_copy_plan_user_active_wallets
ON futuros_operaciones.user_wallet_copy_plan (id_user, wallet_lc)
WHERE is_active = true AND status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_promotion_audit (
    id bigserial PRIMARY KEY,
    id_user uuid,
    wallet_id varchar(128),
    copy_strategy_code varchar(64),
    source_execution_mode varchar(32),
    target_execution_mode varchar(32),
    decision varchar(64) NOT NULL,
    reason_code varchar(120) NOT NULL,
    reason_details jsonb NOT NULL DEFAULT '{}'::jsonb,
    shadow_allocation_id bigint,
    micro_live_allocation_id bigint,
    live_allocation_id bigint,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_copy_promotion_audit_user_created
ON futuros_operaciones.copy_promotion_audit (id_user, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_copy_promotion_audit_shadow_created
ON futuros_operaciones.copy_promotion_audit (shadow_allocation_id, created_at DESC)
WHERE shadow_allocation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_copy_promotion_audit_micro_created
ON futuros_operaciones.copy_promotion_audit (micro_live_allocation_id, created_at DESC)
WHERE micro_live_allocation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_copy_promotion_audit_live_created
ON futuros_operaciones.copy_promotion_audit (live_allocation_id, created_at DESC)
WHERE live_allocation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_copy_promotion_audit_decision_reason_created
ON futuros_operaciones.copy_promotion_audit (decision, reason_code, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_shadow_copy_allocation_promotion_candidates
ON futuros_operaciones.shadow_copy_allocation (
    is_active,
    ends_at,
    linked_live_allocation_id,
    status,
    strategy_score DESC,
    decision_score DESC,
    updated_at DESC
)
WHERE is_active = true
  AND ends_at IS NULL
  AND linked_live_allocation_id IS NULL;

CREATE INDEX IF NOT EXISTS ix_user_copy_allocation_micro_live_promotion
ON futuros_operaciones.user_copy_allocation (
    execution_mode,
    is_active,
    status,
    promoted_from_shadow_at,
    updated_at
)
WHERE ends_at IS NULL
  AND is_active = true
  AND execution_mode = 'MICRO_LIVE';

CREATE INDEX IF NOT EXISTS ix_copy_operation_event_allocation_mode_time
ON futuros_operaciones.copy_operation_event (
    user_copy_allocation_id,
    execution_mode,
    event_time
)
WHERE user_copy_allocation_id IS NOT NULL
  AND coalesce(is_shadow, false) = false;
