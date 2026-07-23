CREATE TABLE IF NOT EXISTS futuros_operaciones.micro_live_account_capacity (
    execution_account_id uuid PRIMARY KEY
        REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys),
    asset varchar(16) NOT NULL,
    authoritative_equity_usd numeric(38,8) NOT NULL,
    available_balance_usd numeric(38,8) NOT NULL,
    safety_buffer_usd numeric(38,8) NOT NULL DEFAULT 0,
    eligible_capital_usd numeric(38,8) NOT NULL,
    budget_per_allocation_usd numeric(38,8) NOT NULL,
    theoretical_capacity integer NOT NULL,
    effective_capacity integer NOT NULL,
    configured_max integer NOT NULL DEFAULT 0,
    reserved_recertification_slots integer NOT NULL DEFAULT 0,
    observed_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chk_micro_capacity_amounts CHECK (
        authoritative_equity_usd >= 0
        AND available_balance_usd >= 0
        AND safety_buffer_usd >= 0
        AND eligible_capital_usd >= 0
        AND eligible_capital_usd <= authoritative_equity_usd
        AND budget_per_allocation_usd > 0
    ),
    CONSTRAINT chk_micro_capacity_slots CHECK (
        theoretical_capacity >= 0
        AND effective_capacity >= 0
        AND effective_capacity <= theoretical_capacity
        AND configured_max >= 0
        AND (configured_max = 0 OR effective_capacity <= configured_max)
        AND reserved_recertification_slots >= 0
        AND reserved_recertification_slots <= effective_capacity
    ),
    CONSTRAINT chk_micro_capacity_window CHECK (valid_until > observed_at)
);

CREATE TABLE IF NOT EXISTS futuros_operaciones.micro_live_recertification_request (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    certification_id uuid NOT NULL
        REFERENCES futuros_operaciones.strategy_live_certification(id),
    wallet_id varchar(128) NOT NULL,
    strategy_code varchar(100) NOT NULL,
    strategy_version varchar(100) NOT NULL,
    user_id uuid NOT NULL REFERENCES futuros_operaciones.users(id),
    execution_account_id uuid NOT NULL
        REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys),
    requested_at timestamptz NOT NULL DEFAULT now(),
    priority integer NOT NULL DEFAULT 100,
    status varchar(32) NOT NULL DEFAULT 'PENDING_CAPACITY',
    attempts integer NOT NULL DEFAULT 0,
    reason_code varchar(160) NOT NULL DEFAULT 'MICRO_LIVE_RECERTIFICATION_PENDING_CAPACITY',
    idempotency_key varchar(320) NOT NULL,
    user_copy_allocation_id bigint
        REFERENCES futuros_operaciones.user_copy_allocation(id),
    claimed_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT ux_micro_live_recertification_idempotency UNIQUE (idempotency_key),
    CONSTRAINT chk_micro_live_recertification_priority CHECK (priority > 0),
    CONSTRAINT chk_micro_live_recertification_attempts CHECK (attempts >= 0),
    CONSTRAINT chk_micro_live_recertification_status CHECK (status IN (
        'PENDING_CAPACITY','CLAIMED','ADMITTED','CANCELLED','INELIGIBLE','FAILED'
    ))
);

CREATE INDEX IF NOT EXISTS ix_micro_live_recertification_pending_priority
    ON futuros_operaciones.micro_live_recertification_request
        (execution_account_id, priority DESC, requested_at, id)
    WHERE status = 'PENDING_CAPACITY';

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS chk_micro_live_reserved_capital;
ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT chk_micro_live_reserved_capital
    CHECK (execution_mode <> 'MICRO_LIVE' OR reserved_capital_usd > 0) NOT VALID;

CREATE OR REPLACE FUNCTION futuros_operaciones.enforce_execution_account_and_micro_capacity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    account_row futuros_operaciones.user_api_keys%ROWTYPE;
    capacity_row futuros_operaciones.micro_live_account_capacity%ROWTYPE;
    occupied_slots integer;
    admission_limit integer;
    is_recertification boolean;
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.exchange_account_id IS NOT NULL
       AND NEW.exchange_account_id IS DISTINCT FROM OLD.exchange_account_id THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'EXECUTION_ACCOUNT_REBIND_FORBIDDEN';
    END IF;
    IF NEW.execution_mode = 'SHADOW' THEN
        RETURN NEW;
    END IF;
    IF NEW.exchange_account_id IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE =
            CASE WHEN NEW.execution_mode = 'MICRO_LIVE'
                 THEN 'MICRO_LIVE_EXECUTION_ACCOUNT_MISSING'
                 ELSE 'LIVE_EXECUTION_ACCOUNT_MISSING' END;
    END IF;

    SELECT * INTO account_row
    FROM futuros_operaciones.user_api_keys
    WHERE id_user_api_keys = NEW.exchange_account_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'EXECUTION_ACCOUNT_MISSING';
    END IF;
    IF account_row.user_id <> NEW.id_user
       OR upper(account_row.exchange) <> 'BINANCE'
       OR account_row.account_purpose IS DISTINCT FROM NEW.execution_mode THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'EXECUTION_ACCOUNT_PURPOSE_MISMATCH';
    END IF;
    IF NOT account_row.active THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'EXECUTION_ACCOUNT_INACTIVE';
    END IF;

    IF NEW.execution_mode = 'MICRO_LIVE'
       AND NEW.ends_at IS NULL
       AND NEW.is_active
       AND lower(NEW.status) <> 'closed' THEN
        SELECT * INTO capacity_row
        FROM futuros_operaciones.micro_live_account_capacity
        WHERE execution_account_id = NEW.exchange_account_id
        FOR UPDATE;
        IF NOT FOUND OR capacity_row.valid_until <= now() THEN
            RAISE EXCEPTION USING ERRCODE = '23514',
                MESSAGE = 'MICRO_LIVE_CAPACITY_ACCOUNT_BALANCE_UNAVAILABLE';
        END IF;
        IF NEW.reserved_capital_usd IS DISTINCT FROM capacity_row.budget_per_allocation_usd THEN
            RAISE EXCEPTION USING ERRCODE = '23514',
                MESSAGE = 'MICRO_LIVE_RESERVED_CAPITAL_INSUFFICIENT';
        END IF;

        is_recertification := coalesce(NEW.status_reason, '') LIKE 'LIVE_CERTIFICATION_RECERTIFICATION%'
            OR coalesce(NEW.status_reason, '') LIKE 'MICRO_LIVE_RECERTIFICATION%';
        admission_limit := capacity_row.effective_capacity;
        IF NOT is_recertification THEN
            admission_limit := greatest(0, admission_limit - capacity_row.reserved_recertification_slots);
            IF capacity_row.reserved_recertification_slots = 0 AND EXISTS (
                SELECT 1
                FROM futuros_operaciones.micro_live_recertification_request request
                WHERE request.execution_account_id = NEW.exchange_account_id
                  AND request.status IN ('PENDING_CAPACITY','CLAIMED')
            ) THEN
                RAISE EXCEPTION USING ERRCODE = '23514',
                    MESSAGE = 'MICRO_LIVE_RECERTIFICATION_PENDING_PRIORITY';
            END IF;
        END IF;

        SELECT count(*) INTO occupied_slots
        FROM futuros_operaciones.user_copy_allocation allocation
        WHERE allocation.exchange_account_id = NEW.exchange_account_id
          AND allocation.execution_mode = 'MICRO_LIVE'
          AND allocation.ends_at IS NULL
          AND allocation.is_active
          AND lower(allocation.status) <> 'closed'
          AND (NEW.id IS NULL OR allocation.id <> NEW.id);

        IF capacity_row.eligible_capital_usd
                < capacity_row.budget_per_allocation_usd * occupied_slots THEN
            RAISE EXCEPTION USING ERRCODE = '23514',
                MESSAGE = 'MICRO_LIVE_ACCOUNT_UNDER_RESERVED';
        END IF;
        IF occupied_slots >= admission_limit THEN
            RAISE EXCEPTION USING ERRCODE = '23514',
                MESSAGE = CASE
                    WHEN capacity_row.configured_max > 0
                         AND capacity_row.effective_capacity < capacity_row.configured_max
                    THEN 'MICRO_LIVE_ACCOUNT_UNDER_RESERVED'
                    ELSE 'MICRO_LIVE_CAPACITY_EXHAUSTED'
                END;
        END IF;
        IF capacity_row.eligible_capital_usd
                < capacity_row.budget_per_allocation_usd * (occupied_slots + 1) THEN
            RAISE EXCEPTION USING ERRCODE = '23514',
                MESSAGE = 'MICRO_LIVE_RESERVED_CAPITAL_INSUFFICIENT';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION futuros_operaciones.prevent_micro_live_release_until_flat()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.execution_mode = 'MICRO_LIVE'
       AND OLD.ends_at IS NULL
       AND OLD.is_active
       AND lower(OLD.status) <> 'closed'
       AND (NEW.ends_at IS NOT NULL OR NOT NEW.is_active OR lower(NEW.status) = 'closed')
       AND (
           EXISTS (SELECT 1 FROM futuros_operaciones.copy_operation operation
                   WHERE operation.user_copy_allocation_id = OLD.id AND operation.is_active)
           OR EXISTS (SELECT 1 FROM futuros_operaciones.copy_dispatch_intent intent
                      WHERE intent.user_copy_allocation_id = OLD.id
                        AND intent.status IN ('CREATED','CLAIMED','DISPATCHING','ACKNOWLEDGED','NEW',
                                             'PARTIALLY_FILLED','FILLED','RECONCILING','PERSISTENCE_PENDING'))
           OR EXISTS (SELECT 1 FROM futuros_operaciones.copy_position_ownership ownership
                      WHERE ownership.user_copy_allocation_id = OLD.id
                        AND ownership.ownership_status <> 'CLOSED')
           OR EXISTS (SELECT 1 FROM futuros_operaciones.copy_flip_saga saga
                      WHERE saga.user_copy_allocation_id = OLD.id
                        AND saga.saga_status NOT IN ('COMPLETED','COMPLETED_FLAT_NEW_SKIPPED','FAILED'))
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'MICRO_LIVE_RELEASE_REQUIRES_FLAT';
    END IF;
    RETURN NEW;
END;
$$;
