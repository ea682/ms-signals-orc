-- Stable Binance execution-account identity. The credential row is the account
-- identity; credential rotation updates api_key/api_secret without replacing its UUID.
ALTER TABLE futuros_operaciones.user_api_keys
    ADD COLUMN IF NOT EXISTS account_purpose varchar(24),
    ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT true;

-- The audited production table predates a declared primary key even though the
-- UUID is its JPA identity. Make that identity enforceable before referencing it.
CREATE UNIQUE INDEX IF NOT EXISTS ux_user_api_keys_stable_id
    ON futuros_operaciones.user_api_keys (id_user_api_keys);

ALTER TABLE futuros_operaciones.user_api_keys
    DROP CONSTRAINT IF EXISTS chk_user_api_keys_account_purpose;
ALTER TABLE futuros_operaciones.user_api_keys
    ADD CONSTRAINT chk_user_api_keys_account_purpose
    CHECK (account_purpose IS NULL OR account_purpose IN ('LIVE', 'MICRO_LIVE')) NOT VALID;

-- Only explicit labels are safe to backfill automatically. Unlabelled legacy keys
-- deliberately remain unresolved and therefore fail closed until classified.
UPDATE futuros_operaciones.user_api_keys
SET account_purpose = CASE
    WHEN upper(replace(trim(label), '-', '_')) = 'LIVE' THEN 'LIVE'
    WHEN upper(replace(trim(label), '-', '_')) IN ('MICRO_LIVE', 'MICROLIVE') THEN 'MICRO_LIVE'
    ELSE account_purpose
END
WHERE account_purpose IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_api_keys_active_purpose
    ON futuros_operaciones.user_api_keys (user_id, upper(exchange), account_purpose)
    WHERE active AND account_purpose IS NOT NULL;

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD COLUMN IF NOT EXISTS exchange_account_id uuid,
    ADD COLUMN IF NOT EXISTS reserved_capital_usd numeric(38,8);

ALTER TABLE futuros_operaciones.copy_operation
    ADD COLUMN IF NOT EXISTS exchange_account_id uuid,
    ADD COLUMN IF NOT EXISTS source_position_cycle_id uuid;

ALTER TABLE futuros_operaciones.copy_operation_event
    ADD COLUMN IF NOT EXISTS exchange_account_id uuid,
    ADD COLUMN IF NOT EXISTS source_position_cycle_id uuid;

ALTER TABLE futuros_operaciones.copy_dispatch_intent
    ADD COLUMN IF NOT EXISTS exchange_account_id uuid,
    ADD COLUMN IF NOT EXISTS source_position_cycle_id uuid,
    ADD COLUMN IF NOT EXISTS fixed_margin_mode varchar(24),
    ADD COLUMN IF NOT EXISTS fixed_position_mode varchar(24);

ALTER TABLE futuros_operaciones.copy_economic_cycle
    ADD COLUMN IF NOT EXISTS exchange_account_id uuid,
    ADD COLUMN IF NOT EXISTS source_position_cycle_id uuid,
    ADD COLUMN IF NOT EXISTS fixed_leverage numeric(12,4),
    ADD COLUMN IF NOT EXISTS fixed_margin_mode varchar(24),
    ADD COLUMN IF NOT EXISTS fixed_position_mode varchar(24),
    ADD COLUMN IF NOT EXISTS virtual_owned_qty numeric(38,18) NOT NULL DEFAULT 0;

ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT fk_user_copy_allocation_exchange_account
    FOREIGN KEY (exchange_account_id)
    REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys) NOT VALID;
ALTER TABLE futuros_operaciones.copy_operation
    ADD CONSTRAINT fk_copy_operation_exchange_account
    FOREIGN KEY (exchange_account_id)
    REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys) NOT VALID;
ALTER TABLE futuros_operaciones.copy_operation_event
    ADD CONSTRAINT fk_copy_operation_event_exchange_account
    FOREIGN KEY (exchange_account_id)
    REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys) NOT VALID;
ALTER TABLE futuros_operaciones.copy_dispatch_intent
    ADD CONSTRAINT fk_copy_dispatch_intent_exchange_account
    FOREIGN KEY (exchange_account_id)
    REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys) NOT VALID;
ALTER TABLE futuros_operaciones.copy_economic_cycle
    ADD CONSTRAINT fk_copy_economic_cycle_exchange_account
    FOREIGN KEY (exchange_account_id)
    REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys) NOT VALID;

-- Safe deterministic backfill is possible only after an account purpose has been
-- classified. No latest-key or first-key fallback is used.
UPDATE futuros_operaciones.user_copy_allocation a
SET exchange_account_id = k.id_user_api_keys,
    reserved_capital_usd = CASE WHEN a.execution_mode = 'MICRO_LIVE' THEN 100 ELSE a.reserved_capital_usd END
FROM futuros_operaciones.user_api_keys k
WHERE a.exchange_account_id IS NULL
  AND a.execution_mode IN ('LIVE', 'MICRO_LIVE')
  AND k.user_id = a.id_user
  AND upper(k.exchange) = 'BINANCE'
  AND k.active
  AND k.account_purpose = a.execution_mode;

UPDATE futuros_operaciones.copy_operation o
SET exchange_account_id = a.exchange_account_id
FROM futuros_operaciones.user_copy_allocation a
WHERE o.user_copy_allocation_id = a.id
  AND o.exchange_account_id IS NULL;

UPDATE futuros_operaciones.copy_operation_event e
SET exchange_account_id = a.exchange_account_id
FROM futuros_operaciones.user_copy_allocation a
WHERE e.user_copy_allocation_id = a.id
  AND e.exchange_account_id IS NULL;

UPDATE futuros_operaciones.copy_dispatch_intent i
SET exchange_account_id = a.exchange_account_id
FROM futuros_operaciones.user_copy_allocation a
WHERE i.user_copy_allocation_id = a.id
  AND i.exchange_account_id IS NULL;

UPDATE futuros_operaciones.copy_economic_cycle c
SET exchange_account_id = a.exchange_account_id
FROM futuros_operaciones.user_copy_allocation a
WHERE c.user_copy_allocation_id = a.id
  AND c.exchange_account_id IS NULL;

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS chk_real_allocation_execution_account;
ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT chk_real_allocation_execution_account
    CHECK (execution_mode = 'SHADOW' OR exchange_account_id IS NOT NULL) NOT VALID;

ALTER TABLE futuros_operaciones.user_copy_allocation
    DROP CONSTRAINT IF EXISTS chk_micro_live_reserved_capital;
ALTER TABLE futuros_operaciones.user_copy_allocation
    ADD CONSTRAINT chk_micro_live_reserved_capital
    CHECK (execution_mode <> 'MICRO_LIVE' OR reserved_capital_usd = 100) NOT VALID;

CREATE OR REPLACE FUNCTION futuros_operaciones.prevent_exchange_account_rebind()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.exchange_account_id IS NOT NULL
       AND NEW.exchange_account_id IS DISTINCT FROM OLD.exchange_account_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'EXECUTION_ACCOUNT_REBIND_FORBIDDEN';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_prevent_exchange_account_rebind
    ON futuros_operaciones.user_copy_allocation;
CREATE TRIGGER trg_prevent_exchange_account_rebind
BEFORE UPDATE OF exchange_account_id
ON futuros_operaciones.user_copy_allocation
FOR EACH ROW EXECUTE FUNCTION futuros_operaciones.prevent_exchange_account_rebind();

CREATE OR REPLACE FUNCTION futuros_operaciones.enforce_execution_account_and_micro_capacity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    account_row futuros_operaciones.user_api_keys%ROWTYPE;
    occupied_slots integer;
BEGIN
    -- PostgreSQL orders same-kind triggers by name. Repeat the immutable check
    -- here so the public reason is deterministic even if this trigger runs first.
    IF TG_OP = 'UPDATE'
       AND OLD.exchange_account_id IS NOT NULL
       AND NEW.exchange_account_id IS DISTINCT FROM OLD.exchange_account_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'EXECUTION_ACCOUNT_REBIND_FORBIDDEN';
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
        IF NEW.reserved_capital_usd IS DISTINCT FROM 100::numeric THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'MICRO_LIVE_RESERVED_CAPITAL_INSUFFICIENT';
        END IF;
        SELECT count(*) INTO occupied_slots
        FROM futuros_operaciones.user_copy_allocation a
        WHERE a.exchange_account_id = NEW.exchange_account_id
          AND a.execution_mode = 'MICRO_LIVE'
          AND a.ends_at IS NULL
          AND a.is_active
          AND lower(a.status) <> 'closed'
          AND (NEW.id IS NULL OR a.id <> NEW.id);
        IF occupied_slots >= 5 THEN
            RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'MICRO_LIVE_CAPACITY_EXHAUSTED';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_enforce_execution_account_and_micro_capacity
    ON futuros_operaciones.user_copy_allocation;
CREATE TRIGGER trg_enforce_execution_account_and_micro_capacity
BEFORE INSERT OR UPDATE OF exchange_account_id, execution_mode, status, is_active, ends_at, reserved_capital_usd
ON futuros_operaciones.user_copy_allocation
FOR EACH ROW EXECUTE FUNCTION futuros_operaciones.enforce_execution_account_and_micro_capacity();

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
           EXISTS (SELECT 1 FROM futuros_operaciones.copy_operation o
                   WHERE o.user_copy_allocation_id = OLD.id AND o.is_active)
           OR EXISTS (SELECT 1 FROM futuros_operaciones.copy_dispatch_intent i
                      WHERE i.user_copy_allocation_id = OLD.id
                        AND i.status IN ('CREATED','CLAIMED','DISPATCHING','ACKNOWLEDGED','NEW',
                                         'PARTIALLY_FILLED','FILLED','RECONCILING','PERSISTENCE_PENDING'))
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'MICRO_LIVE_RELEASE_REQUIRES_FLAT';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_prevent_micro_live_release_until_flat
    ON futuros_operaciones.user_copy_allocation;
CREATE TRIGGER trg_prevent_micro_live_release_until_flat
BEFORE UPDATE OF status, is_active, ends_at
ON futuros_operaciones.user_copy_allocation
FOR EACH ROW EXECUTE FUNCTION futuros_operaciones.prevent_micro_live_release_until_flat();

CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_position_ownership (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    exchange_account_id uuid NOT NULL REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys),
    user_copy_allocation_id bigint NOT NULL REFERENCES futuros_operaciones.user_copy_allocation(id),
    source_position_cycle_id uuid NOT NULL,
    economic_cycle_id uuid REFERENCES futuros_operaciones.copy_economic_cycle(cycle_id),
    symbol varchar(40) NOT NULL,
    position_side varchar(16) NOT NULL,
    owned_qty numeric(38,18) NOT NULL DEFAULT 0,
    actual_binance_qty numeric(38,18),
    fixed_leverage numeric(12,4) NOT NULL,
    fixed_margin_mode varchar(24) NOT NULL,
    fixed_position_mode varchar(24) NOT NULL,
    ownership_status varchar(24) NOT NULL DEFAULT 'OPEN',
    reconciliation_required boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    closed_at timestamptz,
    CONSTRAINT chk_copy_position_owned_qty CHECK (owned_qty >= 0),
    CONSTRAINT chk_copy_position_ownership_status
        CHECK (ownership_status IN ('OPEN','CLOSING','RECONCILING','CLOSED'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_position_ownership_active_account_symbol
    ON futuros_operaciones.copy_position_ownership (exchange_account_id, upper(symbol))
    WHERE ownership_status <> 'CLOSED';
CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_position_ownership_source_cycle
    ON futuros_operaciones.copy_position_ownership (user_copy_allocation_id, source_position_cycle_id);

CREATE TABLE IF NOT EXISTS futuros_operaciones.copy_flip_saga (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_copy_allocation_id bigint NOT NULL REFERENCES futuros_operaciones.user_copy_allocation(id),
    exchange_account_id uuid NOT NULL REFERENCES futuros_operaciones.user_api_keys(id_user_api_keys),
    old_source_position_cycle_id uuid,
    new_source_position_cycle_id uuid NOT NULL,
    symbol varchar(40) NOT NULL,
    old_side varchar(16),
    new_side varchar(16) NOT NULL,
    saga_status varchar(40) NOT NULL,
    old_leg_result varchar(80),
    new_leg_result varchar(80),
    reason_code varchar(120),
    old_close_client_order_id varchar(36),
    new_open_client_order_id varchar(36),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT chk_copy_flip_saga_status CHECK (saga_status IN (
        'CLOSE_OLD_PENDING','CONFIRM_FLAT_PENDING','OPEN_NEW_PENDING',
        'COMPLETED','COMPLETED_FLAT_NEW_SKIPPED','RECONCILIATION_REQUIRED','FAILED'
    ))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_copy_flip_saga_allocation_new_cycle
    ON futuros_operaciones.copy_flip_saga (user_copy_allocation_id, new_source_position_cycle_id);

CREATE INDEX IF NOT EXISTS ix_copy_dispatch_exchange_account_symbol_status
    ON futuros_operaciones.copy_dispatch_intent (exchange_account_id, symbol, status);
CREATE INDEX IF NOT EXISTS ix_copy_operation_exchange_account_symbol_active
    ON futuros_operaciones.copy_operation (exchange_account_id, parsymbol, is_active);
