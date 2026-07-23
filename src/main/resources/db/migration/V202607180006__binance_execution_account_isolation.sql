ALTER TABLE futuros_operaciones.user_api_keys
    ADD COLUMN IF NOT EXISTS exchange_account_ref varchar(128),
    ADD COLUMN IF NOT EXISTS identity_verified_at timestamptz;

ALTER TABLE futuros_operaciones.user_api_keys
    DROP CONSTRAINT IF EXISTS chk_user_api_keys_exchange_account_ref;
ALTER TABLE futuros_operaciones.user_api_keys
    ADD CONSTRAINT chk_user_api_keys_exchange_account_ref
    CHECK (exchange_account_ref IS NULL OR length(trim(exchange_account_ref)) > 0) NOT VALID;

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_api_keys_active_external_account
    ON futuros_operaciones.user_api_keys (user_id, upper(exchange), exchange_account_ref)
    WHERE active AND exchange_account_ref IS NOT NULL;

CREATE OR REPLACE FUNCTION futuros_operaciones.prevent_execution_account_identity_rebind()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.exchange_account_ref IS NOT NULL
       AND NEW.exchange_account_ref IS DISTINCT FROM OLD.exchange_account_ref THEN
        RAISE EXCEPTION USING ERRCODE = '23514', MESSAGE = 'EXECUTION_ACCOUNT_REBIND_FORBIDDEN';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_prevent_execution_account_identity_rebind
    ON futuros_operaciones.user_api_keys;
CREATE TRIGGER trg_prevent_execution_account_identity_rebind
BEFORE UPDATE OF exchange_account_ref
ON futuros_operaciones.user_api_keys
FOR EACH ROW EXECUTE FUNCTION futuros_operaciones.prevent_execution_account_identity_rebind();

ALTER TABLE futuros_operaciones.user_api_keys
    VALIDATE CONSTRAINT chk_user_api_keys_exchange_account_ref;
ALTER TABLE futuros_operaciones.user_copy_allocation
    VALIDATE CONSTRAINT chk_micro_live_reserved_capital;
