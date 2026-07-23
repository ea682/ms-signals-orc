-- V202607180002 installs these constraints as NOT VALID so the online DDL does
-- not take an immediate long-running validation lock.  Once its deterministic
-- account-purpose backfill is complete, certify the full historical data set.
ALTER TABLE futuros_operaciones.user_api_keys
    VALIDATE CONSTRAINT chk_user_api_keys_account_purpose;

ALTER TABLE futuros_operaciones.user_copy_allocation
    VALIDATE CONSTRAINT fk_user_copy_allocation_exchange_account;
ALTER TABLE futuros_operaciones.copy_operation
    VALIDATE CONSTRAINT fk_copy_operation_exchange_account;
ALTER TABLE futuros_operaciones.copy_operation_event
    VALIDATE CONSTRAINT fk_copy_operation_event_exchange_account;
ALTER TABLE futuros_operaciones.copy_dispatch_intent
    VALIDATE CONSTRAINT fk_copy_dispatch_intent_exchange_account;
ALTER TABLE futuros_operaciones.copy_economic_cycle
    VALIDATE CONSTRAINT fk_copy_economic_cycle_exchange_account;

ALTER TABLE futuros_operaciones.user_copy_allocation
    VALIDATE CONSTRAINT chk_real_allocation_execution_account;
ALTER TABLE futuros_operaciones.user_copy_allocation
    VALIDATE CONSTRAINT chk_micro_live_reserved_capital;
