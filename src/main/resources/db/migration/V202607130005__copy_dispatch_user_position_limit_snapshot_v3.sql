ALTER TABLE futuros_operaciones.copy_dispatch_intent
    ADD COLUMN IF NOT EXISTS user_max_concurrent_positions INTEGER;

ALTER TABLE futuros_operaciones.copy_dispatch_intent
    DROP CONSTRAINT IF EXISTS copy_dispatch_intent_user_position_limit_chk;

ALTER TABLE futuros_operaciones.copy_dispatch_intent
    ADD CONSTRAINT copy_dispatch_intent_user_position_limit_chk
    CHECK (user_max_concurrent_positions IS NULL OR user_max_concurrent_positions > 0);

COMMENT ON COLUMN futuros_operaciones.copy_dispatch_intent.user_max_concurrent_positions IS
    'Nullable allocation-scoped position limit used by the atomic V3 reservation; NULL means unlimited.';
