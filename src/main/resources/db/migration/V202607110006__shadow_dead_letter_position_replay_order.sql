CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_shadow_dead_letter_position_replay_order
    ON futuros_operaciones.shadow_event_dead_letter (
        (coalesce(position_key, idempotency_key)),
        first_failed_at,
        idempotency_key
    )
    WHERE status IN ('RECOVERABLE', 'REPLAYING');

COMMENT ON INDEX futuros_operaciones.ix_shadow_dead_letter_position_replay_order IS
    'Keeps SHADOW dead-letter replay ordered and single-flight per source position.';
