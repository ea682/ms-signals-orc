SET lock_timeout = '60s';

DROP INDEX CONCURRENTLY IF EXISTS futuros_operaciones.ix_shadow_event_profile_decision_aggregate;

CREATE INDEX CONCURRENTLY ix_shadow_event_profile_decision_aggregate
    ON futuros_operaciones.shadow_copy_operation_event (wallet_profile_id)
    INCLUDE (decision)
    WHERE wallet_profile_id IS NOT NULL
      AND decision IN ('SIMULATED', 'RECORDED', 'SKIPPED', 'DUPLICATE', 'ERROR');

COMMENT ON INDEX futuros_operaciones.ix_shadow_event_profile_decision_aggregate IS
    'Index-only decision counters for one SHADOW wallet profile';
