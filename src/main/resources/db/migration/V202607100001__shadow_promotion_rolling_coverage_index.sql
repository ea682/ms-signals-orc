-- Supports the bounded rolling-coverage lookup used only by the promotion job.
-- This follows the repository's transactional Flyway convention. PostgreSQL
-- takes a SHARE lock while building it, so schedule the migration accordingly
-- on a large ledger or pre-create the identical index CONCURRENTLY via DBA runbook.
CREATE INDEX IF NOT EXISTS ix_shadow_event_allocation_rolling_coverage
    ON futuros_operaciones.shadow_copy_operation_event (
        shadow_allocation_id, event_time DESC, id_event DESC
    )
    INCLUDE (decision)
    WHERE shadow_allocation_id IS NOT NULL
      AND event_time IS NOT NULL
      AND decision IN ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR');

COMMENT ON INDEX futuros_operaciones.ix_shadow_event_allocation_rolling_coverage IS
    'Bounded SHADOW promotion coverage by exact allocation and UTC event_time';
