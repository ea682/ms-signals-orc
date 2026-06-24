ALTER TABLE futuros_operaciones.shadow_wallet_profile_validation
    ADD COLUMN IF NOT EXISTS simulated_events bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recorded_events bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS error_events bigint NOT NULL DEFAULT 0;

UPDATE futuros_operaciones.shadow_wallet_profile_validation v
SET simulated_events = COALESCE(e.simulated_events, 0),
    recorded_events = COALESCE(e.recorded_events, 0),
    skipped_events = COALESCE(e.skipped_events, 0),
    duplicate_events = COALESCE(e.duplicate_events, 0),
    error_events = COALESCE(e.error_events, 0)
FROM (
    SELECT
        wallet_profile_id,
        count(*) FILTER (WHERE decision = 'SIMULATED') AS simulated_events,
        count(*) FILTER (WHERE decision = 'RECORDED') AS recorded_events,
        count(*) FILTER (WHERE decision = 'SKIPPED') AS skipped_events,
        count(*) FILTER (WHERE decision = 'DUPLICATE') AS duplicate_events,
        count(*) FILTER (WHERE decision = 'ERROR') AS error_events
    FROM futuros_operaciones.shadow_copy_operation_event
    WHERE wallet_profile_id IS NOT NULL
    GROUP BY wallet_profile_id
) e
WHERE v.wallet_profile_id = e.wallet_profile_id;
