ALTER TABLE futuros_operaciones.shadow_position_state
    ADD COLUMN IF NOT EXISTS last_accepted_event_at timestamptz;

WITH latest_accepted AS (
    SELECT shadow_position_id, max(event_time) AS last_event_at
    FROM futuros_operaciones.shadow_copy_operation_event
    WHERE shadow_position_id IS NOT NULL
      AND decision = 'SIMULATED'
    GROUP BY shadow_position_id
)
UPDATE futuros_operaciones.shadow_position_state state
SET last_accepted_event_at = COALESCE(
        latest_accepted.last_event_at,
        GREATEST(state.opened_at, state.closed_at),
        state.opened_at,
        state.closed_at
    )
FROM latest_accepted
WHERE latest_accepted.shadow_position_id = state.id
  AND state.last_accepted_event_at IS NULL;

UPDATE futuros_operaciones.shadow_position_state
SET last_accepted_event_at = COALESCE(
        GREATEST(opened_at, closed_at),
        opened_at,
        closed_at
    )
WHERE last_accepted_event_at IS NULL;

COMMENT ON COLUMN futuros_operaciones.shadow_position_state.last_accepted_event_at IS
    'UTC source timestamp of the latest event accepted into this SHADOW position lifecycle.';
