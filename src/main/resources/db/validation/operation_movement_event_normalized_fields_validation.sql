-- Should be near zero for new events after deploying the normalized-field patch.
WITH checked AS (
    SELECT
        e.*,
        abs(e.effective_close_qty) * e.effective_exit_price AS expected_closed_notional_usd
    FROM futuros_operaciones.operation_movement_event e
    WHERE e.event_time >= now() - interval '24 hours'
      AND e.effective_close_qty IS NOT NULL
      AND e.effective_exit_price IS NOT NULL
      AND abs(e.effective_close_qty) > 0
      AND e.effective_exit_price > 0
)
SELECT
    count(*) AS checked_rows,
    count(*) FILTER (
        WHERE abs(COALESCE(closed_notional_usd, 0) - expected_closed_notional_usd) > 1
    ) AS bad_closed_notional_rows,
    round(
        100.0 * count(*) FILTER (
            WHERE abs(COALESCE(closed_notional_usd, 0) - expected_closed_notional_usd) > 1
        ) / nullif(count(*), 0),
        4
    ) AS bad_closed_notional_pct
FROM checked;
