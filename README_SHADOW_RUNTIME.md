# SHADOW runtime diagnostics

After deploying the SHADOW/LIVE certification fixes, run:

```sql
\i src/main/resources/db/validation/shadow_runtime_tables_validation.sql
```

Expected results:

- `shadow_closed_position_with_open_operation`: 0 rows.
- `shadow_position_event_without_position_id`: 0 rows for real position open/resize/close events. SKIPPED events may have no position id only when the reason says no SHADOW position was created.
- `shadow_flip_audit`: includes `SHADOW_POSITION_CLOSED_BY_FLIP` and `SHADOW_POSITION_OPENED_BY_FLIP` rows when FLIP events were processed.
- `shadow_event_decision_counts` must match `shadow_validation_event_counters` for `SIMULATED`, `RECORDED`, `SKIPPED`, `DUPLICATE`, and `ERROR` decisions.

For an idempotent duplicate FLIP, the application log should show `event=shadow_flip_duplicate_ignored reasonCode=DUPLICATE_SHADOW_FLIP_EVENT` and the event counts should not increase.
