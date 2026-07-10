package com.apunto.engine.repository;

public final class ShadowCoverageSql {

    public static final String ROLLING_BATCH_QUERY = """
            with ranked_events as (
                select e.shadow_allocation_id,
                       e.decision,
                       e.event_time,
                       row_number() over (
                           partition by e.shadow_allocation_id
                           order by e.event_time desc, e.id_event desc
                       ) as event_rank
                from futuros_operaciones.shadow_copy_operation_event e
                where e.shadow_allocation_id in (:allocationIds)
                  and e.event_time >= :windowStart
                  and e.event_time <= :windowEnd
                  and e.decision in ('SIMULATED', 'RECORDED', 'SKIPPED', 'ERROR')
            ), bounded_events as (
                select shadow_allocation_id, decision, event_time
                from ranked_events
                where event_rank <= :maxEvents
            )
            select shadow_allocation_id as "shadowAllocationId",
                   count(*) filter (where decision = 'SIMULATED') as "simulatedEvents",
                   count(*) filter (where decision = 'RECORDED') as "recordedEvents",
                   count(*) filter (where decision = 'SKIPPED') as "skippedEvents",
                   count(*) filter (where decision = 'ERROR') as "errorEvents",
                   min(event_time) as "oldestEventTime",
                   max(event_time) as "newestEventTime"
            from bounded_events
            group by shadow_allocation_id
            """;

    private ShadowCoverageSql() {
    }
}
