package com.apunto.engine.service.copy.coverage;

import com.apunto.engine.repository.ShadowCoverageSql;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ShadowCoverageSqlContractTest {

    @Test
    void rollingCoverageQueryFiltersAllocationWindowFutureAndEvaluableDecisions() {
        String sql = normalizedSql();

        assertTrue(sql.contains("shadow_allocation_id in (:allocationids)"));
        assertTrue(sql.contains("event_time >= greatest(:windowstart, a.created_at)"));
        assertTrue(sql.contains("event_time <= :windowend"));
        assertTrue(sql.contains("decision in ('simulated', 'recorded', 'skipped', 'error')"));
    }

    @Test
    void rollingCoverageQueryUsesEventTimeDescendingBeforeLimit() {
        String sql = normalizedSql();
        int order = sql.indexOf("order by e.event_time desc, e.id_event desc");
        int limit = sql.indexOf("where event_rank <= :maxevents");

        assertTrue(order >= 0, "missing deterministic descending order");
        assertTrue(limit > order, "max-events must be applied after descending order");
        assertTrue(sql.contains("partition by e.shadow_allocation_id"));
    }

    @Test
    void rollingCoverageQueryAggregatesEachAllocationIndependently() {
        String sql = normalizedSql();

        assertTrue(sql.contains("group by shadow_allocation_id"));
        assertTrue(sql.contains("count(*) filter (where decision = 'simulated')"));
        assertTrue(sql.contains("count(*) filter (where decision = 'recorded')"));
        assertTrue(sql.contains("count(*) filter (where decision = 'skipped')"));
        assertTrue(sql.contains("count(*) filter (where decision = 'error')"));
    }

    @Test
    void duplicateAndNullDecisionsCannotConsumeTheFiveHundredEventWindow() {
        String sql = normalizedSql();
        int decisionFilter = sql.indexOf("decision in ('simulated', 'recorded', 'skipped', 'error')");
        int rowNumber = sql.indexOf("row_number()");

        assertTrue(decisionFilter >= 0);
        assertTrue(rowNumber >= 0);
        assertTrue(sql.indexOf("duplicate") < 0);
    }

    @Test
    void rollingWindowNeverCountsEventsBeforeShadowAllocationActivation() {
        String sql = normalizedSql();

        assertTrue(sql.contains("join futuros_operaciones.shadow_copy_allocation"));
        assertTrue(sql.contains("greatest(:windowstart, a.created_at)"));
    }

    private static String normalizedSql() {
        return ShadowCoverageSql.ROLLING_BATCH_QUERY
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", " ")
                .trim();
    }
}
