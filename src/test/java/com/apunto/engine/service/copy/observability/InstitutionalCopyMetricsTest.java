package com.apunto.engine.service.copy.observability;

import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InstitutionalCopyMetricsTest {

    @Test
    void classifiesGuardAndPortfolioSignalsWithoutTreatingNormalAllowAsRecovery() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        InstitutionalCopyMetrics.observeGuard(registry,
                CopyStrategyGuardDecision.warn("HEALTH_WATCH", "", 0.75), true);
        InstitutionalCopyMetrics.observeGuard(registry,
                CopyStrategyGuardDecision.reduce("HEALTH_REDUCE", "", 0.5), true);
        InstitutionalCopyMetrics.observeGuard(registry,
                CopyStrategyGuardDecision.blocked("PORTFOLIO_CORRELATION_LIMIT", ""), false);
        InstitutionalCopyMetrics.observeGuard(registry,
                CopyStrategyGuardDecision.blocked("CAPACITY_EXCEEDED", ""), false);
        InstitutionalCopyMetrics.observeGuard(registry,
                CopyStrategyGuardDecision.allow(), true);
        InstitutionalCopyMetrics.observeGuard(registry,
                new CopyStrategyGuardDecision(true, "GRADUAL_RECOVERY_CONFIRMED", "", null,
                        "ALLOW", 0.25, "KEEP"), true);

        assertEquals(1.0, counter(registry, "signals.copy_guard.warning.total"));
        assertEquals(1.0, counter(registry, "signals.copy_guard.reduce.total"));
        assertEquals(2.0, counter(registry, "signals.copy_guard.pause.total"));
        assertEquals(1.0, counter(registry, "signals.copy_guard.recovery.total"));
        assertEquals(1.0, counter(registry, "signals.portfolio.correlation_limit.total"));
        assertEquals(1.0, counter(registry, "signals.portfolio.capacity_limit.total"));
    }

    private double counter(SimpleMeterRegistry registry, String name) {
        return registry.get(name).counter().count();
    }
}
