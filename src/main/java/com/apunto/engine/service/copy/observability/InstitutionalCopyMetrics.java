package com.apunto.engine.service.copy.observability;

import com.apunto.engine.service.copy.CopyStrategyGuardDecision;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Locale;

public final class InstitutionalCopyMetrics {

    private InstitutionalCopyMetrics() {
    }

    public static void observeGuard(MeterRegistry registry,
                                    CopyStrategyGuardDecision decision,
                                    boolean runtimeAllowed) {
        if (registry == null || decision == null) return;
        String action = normalize(decision.action());
        String tokens = normalize(String.join("|",
                text(decision.reason()), text(decision.detail()),
                text(decision.statusWhenBlocked()), text(decision.action())));

        if ("WARNING".equals(action)) {
            registry.counter("signals.copy_guard.warning.total").increment();
        }
        if ("REDUCE_CAPITAL".equals(action)) {
            registry.counter("signals.copy_guard.reduce.total").increment();
        }
        if (!runtimeAllowed) {
            registry.counter("signals.copy_guard.pause.total").increment();
        }
        if (runtimeAllowed && "ALLOW".equals(action) && tokens.contains("RECOVERY")) {
            registry.counter("signals.copy_guard.recovery.total").increment();
        }
        if (tokens.contains("CORRELATION") && tokens.contains("LIMIT")) {
            registry.counter("signals.portfolio.correlation_limit.total").increment();
        }
        if (tokens.contains("CAPACITY") && (tokens.contains("LIMIT") || tokens.contains("EXCEEDED"))) {
            registry.counter("signals.portfolio.capacity_limit.total").increment();
        }
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private static String text(String value) {
        return value == null ? "" : value;
    }
}
