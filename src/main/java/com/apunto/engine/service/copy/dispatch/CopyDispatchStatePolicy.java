package com.apunto.engine.service.copy.dispatch;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class CopyDispatchStatePolicy {

    private static final Map<String, Set<String>> TRANSITIONS = Map.ofEntries(
            Map.entry("CREATED", Set.of("DISPATCHING", "REJECTED")),
            Map.entry("DISPATCHING", Set.of("NEW", "PARTIALLY_FILLED", "FILLED", "RECONCILING", "REJECTED")),
            Map.entry("ACKNOWLEDGED", Set.of("NEW", "PARTIALLY_FILLED", "FILLED", "RECONCILING", "PERSISTENCE_PENDING")),
            Map.entry("NEW", Set.of("NEW", "RECONCILING", "PARTIALLY_FILLED", "FILLED", "REJECTED", "MANUAL_REVIEW")),
            Map.entry("PARTIALLY_FILLED", Set.of("PARTIALLY_FILLED", "FILLED", "RECONCILING", "PERSISTENCE_PENDING", "PERSISTED", "MANUAL_REVIEW")),
            Map.entry("FILLED", Set.of("FILLED", "PERSISTENCE_PENDING", "PERSISTED", "RECONCILING", "MANUAL_REVIEW")),
            Map.entry("RECONCILING", Set.of("RECONCILING", "NEW", "PARTIALLY_FILLED", "FILLED", "PERSISTENCE_PENDING", "PERSISTED", "REJECTED", "MANUAL_REVIEW")),
            Map.entry("PERSISTENCE_PENDING", Set.of("PERSISTENCE_PENDING", "RECONCILING", "PARTIALLY_FILLED", "FILLED", "PERSISTED", "MANUAL_REVIEW")),
            Map.entry("PERSISTED", Set.of("PERSISTED", "RECONCILING", "MANUAL_REVIEW")),
            Map.entry("REJECTED", Set.of("REJECTED")),
            Map.entry("FAILED_FINAL", Set.of("FAILED_FINAL", "MANUAL_REVIEW")),
            Map.entry("CANCELLED", Set.of("CANCELLED")),
            Map.entry("MANUAL_REVIEW", Set.of("MANUAL_REVIEW"))
    );

    private CopyDispatchStatePolicy() {
    }

    public static boolean mayAuthorizeSend(String currentStatus) {
        return "CREATED".equals(normalize(currentStatus));
    }

    public static boolean mayTransition(String currentStatus, String nextStatus) {
        String current = normalize(currentStatus);
        String next = normalize(nextStatus);
        return current != null && next != null
                && TRANSITIONS.getOrDefault(current, Set.of()).contains(next);
    }

    public static boolean terminalWithoutAutomaticSend(String status) {
        return Set.of("PERSISTED", "REJECTED", "FAILED_FINAL", "CANCELLED", "MANUAL_REVIEW")
                .contains(normalize(status));
    }

    public static void requireTransition(String currentStatus, String nextStatus) {
        if (!mayTransition(currentStatus, nextStatus)) {
            throw new IllegalStateException("Invalid copy dispatch transition: "
                    + normalize(currentStatus) + " -> " + normalize(nextStatus));
        }
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) return null;
        return value.trim().toUpperCase(Locale.ROOT);
    }
}
