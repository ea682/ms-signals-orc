package com.apunto.engine.service.movement;

import java.util.Locale;

public final class AuthoritativeMovementIdentity {
    private AuthoritativeMovementIdentity() {
    }

    public static String userFill(
            String exchange,
            String wallet,
            long tid,
            String hash
    ) {
        if (tid <= 0L) {
            throw new IllegalArgumentException("tid must be positive");
        }
        return String.join(":",
                required(exchange, "exchange").toLowerCase(Locale.ROOT),
                "user-fill",
                required(wallet, "wallet").toLowerCase(Locale.ROOT),
                Long.toString(tid),
                required(hash, "hash").toLowerCase(Locale.ROOT)
        );
    }

    public static String sourceAwareMovementMaterial(
            String baseMaterial,
            String sourceEventId,
            Long sourceSequence
    ) {
        String base = baseMaterial == null ? "" : baseMaterial;
        if ((sourceEventId == null || sourceEventId.isBlank())
                && sourceSequence == null) {
            return base;
        }
        return String.join("|",
                base,
                "sourceEventId=" + safe(sourceEventId),
                "sourceSequence=" + (sourceSequence == null ? "NA" : sourceSequence)
        );
    }

    private static String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value.trim();
    }

    private static String safe(String value) {
        return value == null || value.isBlank() ? "NA" : value.trim();
    }
}
