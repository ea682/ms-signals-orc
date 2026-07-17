package com.apunto.engine.service.copy.dispatch;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;
import org.springframework.stereotype.Component;

@Component
public final class CopyIdempotencyKeyFactory {

    public String create(CopyDispatchIdentity identity) {
        Objects.requireNonNull(identity, "identity is required");
        String sourceEventId = required(identity.sourceEventId(), "sourceEventId");
        String canonical = String.join("|",
                "copy-dispatch-v2",
                normalize(identity.userId()),
                identity.userCopyAllocationId() == null ? "legacy" : identity.userCopyAllocationId().toString(),
                normalizeUpper(identity.executionMode()),
                normalizeUpper(identity.strategyCode()),
                normalizeUpper(identity.scopeType()),
                normalizeUpper(identity.scopeValue()),
                normalize(identity.generationId()),
                sourceEventId,
                normalizeUpper(identity.copyIntent()));
        return sha256(canonical);
    }

    public String hashPayload(String payload) {
        return sha256(payload == null ? "" : payload);
    }

    public String clientOrderId(String idempotencyKey) {
        String key = required(idempotencyKey, "idempotencyKey");
        if (key.length() < 32) throw new IllegalArgumentException("idempotencyKey is too short");
        return "cpO_" + key.substring(0, 32);
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is not available", impossible);
        }
    }

    // IMPORTANT:
    // Idempotency is scoped to the exact user copy allocation and immutable source
    // event. Different strategies may intentionally copy the same source event once
    // each. Do not deduplicate globally by wallet or origin order.
    private String normalize(String value) {
        return value == null || value.isBlank() ? "NA" : value.trim();
    }

    private String normalizeUpper(String value) {
        return normalize(value).toUpperCase(Locale.ROOT).replace('-', '_');
    }

    private String required(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " is required");
        return value.trim();
    }
}
