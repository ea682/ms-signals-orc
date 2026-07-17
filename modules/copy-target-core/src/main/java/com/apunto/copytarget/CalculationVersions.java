package com.apunto.copytarget;

import java.util.Objects;

public record CalculationVersions(
        String strategyVersion,
        String sizingPolicyVersion,
        String symbolMappingVersion
) {
    public CalculationVersions {
        strategyVersion = requireVersion(strategyVersion, "strategyVersion");
        sizingPolicyVersion = requireVersion(sizingPolicyVersion, "sizingPolicyVersion");
        symbolMappingVersion = requireVersion(symbolMappingVersion, "symbolMappingVersion");
    }

    private static String requireVersion(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
