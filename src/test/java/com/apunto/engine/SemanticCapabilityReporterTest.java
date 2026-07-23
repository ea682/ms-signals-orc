package com.apunto.engine;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SemanticCapabilityReporterTest {

    @Test
    void reportsEveryDeploymentCriticalCapabilityWithoutSecrets() {
        Map<String, Object> capabilities =
                SemanticCapabilityReporter.capabilities("1.4.31", "abc123");

        assertEquals("1.4.31", capabilities.get("applicationVersion"));
        assertEquals("abc123", capabilities.get("commitSha"));
        assertEquals("hyperliquid-semantic-v3",
                capabilities.get("semanticClassificationVersion"));
        assertEquals("wallet-tid-v2",
                capabilities.get("sourceIdentityVersion"));
        assertEquals("consumer-readiness-v2",
                capabilities.get("readinessPolicyVersion"));
        assertEquals(true, capabilities.get("baselinePolicyEnabled"));
        assertEquals(true,
                capabilities.get("estimatedFlipGuardEnabled"));
        assertEquals(false, capabilities.get("userFillPublisherEnabled"));
    }
}
