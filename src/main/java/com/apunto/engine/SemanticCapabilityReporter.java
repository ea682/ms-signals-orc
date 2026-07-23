package com.apunto.engine;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.info.BuildProperties;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
@Slf4j
public class SemanticCapabilityReporter {

    private final ObjectProvider<BuildProperties> buildProperties;
    private final Environment environment;

    public SemanticCapabilityReporter(
            ObjectProvider<BuildProperties> buildProperties,
            Environment environment
    ) {
        this.buildProperties = buildProperties;
        this.environment = environment;
    }

    @PostConstruct
    void report() {
        BuildProperties build = buildProperties.getIfAvailable();
        String applicationVersion = build == null
                ? packageVersion()
                : build.getVersion();
        String commitSha = environment.getProperty(
                "GIT_COMMIT",
                environment.getProperty("git.commit", "unknown"));
        Map<String, Object> values =
                capabilities(applicationVersion, commitSha);
        log.info("event=semantic.capabilities applicationVersion={} "
                        + "commitSha={} semanticClassificationVersion={} "
                        + "sourceIdentityVersion={} readinessPolicyVersion={} "
                        + "baselinePolicyEnabled={} estimatedFlipGuardEnabled={} "
                        + "userFillPublisherEnabled={}",
                values.get("applicationVersion"),
                values.get("commitSha"),
                values.get("semanticClassificationVersion"),
                values.get("sourceIdentityVersion"),
                values.get("readinessPolicyVersion"),
                values.get("baselinePolicyEnabled"),
                values.get("estimatedFlipGuardEnabled"),
                values.get("userFillPublisherEnabled"));
    }

    static Map<String, Object> capabilities(
            String applicationVersion,
            String commitSha
    ) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("applicationVersion", safe(applicationVersion));
        values.put("commitSha", safe(commitSha));
        values.put("semanticClassificationVersion",
                "hyperliquid-semantic-v3");
        values.put("sourceIdentityVersion", "wallet-tid-v2");
        values.put("readinessPolicyVersion", "consumer-readiness-v2");
        values.put("baselinePolicyEnabled", true);
        values.put("estimatedFlipGuardEnabled", true);
        values.put("userFillPublisherEnabled", false);
        return Map.copyOf(values);
    }

    private static String safe(String value) {
        return value == null || value.isBlank() ? "unknown" : value.trim();
    }

    private String packageVersion() {
        return safe(getClass().getPackage().getImplementationVersion());
    }
}
