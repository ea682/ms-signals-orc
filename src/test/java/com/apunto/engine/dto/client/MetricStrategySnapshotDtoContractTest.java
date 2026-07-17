package com.apunto.engine.dto.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MetricStrategySnapshotDtoContractTest {

    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void deserializesSummaryAsNonOperable() throws Exception {
        MetricStrategySnapshotDto summary = fixture("metric-v2-summary.json").getFirst();

        assertEquals(List.of(), summary.contractErrors());
        assertEquals("0xabc|LONG_ONLY|DIRECTION|LONG", summary.canonicalStrategyKey());
        assertEquals(MetricStrategySnapshotDto.EvaluationMode.SUMMARY, summary.getEvaluationMode());
        assertFalse(summary.isDecisionFinal());
        assertFalse(summary.isAllowNewEntries());
        assertTrue(Boolean.TRUE.equals(summary.getRequiresFullSimulation()));
    }

    @Test
    void deserializesFullWithoutTurningUnknownIntoZero() throws Exception {
        MetricStrategySnapshotDto full = fixture("metric-v2-full.json").getFirst();

        assertEquals(List.of(), full.contractErrors());
        assertEquals(MetricStrategySnapshotDto.EvaluationMode.FULL, full.getEvaluationMode());
        assertTrue(full.isEligibleForShadow());
        assertTrue(full.hasInstitutionalFinancialContract());
        assertEquals(12.34, ((Number) full.getSimulation().get("copyNetPnlUsd")).doubleValue());
        assertNotNull(full.getSimulationMatrix());
        assertEquals(44, full.getSimulationMatrix().getScenarioCount());
        assertEquals(44, full.getSimulationMatrix().getScenarios().size());
        assertTrue(full.getSimulationMatrix().isExecutableFor(
                full.getStrategyKey(), full.getGenerationId()));
    }

    @Test
    void missingOrCrossGenerationSimulationMatrixFailsClosed() throws Exception {
        MetricStrategySnapshotDto full = fixture("metric-v2-full.json").getFirst();
        full.setSimulationMatrix(null);

        assertTrue(full.institutionalFinancialContractErrors()
                .contains("SIMULATION_MATRIX_REQUIRED"));
        assertFalse(full.isEligibleForShadow());

        full = fixture("metric-v2-full.json").getFirst();
        full.getSimulationMatrix().setGenerationId("another-generation");

        assertTrue(full.institutionalFinancialContractErrors()
                .contains("SIMULATION_MATRIX_GENERATION_MISMATCH"));
        assertFalse(full.isEligibleForShadow());
    }

    @Test
    void preservesEveryGuardWindow() throws Exception {
        MetricStrategySnapshotDto guard = fixture("metric-v2-copy-guard.json").getFirst();

        assertEquals(List.of(), guard.contractErrors());
        assertEquals(Set.of("1d", "3d", "1w", "2w", "3w", "1mo", "2mo", "3mo", "6mo", "9mo", "1y", "2y", "all"),
                guard.getWindows().keySet());
    }

    @Test
    void missingGenerationAndMismatchedIdentityFailClosed() throws Exception {
        MetricStrategySnapshotDto full = fixture("metric-v2-full.json").getFirst();
        full.setGenerationId(null);
        full.setStrategyKey("0xabc|LONG_ONLY");

        assertTrue(full.contractErrors().contains("GENERATION_ID_REQUIRED"));
        assertTrue(full.contractErrors().contains("STRATEGY_KEY_MISMATCH"));
    }

    @Test
    void missingGenerationProvenanceAndEvidenceMetadataFailsClosed() throws Exception {
        MetricStrategySnapshotDto full = fixture("metric-v2-full.json").getFirst();
        ObjectNode incomplete = mapper.valueToTree(full);
        for (String field : List.of(
                "generationStatus", "readMode", "responseSource", "calculatorVersion",
                "policyVersion", "coveragePct", "evidenceStatus", "factPayloadLoaded",
                "requiresFullSimulation")) {
            incomplete.remove(field);
        }
        MetricStrategySnapshotDto decoded = mapper.treeToValue(incomplete, MetricStrategySnapshotDto.class);

        assertTrue(decoded.contractErrors().containsAll(List.of(
                "GENERATION_STATUS_REQUIRED", "READ_MODE_INVALID", "RESPONSE_SOURCE_INVALID",
                "CALCULATOR_VERSION_REQUIRED", "POLICY_VERSION_REQUIRED", "COVERAGE_PCT_INVALID",
                "EVIDENCE_STATUS_REQUIRED", "FACT_PAYLOAD_LOADED_REQUIRED",
                "REQUIRES_FULL_SIMULATION_REQUIRED"
        )));
        assertFalse(decoded.isEligibleForShadow());
    }

    private List<MetricStrategySnapshotDto> fixture(String name) throws Exception {
        try (InputStream input = getClass().getResourceAsStream("/contracts/" + name)) {
            assertNotNull(input, name);
            return mapper.readValue(input, new TypeReference<>() {});
        }
    }
}
