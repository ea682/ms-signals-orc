package com.apunto.engine.dto.client;

import com.apunto.engine.shared.metric.MetricStrategyIdentity;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricStrategySnapshotDto {

    public static final String SOURCE_VERSION = "wallet_metric_economic_v2";
    public static final String RESPONSE_SOURCE = "ECONOMIC_V2_ACTIVE_GENERATION";

    public enum EvaluationMode {
        SUMMARY,
        FULL
    }

    private Integer metricVersion;
    private String sourceVersion;
    private String generationId;
    private String generationStatus;
    private OffsetDateTime generationActivatedAt;
    private OffsetDateTime computedAt;
    private OffsetDateTime dataAsOf;
    private String readMode;
    private String responseSource;
    private String calculatorVersion;
    private String policyVersion;
    private Double coveragePct;
    private String evidenceStatus;
    private Boolean factPayloadLoaded;
    private String walletId;
    private String strategyCode;
    private String scopeType;
    private String scopeValue;
    private String strategyKey;
    private String certificationStatus;
    private String degradationState;
    private boolean allowNewEntries;
    private boolean decisionFinal;
    @Builder.Default
    private List<String> qualityFlags = List.of();
    @Builder.Default
    private List<String> reasonCodes = List.of();
    private Integer completeCycles;
    private Integer historyDays;
    private Long dataFreshnessSeconds;
    private CoverageDto coverage;
    @Builder.Default
    private List<String> unknownEconomicFields = List.of();
    private EvaluationMode evaluationMode;
    private String decisionUse;
    private Boolean requiresFullSimulation;
    private Boolean allowsMoney;
    private Boolean eligibleForShadow;
    private Integer rankWithinStrategy;
    private Integer globalRank;
    private Map<String, Object> summary;
    private Map<String, Object> simulation;
    private SimulationMatrixDto simulationMatrix;
    private Map<String, WindowDto> windows;
    private Map<String, Object> financialMetrics;
    private Map<String, Object> scores;
    private Map<String, Object> operationalDecision;
    private Map<String, Object> fieldAvailability;
    private Map<String, Object> versions;

    public String canonicalStrategyKey() {
        return canonicalStrategyKey(walletId, strategyCode, scopeType, scopeValue);
    }

    public List<String> contractErrors() {
        Set<String> errors = new LinkedHashSet<>();
        if (!Integer.valueOf(2).equals(metricVersion)) errors.add("METRIC_VERSION_INVALID");
        if (!SOURCE_VERSION.equals(sourceVersion)) errors.add("SOURCE_VERSION_INVALID");
        if (blank(generationId)) errors.add("GENERATION_ID_REQUIRED");
        if (blank(generationStatus)) errors.add("GENERATION_STATUS_REQUIRED");
        else if (!"ACTIVE".equals(generationStatus)) errors.add("GENERATION_STATUS_INVALID");
        if (generationActivatedAt == null) errors.add("GENERATION_ACTIVATED_AT_REQUIRED");
        if (computedAt == null) errors.add("COMPUTED_AT_REQUIRED");
        if (dataAsOf == null) errors.add("DATA_AS_OF_REQUIRED");
        if (!"V2".equals(readMode)) errors.add("READ_MODE_INVALID");
        if (!RESPONSE_SOURCE.equals(responseSource)) errors.add("RESPONSE_SOURCE_INVALID");
        if (blank(calculatorVersion)) errors.add("CALCULATOR_VERSION_REQUIRED");
        if (blank(policyVersion)) errors.add("POLICY_VERSION_REQUIRED");
        if (coveragePct == null || !Double.isFinite(coveragePct)
                || coveragePct < 0 || coveragePct > 100) errors.add("COVERAGE_PCT_INVALID");
        if (blank(evidenceStatus)) errors.add("EVIDENCE_STATUS_REQUIRED");
        if (factPayloadLoaded == null) errors.add("FACT_PAYLOAD_LOADED_REQUIRED");
        if (requiresFullSimulation == null) errors.add("REQUIRES_FULL_SIMULATION_REQUIRED");
        if (blank(walletId)) errors.add("WALLET_ID_REQUIRED");
        if (blank(strategyCode)) errors.add("STRATEGY_CODE_REQUIRED");
        if (blank(scopeType)) errors.add("SCOPE_TYPE_REQUIRED");
        if (blank(scopeValue)) errors.add("SCOPE_VALUE_REQUIRED");
        if (blank(strategyKey)) errors.add("STRATEGY_KEY_REQUIRED");
        if (blank(certificationStatus)) errors.add("CERTIFICATION_STATUS_REQUIRED");
        if (blank(degradationState)) errors.add("DEGRADATION_STATE_REQUIRED");
        if (evaluationMode == null) errors.add("EVALUATION_MODE_REQUIRED");
        if (completeCycles == null || completeCycles < 0) errors.add("COMPLETE_CYCLES_INVALID");
        if (historyDays == null || historyDays < 0) errors.add("HISTORY_DAYS_INVALID");
        if (dataFreshnessSeconds == null || dataFreshnessSeconds < 0) errors.add("DATA_FRESHNESS_INVALID");
        if (coverage == null) errors.add("COVERAGE_REQUIRED");
        if (qualityFlags == null) errors.add("QUALITY_FLAGS_REQUIRED");
        if (reasonCodes == null) errors.add("REASON_CODES_REQUIRED");
        if (unknownEconomicFields == null) errors.add("UNKNOWN_ECONOMIC_FIELDS_REQUIRED");

        if (!blank(walletId) && !walletId.equals(walletId.toLowerCase(Locale.ROOT))) {
            errors.add("WALLET_ID_NOT_CANONICAL");
        }
        if (!blank(strategyCode) && !strategyCode.equals(strategyCode.toUpperCase(Locale.ROOT))) {
            errors.add("STRATEGY_CODE_NOT_CANONICAL");
        }
        if (!blank(scopeType) && !scopeType.equals(scopeType.toUpperCase(Locale.ROOT))) {
            errors.add("SCOPE_TYPE_NOT_CANONICAL");
        }
        if (!blank(scopeValue) && !scopeValue.equals(scopeValue.toUpperCase(Locale.ROOT))) {
            errors.add("SCOPE_VALUE_NOT_CANONICAL");
        }
        if (!blank(strategyKey) && !strategyKey.equals(canonicalStrategyKey())) {
            errors.add("STRATEGY_KEY_MISMATCH");
        }
        if (evaluationMode == EvaluationMode.SUMMARY) {
            if (decisionFinal) errors.add("SUMMARY_DECISION_FINAL_INVALID");
            if (allowNewEntries) errors.add("SUMMARY_ALLOW_NEW_ENTRIES_INVALID");
            if (!Boolean.FALSE.equals(allowsMoney)) errors.add("SUMMARY_ALLOWS_MONEY_INVALID");
            if (!Boolean.TRUE.equals(requiresFullSimulation)) {
                errors.add("SUMMARY_REQUIRES_FULL_SIMULATION_INVALID");
            }
        }
        if (evaluationMode == EvaluationMode.FULL && !Boolean.FALSE.equals(requiresFullSimulation)) {
            errors.add("FULL_REQUIRES_FULL_SIMULATION_INVALID");
        }
        return new ArrayList<>(errors);
    }

    public boolean isEligibleForShadow() {
        return contractErrors().isEmpty()
                && evaluationMode == EvaluationMode.FULL
                && decisionFinal
                && allowNewEntries
                && Boolean.TRUE.equals(eligibleForShadow)
                && Boolean.TRUE.equals(factPayloadLoaded)
                && "PASSED".equals(evidenceStatus)
                && simulationMatrixContractErrors().isEmpty()
                && coverage != null
                && coverage.isComplete()
                && safe(qualityFlags).isEmpty()
                && safe(unknownEconomicFields).isEmpty()
                && "CERTIFIED".equals(certificationStatus)
                && ("ACTIVE".equals(degradationState) || "WATCH".equals(degradationState));
    }

    public boolean isCopyGuardAllowed() {
        return contractErrors().isEmpty()
                && evaluationMode == EvaluationMode.FULL
                && windows != null
                && !windows.isEmpty()
                && "PASSED".equals(evidenceStatus)
                && decisionFinal
                && allowNewEntries
                && safe(unknownEconomicFields).isEmpty();
    }

    public List<String> institutionalFinancialContractErrors() {
        List<String> errors = new ArrayList<>();
        if (financialMetrics == null || financialMetrics.isEmpty()) {
            errors.add("FINANCIAL_METRICS_REQUIRED");
        }
        if (scores == null || scores.isEmpty()) errors.add("SEPARATE_SCORES_REQUIRED");
        if (operationalDecision == null || operationalDecision.isEmpty()) {
            errors.add("OPERATIONAL_DECISION_REQUIRED");
        }
        if (fieldAvailability == null || fieldAvailability.isEmpty()) {
            errors.add("FIELD_AVAILABILITY_REQUIRED");
        }
        if (versions == null || versions.isEmpty()) errors.add("ENGINE_VERSIONS_REQUIRED");
        errors.addAll(simulationMatrixContractErrors());
        return errors;
    }

    public List<String> simulationMatrixContractErrors() {
        if (simulationMatrix == null) return List.of("SIMULATION_MATRIX_REQUIRED");
        return simulationMatrix.contractErrors(strategyKey, generationId);
    }

    public boolean hasExecutableSimulationMatrix() {
        return simulationMatrix != null
                && simulationMatrix.isExecutableFor(strategyKey, generationId);
    }

    public boolean hasInstitutionalFinancialContract() {
        return institutionalFinancialContractErrors().isEmpty();
    }

    public static String canonicalStrategyKey(
            String walletId,
            String strategyCode,
            String scopeType,
            String scopeValue
    ) {
        return MetricStrategyIdentity.canonicalKey(walletId, strategyCode, scopeType, scopeValue);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static List<String> safe(List<String> values) {
        return values == null ? List.of("NULL_LIST") : values;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CoverageDto {
        private String status;
        private boolean complete;
        private Integer completeCycles;
        private Integer factsReturned;
        private Integer factsAvailable;
        private Boolean truncated;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WindowDto {
        private Integer days;
        private boolean mature;
        private boolean complete;
        private Integer cycles;
        private Double pnlNetUsd;
        private Double coveragePct;
        @Builder.Default
        private List<String> reasonCodes = List.of();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SimulationMatrixDto {
        public static final int EXPECTED_SCENARIO_COUNT = 44;
        private static final List<BigDecimal> CAPITAL_BANDS = List.of(
                new BigDecimal("100"),
                new BigDecimal("250"),
                new BigDecimal("500"),
                new BigDecimal("1000"),
                new BigDecimal("5000"),
                new BigDecimal("10000"),
                new BigDecimal("50000"),
                new BigDecimal("100000"),
                new BigDecimal("250000"),
                new BigDecimal("500000"),
                new BigDecimal("1000000")
        );
        private static final List<BigDecimal> LEVERAGES = List.of(
                new BigDecimal("5"),
                new BigDecimal("10"),
                new BigDecimal("15"),
                new BigDecimal("20")
        );

        private Boolean available;
        private String status;
        private String strategyKey;
        private String generationId;
        private String jobId;
        private OffsetDateTime completedAt;
        private Integer scenarioCount;
        private Integer expectedScenarioCount;
        @Builder.Default
        private List<SimulationScenarioDto> scenarios = List.of();
        @Builder.Default
        private List<String> reasonCodes = List.of();

        public List<String> contractErrors(String expectedStrategyKey, String expectedGenerationId) {
            Set<String> errors = new LinkedHashSet<>();
            String normalizedStatus = status == null ? "" : status.trim().toUpperCase(Locale.ROOT);
            if (available == null) errors.add("SIMULATION_MATRIX_AVAILABILITY_REQUIRED");
            if (!Set.of("COMPLETE", "INCOMPLETE", "UNKNOWN").contains(normalizedStatus)) {
                errors.add("SIMULATION_MATRIX_STATUS_INVALID");
            }
            if (blank(strategyKey)) errors.add("SIMULATION_MATRIX_STRATEGY_KEY_REQUIRED");
            else if (!Objects.equals(strategyKey, expectedStrategyKey)) {
                errors.add("SIMULATION_MATRIX_STRATEGY_KEY_MISMATCH");
            }
            if (blank(generationId)) errors.add("SIMULATION_MATRIX_GENERATION_ID_REQUIRED");
            else if (!Objects.equals(generationId, expectedGenerationId)) {
                errors.add("SIMULATION_MATRIX_GENERATION_MISMATCH");
            }
            if (!Integer.valueOf(EXPECTED_SCENARIO_COUNT).equals(expectedScenarioCount)) {
                errors.add("SIMULATION_MATRIX_EXPECTED_COUNT_INVALID");
            }
            if (scenarioCount == null || scenarioCount < 0) {
                errors.add("SIMULATION_MATRIX_SCENARIO_COUNT_INVALID");
            }
            if (scenarios == null) errors.add("SIMULATION_MATRIX_SCENARIOS_REQUIRED");
            if (reasonCodes == null) errors.add("SIMULATION_MATRIX_REASON_CODES_REQUIRED");

            List<SimulationScenarioDto> values = scenarios == null ? List.of() : scenarios;
            if (scenarioCount != null && scenarioCount != values.size()) {
                errors.add("SIMULATION_MATRIX_SCENARIO_COUNT_MISMATCH");
            }
            Set<Integer> indexes = new HashSet<>();
            Set<String> combinations = new HashSet<>();
            for (SimulationScenarioDto scenario : values) {
                if (scenario == null) {
                    errors.add("SIMULATION_MATRIX_SCENARIO_REQUIRED");
                    continue;
                }
                if (scenario.getScenarioIndex() == null
                        || scenario.getScenarioIndex() < 0
                        || scenario.getScenarioIndex() >= EXPECTED_SCENARIO_COUNT) {
                    errors.add("SIMULATION_MATRIX_SCENARIO_INDEX_INVALID");
                } else if (!indexes.add(scenario.getScenarioIndex())) {
                    errors.add("SIMULATION_MATRIX_SCENARIO_INDEX_DUPLICATE");
                }
                if (scenario.getCapitalUsd() == null || scenario.getCapitalUsd().signum() <= 0
                        || scenario.getTargetLeverage() == null || scenario.getTargetLeverage().signum() <= 0) {
                    errors.add("SIMULATION_MATRIX_SCENARIO_BAND_INVALID");
                } else if (!combinations.add(number(scenario.getCapitalUsd()) + "|"
                        + number(scenario.getTargetLeverage()))) {
                    errors.add("SIMULATION_MATRIX_SCENARIO_BAND_DUPLICATE");
                }
            }

            if ("COMPLETE".equals(normalizedStatus)) {
                if (!Boolean.TRUE.equals(available)) errors.add("SIMULATION_MATRIX_COMPLETE_NOT_AVAILABLE");
                if (!Integer.valueOf(EXPECTED_SCENARIO_COUNT).equals(scenarioCount)
                        || values.size() != EXPECTED_SCENARIO_COUNT) {
                    errors.add("SIMULATION_MATRIX_COMPLETE_COUNT_INVALID");
                }
                if (!expectedIndexes().equals(indexes)) {
                    errors.add("SIMULATION_MATRIX_COMPLETE_INDEX_SET_INVALID");
                }
                if (!expectedCombinations().equals(combinations)) {
                    errors.add("SIMULATION_MATRIX_COMPLETE_BAND_SET_INVALID");
                }
                if (blank(jobId)) errors.add("SIMULATION_MATRIX_JOB_ID_REQUIRED");
                if (completedAt == null) errors.add("SIMULATION_MATRIX_COMPLETED_AT_REQUIRED");
            } else if (Boolean.TRUE.equals(available)) {
                errors.add("SIMULATION_MATRIX_UNAVAILABLE_STATUS_MISMATCH");
            }
            if (!"COMPLETE".equals(normalizedStatus)
                    && reasonCodes != null
                    && reasonCodes.stream().noneMatch(code -> code != null && !code.isBlank())) {
                errors.add("SIMULATION_MATRIX_UNAVAILABLE_REASON_REQUIRED");
            }
            return new ArrayList<>(errors);
        }

        public boolean isExecutableFor(String expectedStrategyKey, String expectedGenerationId) {
            return contractErrors(expectedStrategyKey, expectedGenerationId).isEmpty()
                    && Boolean.TRUE.equals(available)
                    && "COMPLETE".equalsIgnoreCase(status);
        }

        private static Set<Integer> expectedIndexes() {
            Set<Integer> result = new HashSet<>();
            for (int index = 0; index < EXPECTED_SCENARIO_COUNT; index++) result.add(index);
            return result;
        }

        private static Set<String> expectedCombinations() {
            Set<String> result = new HashSet<>();
            for (BigDecimal capital : CAPITAL_BANDS) {
                for (BigDecimal leverage : LEVERAGES) {
                    result.add(number(capital) + "|" + number(leverage));
                }
            }
            return result;
        }

        private static String number(BigDecimal value) {
            return value.stripTrailingZeros().toPlainString();
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SimulationScenarioDto {
        private Integer scenarioIndex;
        private BigDecimal capitalUsd;
        private BigDecimal targetLeverage;
        private BigDecimal targetNotionalUsd;
        private BigDecimal targetMarginUsd;
        private Integer positionsCopied;
        private Integer positionsOmitted;
        private BigDecimal movementCoverage;
        private BigDecimal notionalCoverage;
        private BigDecimal exposureCoverage;
        private BigDecimal roundingLossUsd;
        private Integer minNotionalSkips;
        private BigDecimal feesUsd;
        private BigDecimal fundingUsd;
        private BigDecimal slippageUsd;
        private BigDecimal grossPnlUsd;
        private BigDecimal netPnlUsd;
        private BigDecimal drawdownPct;
        private BigDecimal profitFactor;
        private BigDecimal liquidationRisk;
        private String modeledEconomicsStatus;
        private Map<String, Object> economicEvidence;
        private Map<String, Object> targetPortfolio;
        private String calculatorVersion;
        private String policyVersion;
        private Map<String, Object> fieldAvailability;
    }

}
