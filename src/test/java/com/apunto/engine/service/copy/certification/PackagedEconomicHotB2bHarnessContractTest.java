package com.apunto.engine.service.copy.certification;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PackagedEconomicHotB2bHarnessContractTest {

    private static final Path HARNESS = Path.of("scripts", "run-packaged-economic-hot-b2b.ps1");

    @Test
    void defaultsResolveCurrentArtifactsAndAuthoritativeBaselineInsideTheRepository() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("ms-sentinel-hyperliquid-1.0.39.jar"));
        assertTrue(script.contains("ms-signals-orc-1.4.38.jar"));
        assertTrue(script.contains("ms-wallet-metric-etl-1.0.23.jar"));
        assertTrue(script.contains("[string]$SignalsBaselineSchemaPath = 'target\\audit-schema-baseline.sql'"));
        assertTrue(script.contains("[string]$SignalsBaselineHistoryPath = 'target\\audit-flyway-history.sql'"));
        assertTrue(script.contains("[ValidatePattern('^\\d+$')][string]$SignalsBaselineVersion = '202608010001'"));
        assertTrue(script.contains("[string]$EtlBaselineHistoryPath = 'target\\audit-etl-flyway-history.sql'"));
        assertTrue(script.contains("[ValidatePattern('^\\d+$')][string]$EtlBaselineVersion = '202608220001'"));
        assertTrue(script.contains("$signalsBaselineSchema = Resolve-RepoPath $SignalsBaselineSchemaPath"));
        assertTrue(script.contains("$signalsBaselineHistory = Resolve-RepoPath $SignalsBaselineHistoryPath"));
        assertTrue(script.contains("$etlBaselineHistory = Resolve-RepoPath $EtlBaselineHistoryPath"));
        assertTrue(script.contains("ETL historical baseline restore failed"));
        assertTrue(script.contains("($baseTs + 4500) 259.57 'Close Long'"));
        assertTrue(script.contains("[switch]$ValidateInputsOnly"));
        assertTrue(script.contains("if ($ValidateInputsOnly)"));
        assertTrue(script.contains("$results.TRUE_IDEMPOTENCY_CONFLICT = 'GREEN'"));
        assertTrue(script.contains("$results.SENTINEL_CRASH_RESTART = 'GREEN'"));
        assertTrue(script.contains("$results.MULTI_REPLICA_SIGNALS = 'GREEN'"));
        assertTrue(script.contains("$results.ETL_CRASH_RESTART = 'GREEN'"));
        assertTrue(script.contains("$results.MULTI_REPLICA_ETL = 'GREEN'"));
        assertTrue(script.contains("$results.HYPERLIQUID_429_RESPONSES_CONSUMED = 2"));
        assertTrue(script.contains("$submittedIdentities.Add"));
        assertTrue(script.contains("$results.VALID_AUTHORITATIVE_FILLS = $submittedIdentities.Count"));
        assertTrue(script.contains("$results.CONSERVATION_PROOF"));
        assertTrue(script.contains("$results.UNPROTECTED_ACCEPTED_EVENTS"));
        assertTrue(script.contains("$results.DUPLICATE_JOURNAL_IDENTITIES"));
        assertTrue(script.indexOf("$trueConflictTid = $baseTid + 261")
                        > script.indexOf("$results.POSITION_DELTA_AUDIT_ONLY = 'GREEN'"),
                "the terminal economic contradiction must run after every "
                        + "scenario that requires Sentinel readiness");
        assertTrue(script.contains("journal_economic_contradiction"),
                "the restart must prove the expected global fail-closed state");

        assertFalse(script.contains("signals-hot-live-cert-evidence-20260824"));
        assertFalse(script.contains("ms-sentinel-hyperliquid-1.0.37.jar"));
        assertFalse(script.contains("ms-sentinel-hyperliquid-1.0.38.jar"));
        assertFalse(script.contains("ms-signals-orc-1.4.37.jar"));
        assertFalse(script.contains("ms-wallet-metric-etl-1.0.21.jar"));
        assertFalse(script.contains("ms-wallet-metric-etl-1.0.22.jar"));
        assertFalse(script.contains("flyway_schema_history|202607140002"));
        assertFalse(script.contains("1787361035644"));
        assertFalse(script.contains("$results.VALID_AUTHORITATIVE_FILLS = 30"));
        assertFalse(script.contains("$results.ECONOMICALLY_PROCESSED = 29"));
    }
}
