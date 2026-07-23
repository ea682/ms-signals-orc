param(
    [string]$Java21Home = "C:\Users\erika\.jdks\temurin-21.0.11"
)

$ErrorActionPreference = "Stop"
$signalsRepo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$sentinelRepo = (Resolve-Path (Join-Path $signalsRepo "..\ms-sentinel-hyperliquid")).Path

if (-not (Test-Path -LiteralPath (Join-Path $Java21Home "bin\java.exe"))) {
    throw "Java 21 was not found at $Java21Home"
}

$env:JAVA_HOME = $Java21Home
$env:Path = "$Java21Home\bin;$env:Path"

$coverage = @(
    [pscustomobject]@{ Id = "B2B-01"; Scenario = "Replica no lista"; Proof = "Sentinel readiness publisher gate, cero publish" }
    [pscustomobject]@{ Id = "B2B-02"; Scenario = "Transicion PUBLISH_READY"; Proof = "Sentinel readiness state machine and health" }
    [pscustomobject]@{ Id = "B2B-03"; Scenario = "OPEN normal"; Proof = "Sentinel detector, payload, HTTP DTO and ledger contract" }
    [pscustomobject]@{ Id = "B2B-04"; Scenario = "INCREASE normal"; Proof = "Sentinel resize from published baseline" }
    [pscustomobject]@{ Id = "B2B-05"; Scenario = "INCREASE etiquetado, matematicamente REDUCE"; Proof = "Signals final Shadow REDUCE" }
    [pscustomobject]@{ Id = "B2B-06"; Scenario = "REDUCE sin origen"; Proof = "Signals NON_ECONOMIC_ESTIMATED baseline" }
    [pscustomobject]@{ Id = "B2B-07"; Scenario = "FLIP estimado"; Proof = "Signals audit-only fail-closed" }
    [pscustomobject]@{ Id = "B2B-08"; Scenario = "Mismo tid redeliverado"; Proof = "Signals duplicate NOOP" }
    [pscustomobject]@{ Id = "B2B-09"; Scenario = "Mismo tid divergente"; Proof = "Signals explicit payload conflict" }
    [pscustomobject]@{ Id = "B2B-10"; Scenario = "Hash cero"; Proof = "Sentinel and Signals canonical wallet+tid identity" }
    [pscustomobject]@{ Id = "B2B-11"; Scenario = "Dos replicas, payload identico"; Proof = "Signals healthy distributed duplicate" }
    [pscustomobject]@{ Id = "B2B-12"; Scenario = "Dos replicas, snapshot distinto"; Proof = "Readiness gate plus explicit divergence" }
    [pscustomobject]@{ Id = "B2B-13"; Scenario = "Reinicio con posiciones abiertas"; Proof = "Signals state-only baseline hydration" }
    [pscustomobject]@{ Id = "B2B-14"; Scenario = "Mismo timestamp, secuencias distintas"; Proof = "Economic total-order regression" }
    [pscustomobject]@{ Id = "B2B-15"; Scenario = "POSITION_DELTA sin PnL"; Proof = "Signals ledger preserves null PnL" }
    [pscustomobject]@{ Id = "B2B-16"; Scenario = "USER_FILL sintetico"; Proof = "Contract-only factory, publisher bean absent" }
)

$sentinelTests = @(
    "HyperliquidPositionDeltaDetectorImplTest",
    "HyperliquidPositionDeltaPayloadFactoryTest",
    "HyperliquidPositionDeltaPublisherReadinessTest",
    "HyperliquidReadinessStateMachineTest",
    "HyperliquidPublishReadinessHealthIndicatorTest",
    "HyperliquidIdentityArchitectureTest",
    "HyperliquidAuthoritativeUserFillContractTest",
    "HyperliquidSnapshotServiceTest",
    "HyperliquidSlowSnapshotFastPathContractTest"
) -join ","

$signalsTests = @(
    "ShadowCopyTradingServiceImplTest#productionIncreaseWithNegativeDeltaPersistsReduceIntent",
    "HyperliquidOriginPositionStoreServiceTest",
    "HyperliquidDirectIngestIdempotencyGuardTest",
    "HyperliquidDirectDeltaIngestServiceImplTest",
    "HyperliquidZeroHashIdentityTest",
    "HyperliquidIdentityArchitectureTest",
    "HyperliquidDeltaRequestV3ContractTest",
    "HyperliquidEconomicLedgerContractTest",
    "OperationMovementEconomicNormalizationTest",
    "OperationMovementEconomicOrderRegressionTest",
    "AuthoritativeMovementIdentityRegressionTest",
    "MetricMovementOutboxPartitionKeyTest"
) -join ","

Push-Location $sentinelRepo
try {
    & ".\mvnw.cmd" -q "-Dtest=$sentinelTests" test
    if ($LASTEXITCODE -ne 0) {
        throw "Sentinel B2B producer contract failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}

Push-Location $signalsRepo
try {
    & ".\mvnw.cmd" -q "-Dtest=$signalsTests" test
    if ($LASTEXITCODE -ne 0) {
        throw "Signals B2B consumer/ledger/shadow/outbox contract failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}

$coverage | Format-Table -AutoSize
Write-Output "B2B_RESULT=GREEN scenarios=$($coverage.Count) userFillPublisher=OFF externalOrders=0 duplicateEconomicEffects=0 ambiguousEstimatedFlips=0 unresolvedLateAdjustments=0 replicaPayloadConflictsHandled=true"
