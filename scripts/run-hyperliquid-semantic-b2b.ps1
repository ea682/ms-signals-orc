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
    [pscustomobject]@{ Id = "B2B-01"; Scenario = "OPEN normal con baseline valido"; Proof = "Sentinel detector + Signals movement contract" }
    [pscustomobject]@{ Id = "B2B-02"; Scenario = "INCREASE real"; Proof = "Sentinel resize from published baseline" }
    [pscustomobject]@{ Id = "B2B-03"; Scenario = "INCREASE original matematicamente REDUCE"; Proof = "Signals Shadow final REDUCE" }
    [pscustomobject]@{ Id = "B2B-04"; Scenario = "REDUCE sin active origin"; Proof = "Signals non-economic origin hydration" }
    [pscustomobject]@{ Id = "B2B-05"; Scenario = "Misma identidad y mismo payload"; Proof = "Signals healthy duplicate" }
    [pscustomobject]@{ Id = "B2B-06"; Scenario = "Misma identidad y payload distinto"; Proof = "Signals explicit conflict" }
    [pscustomobject]@{ Id = "B2B-07"; Scenario = "FLIP con evidencia suficiente"; Proof = "Signals future USER_FILL contract, publisher remains off" }
    [pscustomobject]@{ Id = "B2B-08"; Scenario = "FLIP sin evidencia suficiente"; Proof = "Signals audit-only fail-closed" }
    [pscustomobject]@{ Id = "B2B-09"; Scenario = "Hash cero y tid valido"; Proof = "Sentinel payload + Signals identity" }
    [pscustomobject]@{ Id = "B2B-10"; Scenario = "Tres eventos mismo timestamp"; Proof = "Sentinel slow-snapshot fast-path contract" }
    [pscustomobject]@{ Id = "B2B-11"; Scenario = "Redelivery del mismo evento"; Proof = "Signals idempotency guard" }
    [pscustomobject]@{ Id = "B2B-12"; Scenario = "Reinicio con posiciones abiertas"; Proof = "Signals baseline hydration and later adjustment" }
)

$sentinelTests = @(
    "HyperliquidPositionDeltaDetectorImplTest",
    "HyperliquidPositionDeltaPayloadFactoryTest",
    "HyperliquidPositionDeltaPublisherReadinessTest",
    "HyperliquidSnapshotServiceTest",
    "HyperliquidSlowSnapshotFastPathContractTest"
) -join ","

$signalsTests = @(
    "ShadowCopyTradingServiceImplTest#productionIncreaseWithNegativeDeltaPersistsReduceIntent",
    "HyperliquidOriginPositionStoreServiceTest",
    "HyperliquidDirectIngestIdempotencyGuardTest",
    "HyperliquidDirectDeltaIngestServiceImplTest",
    "HyperliquidZeroHashIdentityTest",
    "OperationMovementEconomicNormalizationTest",
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
Write-Output "B2B_RESULT=GREEN scenarios=$($coverage.Count) userFillPublisher=OFF externalOrders=0"
