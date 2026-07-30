param(
    [string]$Java21Home = "C:\Users\erika\.jdks\graalvm-ce-21.0.2"
)

$ErrorActionPreference = "Stop"
$signalsRepo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$sentinelRepo = (Resolve-Path (
        Join-Path $signalsRepo "..\ms-sentinel-hyperliquid"
)).Path
$etlRepo = (Resolve-Path (
        Join-Path $signalsRepo "..\ms-wallet-metric-etl"
)).Path
$b2bDir = Join-Path $signalsRepo "target\economic-basis-b2b"
$payload = Join-Path $b2bDir "signals-payloads.json"
$summary = Join-Path $b2bDir "summary.json"

if (-not (Test-Path -LiteralPath (
        Join-Path $Java21Home "bin\java.exe"
))) {
    throw "Java 21 was not found at $Java21Home"
}

New-Item -ItemType Directory -Force -Path $b2bDir | Out-Null
$env:JAVA_HOME = $Java21Home
$env:Path = "$Java21Home\bin;$env:Path"

Push-Location $sentinelRepo
try {
    & ".\mvnw.cmd" `
        "-Dtest=HyperliquidAuthoritativeUserFillPayloadFactoryTest" test
    if ($LASTEXITCODE -ne 0) {
        throw "Sentinel authoritative source contract failed: $LASTEXITCODE"
    }
} finally {
    Pop-Location
}

Push-Location $signalsRepo
try {
    & ".\mvnw.cmd" `
        "-Dtest=AuthoritativeEconomicBasisPropagationTest" `
        "-DeconomicBasisB2bOutput=$payload" test
    if ($LASTEXITCODE -ne 0) {
        throw "Signals producer/ledger/outbox contract failed: $LASTEXITCODE"
    }
} finally {
    Pop-Location
}

if (-not (Test-Path -LiteralPath $payload)) {
    throw "Signals did not generate $payload"
}

Push-Location $etlRepo
try {
    & ".\mvnw.cmd" `
        "-Dtest=SignalsEconomicBasisB2bTest" `
        "-DeconomicBasisB2bInput=$payload" `
        "-DeconomicBasisB2bSummary=$summary" test
    if ($LASTEXITCODE -ne 0) {
        throw "ETL consumer/adapter/economic contract failed: $LASTEXITCODE"
    }
} finally {
    Pop-Location
}

if (-not (Test-Path -LiteralPath $summary)) {
    throw "ETL did not generate $summary"
}

Get-Content -LiteralPath $summary
Write-Output (
    "B2B_RESULT=GREEN mode=embedded-kafka-contract-harness " +
    "externalServices=0 externalOrders=0"
)
