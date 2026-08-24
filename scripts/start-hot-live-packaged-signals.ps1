[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ArtifactPath,

    [Parameter(Mandatory = $true)]
    [ValidateRange(1024, 65535)]
    [int]$Port,

    [Parameter(Mandatory = $true)]
    [string]$InstanceId,

    [string]$ExpectedSha256,
    [string]$JavaPath = 'java',
    [string]$DatabaseHost = '127.0.0.1',
    [ValidateRange(1024, 65535)]
    [int]$DatabasePort = 35432,
    [string]$DatabaseName = 'signals_cert',
    [string]$DatabaseUser = 'signals_cert',
    [string]$DatabasePassword = 'signals_cert_local',
    [string]$KafkaHost = '127.0.0.1',
    [ValidateRange(1024, 65535)]
    [int]$KafkaPort = 39092,
    [string]$OutputDirectory = 'target/hot-live-cert',
    [ValidateRange(10, 300)]
    [int]$StartupTimeoutSeconds = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-LoopbackHost {
    param([Parameter(Mandatory = $true)][string]$HostName)

    if ($HostName -notin @('127.0.0.1', 'localhost', '::1')) {
        throw "Packaged HOT certification only permits loopback infrastructure; rejected host: $HostName"
    }
}

Assert-LoopbackHost -HostName $DatabaseHost
Assert-LoopbackHost -HostName $KafkaHost

$resolvedArtifact = (Resolve-Path -LiteralPath $ArtifactPath).Path
$artifactHash = (Get-FileHash -LiteralPath $resolvedArtifact -Algorithm SHA256).Hash.ToUpperInvariant()
if ($ExpectedSha256 -and $artifactHash -ne $ExpectedSha256.ToUpperInvariant()) {
    throw "Signals artifact SHA256 mismatch. Expected=$ExpectedSha256 Actual=$artifactHash"
}

$resolvedJava = (Get-Command $JavaPath -ErrorAction Stop).Source
$javaVersion = (& $resolvedJava -version 2>&1 | Select-Object -First 1).ToString()
if ($javaVersion -notmatch 'version "21(?:\.|\")') {
    throw "Java 21 is required; resolved version was: $javaVersion"
}

if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
    throw "Port $Port already has a listener"
}

$resolvedOutputDirectory = if ([IO.Path]::IsPathRooted($OutputDirectory)) {
    $OutputDirectory
} else {
    Join-Path (Split-Path -Parent $PSScriptRoot) $OutputDirectory
}
[IO.Directory]::CreateDirectory($resolvedOutputDirectory) | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$stdoutPath = Join-Path $resolvedOutputDirectory "signals-$InstanceId-$stamp.out.log"
$stderrPath = Join-Path $resolvedOutputDirectory "signals-$InstanceId-$stamp.err.log"

$env:SPRING_PROFILES_ACTIVE = 'prod'
$env:APP_ENV = 'cert-local'
$env:PORT = $Port.ToString()
$env:DB_URL = "jdbc:postgresql://${DatabaseHost}:${DatabasePort}/${DatabaseName}"
$env:DB_USER = $DatabaseUser
$env:DB_PASSWORD = $DatabasePassword
$env:DB_SCHEMA = 'futuros_operaciones'
$env:SPRING_FLYWAY_ENABLED = 'true'
$env:KAFKA_BROKERS = "${KafkaHost}:${KafkaPort}"
$env:KAFKA_LISTENER_AUTO_STARTUP = 'false'
$env:HYPERLIQUID_KAFKA_INGEST_ENABLED = 'false'
$env:KAFKA_TOPIC_OPERATION_MOVEMENT_PERSISTED = 'operation-movement-persisted-v1'
$env:METRIC_OUTBOX_ENABLED = 'true'
$env:METRIC_OUTBOX_PUBLISHER_ENABLED = 'true'
$env:METRIC_OUTBOX_PUBLISHER_POLL_MS = '100'
$env:METRIC_OUTBOX_PUBLISH_TIMEOUT_MS = '2000'
$env:METRIC_OUTBOX_LOCK_TIMEOUT_MS = '2000'
$env:METRIC_OUTBOX_INSTANCE_ID = $InstanceId
$env:HYPERLIQUID_DIRECT_INGEST_ENABLED = 'true'
$env:HYPERLIQUID_DIRECT_INGEST_DISTRIBUTED_DEDUPE_ENABLED = 'true'
$env:HYPERLIQUID_DIRECT_INGEST_FAIL_OPEN_ON_DEDUPE_ERROR = 'false'
$env:HYPERLIQUID_DIRECT_INGEST_DEDUPE_LEASE_TTL_MS = '2000'
$env:HYPERLIQUID_ORIGIN_STORE_BINANCE_PRICE_ENABLED = 'false'
$env:HYPERLIQUID_ORIGIN_STORE_BINANCE_PRICE_ENGINE_ENABLED = 'false'
$env:BINANCE_SYMBOLS_WARMUP_ON_START = 'false'
$env:BINANCE_SYMBOLS_CACHE_TTL_MS = '3600000'
$env:BINANCE_TRADING_CONFIG_PRECONFIGURE_ENABLED = 'false'
$env:FUTURES_CAPITAL_MAINTENANCE_ENABLED = 'false'
$env:COPY_CAPITAL_ALLOCATOR_ENABLED = 'false'
$env:COPY_RECONCILIATION_ENABLED = 'false'
$env:COPY_NEW_DISPATCH_ENABLED = 'false'
$env:COPY_LIVE_ENABLED = 'false'
$env:COPY_LIVE_CANARY_ENABLED = 'false'
$env:COPY_MICRO_LIVE_ENABLED = 'false'
$env:BINANCE_ORDER_SUBMIT_ENABLED = 'false'
$env:COPY_B2B_REAL_MONEY_ENABLED = 'false'
$env:URL_BINANCE = 'http://127.0.0.1:9'
$env:URL_METRIC = 'http://127.0.0.1:9/'

$process = Start-Process `
    -FilePath $resolvedJava `
    -ArgumentList @('-jar', $resolvedArtifact) `
    -WorkingDirectory (Split-Path -Parent $resolvedArtifact) `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -PassThru

$deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
$healthy = $false
do {
    Start-Sleep -Milliseconds 500
    if ($process.HasExited) {
        break
    }
    try {
        $health = Invoke-WebRequest `
            -UseBasicParsing `
            -Uri "http://127.0.0.1:${Port}/actuator/health/liveness" `
            -TimeoutSec 2
        $healthy = $health.StatusCode -eq 200
    } catch {
        $healthy = $false
    }
} while (-not $healthy -and (Get-Date) -lt $deadline)

if (-not $healthy) {
    if (-not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
    }
    $stdoutTail = if (Test-Path -LiteralPath $stdoutPath) {
        (Get-Content -LiteralPath $stdoutPath -Tail 80) -join [Environment]::NewLine
    } else { '' }
    $stderrTail = if (Test-Path -LiteralPath $stderrPath) {
        (Get-Content -LiteralPath $stderrPath -Tail 80) -join [Environment]::NewLine
    } else { '' }
    throw "Signals did not become healthy. stdout=$stdoutTail stderr=$stderrTail"
}

[ordered]@{
    artifactPath = $resolvedArtifact
    artifactSha256 = $artifactHash
    javaVersion = $javaVersion
    instanceId = $InstanceId
    processId = $process.Id
    port = $Port
    healthy = $healthy
    database = "${DatabaseHost}:${DatabasePort}/${DatabaseName}"
    kafka = "${KafkaHost}:${KafkaPort}"
    stdoutPath = $stdoutPath
    stderrPath = $stderrPath
} | ConvertTo-Json -Compress
