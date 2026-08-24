[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$ArtifactPath,
    [Parameter(Mandatory = $true)][ValidateRange(1024, 65535)][int]$Port,
    [Parameter(Mandatory = $true)][string]$ConsumerGroup,
    [string]$ExpectedSha256,
    [string]$JavaPath = 'java',
    [string]$DatabaseHost = '127.0.0.1',
    [ValidateRange(1024, 65535)][int]$DatabasePort = 35432,
    [string]$DatabaseName = 'etl_b2b_cert',
    [string]$DatabaseUser = 'signals_cert',
    [string]$DatabasePassword = 'signals_cert_local',
    [string]$KafkaHost = '127.0.0.1',
    [ValidateRange(1024, 65535)][int]$KafkaPort = 39092,
    [string]$OutputDirectory = 'target/hot-live-cert',
    [ValidateRange(10, 300)][int]$StartupTimeoutSeconds = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-LoopbackHost([string]$HostName) {
    if ($HostName -notin @('127.0.0.1', 'localhost', '::1')) {
        throw "Packaged HOT certification only permits loopback infrastructure; rejected host: $HostName"
    }
}

Assert-LoopbackHost $DatabaseHost
Assert-LoopbackHost $KafkaHost
$resolvedArtifact = (Resolve-Path -LiteralPath $ArtifactPath).Path
$artifactHash = (Get-FileHash -LiteralPath $resolvedArtifact -Algorithm SHA256).Hash.ToUpperInvariant()
if ($ExpectedSha256 -and $artifactHash -ne $ExpectedSha256.ToUpperInvariant()) {
    throw "ETL artifact SHA256 mismatch. Expected=$ExpectedSha256 Actual=$artifactHash"
}
$resolvedJava = (Get-Command $JavaPath -ErrorAction Stop).Source
$javaVersion = (& $resolvedJava -version 2>&1 | Select-Object -First 1).ToString()
if ($javaVersion -notmatch 'version "21(?:\.|\")') { throw "Java 21 is required: $javaVersion" }
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
    throw "Port $Port already has a listener"
}

$resolvedOutputDirectory = if ([IO.Path]::IsPathRooted($OutputDirectory)) {
    $OutputDirectory
} else { Join-Path (Split-Path -Parent $PSScriptRoot) $OutputDirectory }
[IO.Directory]::CreateDirectory($resolvedOutputDirectory) | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$stdoutPath = Join-Path $resolvedOutputDirectory "etl-$ConsumerGroup-$stamp.out.log"
$stderrPath = Join-Path $resolvedOutputDirectory "etl-$ConsumerGroup-$stamp.err.log"

$env:DB_URL = "jdbc:postgresql://${DatabaseHost}:${DatabasePort}/${DatabaseName}"
$env:DB_USER = $DatabaseUser
$env:DB_PASSWORD = $DatabasePassword
$env:KAFKA_BROKERS = "${KafkaHost}:${KafkaPort}"
$env:KAFKA_CONSUMER_GROUP = $ConsumerGroup
$env:KAFKA_LISTENER_CONCURRENCY = '1'
$env:METRIC_ETL_KAFKA_TOPIC = 'operation-movement-persisted-v1'
$env:METRIC_ETL_V2_WORKER_INITIAL_DELAY_MS = '100'
$env:METRIC_ETL_V2_WORKER_DELAY_MS = '100'
$env:METRIC_ETL_V2_CLAIM_BATCH_SIZE = '50'
$env:METRIC_ETL_V2_REBUILD_ENABLED = 'false'
$env:METRIC_ETL_DB_POLL_ENABLED = 'false'
$env:METRIC_ETL_BACKFILL_ENABLED = 'false'
$env:METRIC_ETL_BACKFILL_MAX_BATCHES_PER_RUN = '1'
$env:METRIC_ETL_BACKFILL_FROM = '2026-08-11T23:21:39.836049035Z'
$env:METRIC_ETL_BACKFILL_TO = '2026-08-12T14:22:52.004168594Z'
$env:METRIC_ETL_STALE_REBUILD_THRESHOLD_HOURS = '24'
$env:METRIC_ETL_SCORING_ENABLED = 'false'
$env:METRIC_ETL_ALLOCATION_ENABLED = 'false'
$env:METRIC_ETL_V2_HISTORICAL_LAKE_ENABLED = 'false'
$env:METRIC_ETL_V2_HISTORICAL_LAKE_WORKER_ENABLED = 'false'
$env:METRIC_ETL_V2_HISTORICAL_LAKE_MONTHLY_ENABLED = 'false'
$env:METRIC_ETL_V2_HISTORICAL_LAKE_AUTO_SYNC_ENABLED = 'false'
$env:METRIC_ETL_V2_HISTORICAL_PROCESS_ENABLED = 'false'
$env:METRIC_ETL_V2_HISTORICAL_PROCESS_WORKER_ENABLED = 'false'
$env:METRIC_ETL_V2_HISTORICAL_RETENTION_ENABLED = 'false'
$env:SPRING_FLYWAY_ENABLED = 'false'
$env:METRIC_ETL_V2_CONTINUITY_AUTOMATIC_ACTIVATION_PAUSED = 'true'
$env:PORT = $Port.ToString()

$process = Start-Process -FilePath $resolvedJava `
    -ArgumentList @('-jar', $resolvedArtifact, '--spring.profiles.active=prod-hot,prod') `
    -WorkingDirectory (Split-Path -Parent $resolvedArtifact) -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru

$deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
$started = $false
do {
    Start-Sleep -Milliseconds 500
    if ($process.HasExited) { break }
    if (Test-Path -LiteralPath $stdoutPath) {
        $started = [bool](Select-String -LiteralPath $stdoutPath -SimpleMatch `
            'Started MsWalletMetricEtlApplication' -Quiet)
    }
} while (-not $started -and (Get-Date) -lt $deadline)
if (-not $started) {
    if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
    $tail = if (Test-Path $stdoutPath) { (Get-Content $stdoutPath -Tail 100) -join "`n" } else { '' }
    throw "ETL did not start. log=$tail"
}

[ordered]@{
    artifactPath = $resolvedArtifact; artifactSha256 = $artifactHash
    javaVersion = $javaVersion; processId = $process.Id; port = $Port
    database = "${DatabaseHost}:${DatabasePort}/${DatabaseName}"
    kafka = "${KafkaHost}:${KafkaPort}"; consumerGroup = $ConsumerGroup
    stdoutPath = $stdoutPath; stderrPath = $stderrPath
} | ConvertTo-Json -Compress
