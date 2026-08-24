[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$ArtifactPath,
    [Parameter(Mandatory = $true)][string]$TestClassesPath,
    [Parameter(Mandatory = $true)][ValidateRange(1024, 65535)][int]$Port,
    [Parameter(Mandatory = $true)][string]$FixtureBaseUrl,
    [Parameter(Mandatory = $true)][string]$SignalsBaseUrl,
    [string]$ExpectedSha256,
    [string]$JavaPath = 'java',
    [string]$DatabaseHost = '127.0.0.1',
    [ValidateRange(1024, 65535)][int]$DatabasePort = 35432,
    [string]$DatabaseName = 'sentinel_b2b_cert',
    [string]$DatabaseUser = 'signals_cert',
    [string]$DatabasePassword = 'signals_cert_local',
    [string]$OutputDirectory = 'target/hot-live-cert',
    [ValidateRange(10, 300)][int]$StartupTimeoutSeconds = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
foreach ($value in @($DatabaseHost, ([uri]$FixtureBaseUrl).Host, ([uri]$SignalsBaseUrl).Host)) {
    if ($value -notin @('127.0.0.1', 'localhost', '::1')) {
        throw "Packaged HOT certification only permits loopback infrastructure; rejected host: $value"
    }
}
$resolvedArtifact = (Resolve-Path -LiteralPath $ArtifactPath).Path
$resolvedTestClasses = (Resolve-Path -LiteralPath $TestClassesPath).Path
$artifactHash = (Get-FileHash -LiteralPath $resolvedArtifact -Algorithm SHA256).Hash.ToUpperInvariant()
if ($ExpectedSha256 -and $artifactHash -ne $ExpectedSha256.ToUpperInvariant()) {
    throw "Sentinel artifact SHA256 mismatch. Expected=$ExpectedSha256 Actual=$artifactHash"
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
$stdoutPath = Join-Path $resolvedOutputDirectory "sentinel-$stamp.out.log"
$stderrPath = Join-Path $resolvedOutputDirectory "sentinel-$stamp.err.log"
$arguments = @(
    "-Dloader.path=$resolvedTestClasses", '-cp', $resolvedArtifact,
    'org.springframework.boot.loader.launch.PropertiesLauncher',
    '--spring.profiles.active=prod,artifact-smoke', "--server.port=$Port",
    "--spring.datasource.url=jdbc:postgresql://${DatabaseHost}:${DatabasePort}/${DatabaseName}",
    "--spring.datasource.username=$DatabaseUser", "--spring.datasource.password=$DatabasePassword",
    '--spring.flyway.enabled=true', '--spring.flyway.baseline-on-migrate=true',
    '--hyperliquid.instance-id=packaged-hot-b2b', "--hyperliquid.info-base=$FixtureBaseUrl",
    "--hyperliquid.direct.url=$SignalsBaseUrl/internal/v1/hyperliquid/deltas",
    '--hyperliquid.direct.timeout-ms=5000', '--hyperliquid.direct.connect-timeout-ms=1000',
    '--hyperliquid.direct.fallback-kafka-on-failure=false', '--hyperliquid.kafka.enabled=false',
    '--hyperliquid.snapshot.initial-delay-ms=100', '--hyperliquid.snapshot.refresh-delay-ms=300000',
    '--hyperliquid.snapshot.startup-retry-poll-ms=50',
    '--hyperliquid.snapshot.startup-retry-initial-backoff-ms=100',
    '--hyperliquid.snapshot.startup-retry-max-backoff-ms=1000',
    '--hyperliquid.snapshot.wallet-request-timeout-ms=5000',
    '--hyperliquid.snapshot.cluster.enabled=true', '--hyperliquid.snapshot.cluster.scope-key=global',
    '--hyperliquid.snapshot.cluster.lease-ms=2000', '--hyperliquid.snapshot.cluster.heartbeat-ms=250',
    '--hyperliquid.snapshot.cluster.poll-ms=50', '--hyperliquid.readiness.evaluation-interval-ms=50',
    '--hyperliquid.authoritative-fills.worker-delay-ms=50',
    '--hyperliquid.authoritative-fills.worker-threads=1',
    '--hyperliquid.authoritative-fills.claim-batch-size=2',
    '--hyperliquid.authoritative-fills.retry-delay-ms=100',
    '--hyperliquid.wallets.refresh-interval=PT1H', '--hyperliquid.markets.refresh-interval=PT1H',
    '--hyperliquid.websocket.connection-start-stagger-ms=0',
    '--hyperliquid.websocket.catalog-reconcile-interval=PT1H'
)
$process = Start-Process -FilePath $resolvedJava -ArgumentList $arguments `
    -WorkingDirectory (Split-Path -Parent $resolvedArtifact) -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru

$deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
$ready = $false
do {
    Start-Sleep -Milliseconds 500
    if ($process.HasExited) { break }
    try {
        $health = Invoke-RestMethod -Uri "http://127.0.0.1:${Port}/__artifact_smoke/health" -TimeoutSec 2
        $ready = $health.readinessStatus -eq 'UP'
    } catch { $ready = $false }
} while (-not $ready -and (Get-Date) -lt $deadline)
if (-not $ready) {
    if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
    $tail = if (Test-Path $stdoutPath) { (Get-Content $stdoutPath -Tail 100) -join "`n" } else { '' }
    throw "Sentinel did not become ready. log=$tail"
}

[ordered]@{
    artifactPath = $resolvedArtifact; artifactSha256 = $artifactHash
    javaVersion = $javaVersion; processId = $process.Id; port = $Port; ready = $ready
    database = "${DatabaseHost}:${DatabasePort}/${DatabaseName}"
    fixture = $FixtureBaseUrl; signals = $SignalsBaseUrl
    stdoutPath = $stdoutPath; stderrPath = $stderrPath
} | ConvertTo-Json -Compress
