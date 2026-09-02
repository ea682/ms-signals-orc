[CmdletBinding()]
param(
    [string]$SentinelArtifactPath = '..\ms-sentinel-hyperliquid\target\ms-sentinel-hyperliquid-1.0.39.jar',
    [string]$SentinelTestClassesPath = '..\ms-sentinel-hyperliquid\target\test-classes',
    [string]$SignalsArtifactPath = 'target\ms-signals-orc-1.4.38.jar',
    [string]$EtlArtifactPath = '..\ms-wallet-metric-etl\target\ms-wallet-metric-etl-1.0.23.jar',
    [string]$SignalsBaselineSchemaPath = 'target\audit-schema-baseline.sql',
    [string]$SignalsBaselineHistoryPath = 'target\audit-flyway-history.sql',
    [ValidatePattern('^\d+$')][string]$SignalsBaselineVersion = '202608010001',
    [string]$EtlBaselineHistoryPath = 'target\audit-etl-flyway-history.sql',
    [ValidatePattern('^\d+$')][string]$EtlBaselineVersion = '202608220001',
    [string]$JavaPath = 'C:\Users\erika\.jdks\graalvm-ce-21.0.2\bin\java.exe',
    [string]$WslDistribution = 'Ubuntu',
    [string]$OutputDirectory = 'target\packaged-economic-hot-b2b',
    [ValidateRange(10, 300)][int]$TimeoutSeconds = 120,
    [switch]$ValidateInputsOnly,
    [switch]$KeepLocalInfrastructure
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
function Resolve-RepoPath([string]$Path) {
    if ([IO.Path]::IsPathRooted($Path)) {
        return (Resolve-Path -LiteralPath $Path).Path
    }
    return (Resolve-Path -LiteralPath (Join-Path $repo $Path)).Path
}

$sentinelJar = Resolve-RepoPath $SentinelArtifactPath
$sentinelTestClasses = Resolve-RepoPath $SentinelTestClassesPath
$signalsJar = Resolve-RepoPath $SignalsArtifactPath
$etlJar = Resolve-RepoPath $EtlArtifactPath
$signalsBaselineSchema = Resolve-RepoPath $SignalsBaselineSchemaPath
$signalsBaselineHistory = Resolve-RepoPath $SignalsBaselineHistoryPath
$etlBaselineHistory = Resolve-RepoPath $EtlBaselineHistoryPath
$java = (Resolve-Path -LiteralPath $JavaPath).Path
$output = if ([IO.Path]::IsPathRooted($OutputDirectory)) {
    $OutputDirectory
} else { Join-Path $repo $OutputDirectory }
[IO.Directory]::CreateDirectory($output) | Out-Null

foreach ($artifact in @($sentinelJar, $signalsJar, $etlJar)) {
    if (-not (Test-Path -LiteralPath $artifact -PathType Leaf)) {
        throw "Required packaged artifact is missing: $artifact"
    }
}
if (-not (Test-Path -LiteralPath $sentinelTestClasses -PathType Container)) {
    throw "Sentinel artifact-smoke classes are missing: $sentinelTestClasses"
}
$javaVersion = (& $java -version 2>&1 | Select-Object -First 1).ToString()
if ($javaVersion -notmatch 'version "21(?:\.|\")') {
    throw "Java 21 is required: $javaVersion"
}
if ($ValidateInputsOnly) {
    [ordered]@{
        sentinelArtifact = $sentinelJar
        sentinelTestClasses = $sentinelTestClasses
        signalsArtifact = $signalsJar
        etlArtifact = $etlJar
        signalsBaselineSchema = $signalsBaselineSchema
        signalsBaselineHistory = $signalsBaselineHistory
        etlBaselineHistory = $etlBaselineHistory
        java = $java
    } | ConvertTo-Json -Compress
    return
}

$wsl = (Get-Command wsl.exe -ErrorAction Stop).Source
$python = (Get-Command python -ErrorAction Stop).Source

function Invoke-Docker([Parameter(Mandatory = $true)][string[]]$Arguments) {
    $result = & $wsl -d $WslDistribution -- docker @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed: $($result -join [Environment]::NewLine)"
    }
    return ($result -join [Environment]::NewLine).Trim()
}

function Convert-ToWslPath([Parameter(Mandatory = $true)][string]$Path) {
    if ($Path -notmatch '^([A-Za-z]):\\(.*)$') {
        throw "Expected an absolute Windows path, got: $Path"
    }
    $drive = $Matches[1].ToLowerInvariant()
    $tail = $Matches[2].Replace('\', '/')
    return "/mnt/$drive/$tail"
}

function Get-FreePort {
    $listener = [Net.Sockets.TcpListener]::new(
            [Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try { return ([Net.IPEndPoint]$listener.LocalEndpoint).Port }
    finally { $listener.Stop() }
}

function Wait-Until(
    [Parameter(Mandatory = $true)][scriptblock]$Condition,
    [Parameter(Mandatory = $true)][string]$Failure
) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        if (& $Condition) { return }
        Start-Sleep -Milliseconds 200
    } while ((Get-Date) -lt $deadline)
    throw $Failure
}

function Stop-LocalProcess($Descriptor) {
    if ($null -eq $Descriptor) { return }
    $process = Get-Process -Id $Descriptor.processId -ErrorAction SilentlyContinue
    if ($null -eq $process) { return }
    Stop-Process -Id $process.Id
    try { Wait-Process -Id $process.Id -Timeout 10 -ErrorAction Stop }
    catch { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
}

$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$postgresContainer = "apunto-economic-b2b-postgres-$stamp"
$kafkaContainer = "apunto-economic-b2b-kafka-$stamp"
$postgresPort = Get-FreePort
$kafkaPort = Get-FreePort
$fixturePort = Get-FreePort
$signalsPort = Get-FreePort
$signalsReplicaPort = Get-FreePort
$etlPort = Get-FreePort
$etlReplicaPort = Get-FreePort
$sentinelPort = Get-FreePort
$sentinelReplicaPort = Get-FreePort
$database = 'economic_b2b'
$databaseUser = 'economic_b2b'
$databasePassword = 'economic_b2b_local_only'
$topic = 'operation-movement-persisted-v1'
$etlConsumerGroup = "economic-b2b-$stamp"
$processes = [Collections.Generic.List[object]]::new()
$results = [ordered]@{}
$submittedIdentities = [Collections.Generic.HashSet[string]]::new()
$expectedFailClosedIdentities = [Collections.Generic.HashSet[string]]::new()

function Query-Sql([Parameter(Mandatory = $true)][string]$Sql) {
    return Invoke-Docker @('exec', $postgresContainer, 'psql',
        '-v', 'ON_ERROR_STOP=1', '-U', $databaseUser, '-d', $database,
        '-Atqc', $Sql)
}

function Count-Sql([Parameter(Mandatory = $true)][string]$Sql) {
    return [long](Query-Sql $Sql)
}

function Source-Identity([string]$Wallet, [long]$Tid) {
    return "hyperliquid:trade:$($Wallet.ToLowerInvariant()):$Tid"
}

function Await-Economic([string]$SourceIdentity) {
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$SourceIdentity'") -eq 1
    } "Fact was not created for $SourceIdentity"
    $proof = Query-Sql "SELECT concat_ws('|', (SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$SourceIdentity'), (SELECT count(*) FROM futuros_operaciones.metric_event_outbox WHERE payload->>'sourceEventId'='$SourceIdentity' AND published_at IS NOT NULL), (SELECT count(*) FROM futuros_operaciones.wallet_metric_canonical_event_v2 WHERE payload->>'sourceEventId'='$SourceIdentity'), (SELECT count(*) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$SourceIdentity'), (SELECT count(DISTINCT f.position_cycle_id) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$SourceIdentity'))"
    if ($proof -ne '1|1|1|1|1') {
        throw "Exactly-once economic proof failed for ${SourceIdentity}: $proof"
    }
}

function Await-EconomicFacts(
    [string]$SourceIdentity,
    [int]$ExpectedFacts,
    [int]$ExpectedCycles
) {
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$SourceIdentity'") -eq $ExpectedFacts
    } "Expected $ExpectedFacts facts for $SourceIdentity"
    $proof = Query-Sql "SELECT concat_ws('|', (SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$SourceIdentity'), (SELECT count(*) FROM futuros_operaciones.metric_event_outbox WHERE payload->>'sourceEventId'='$SourceIdentity' AND published_at IS NOT NULL), (SELECT count(*) FROM futuros_operaciones.wallet_metric_canonical_event_v2 WHERE payload->>'sourceEventId'='$SourceIdentity'), (SELECT count(*) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$SourceIdentity'), (SELECT count(DISTINCT f.position_cycle_id) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$SourceIdentity'))"
    $expected = "1|1|1|$ExpectedFacts|$ExpectedCycles"
    if ($proof -ne $expected) {
        throw "Economic fact proof failed for ${SourceIdentity}: expected=$expected actual=$proof"
    }
}

function Post-Live(
    [int]$Port,
    [string]$Wallet,
    [long]$Tid,
    [long]$SourceTs,
    [string]$Coin,
    [string]$Price,
    [string]$Quantity,
    [string]$Side
) {
    $uri = "http://127.0.0.1:$Port/__artifact_smoke/live" +
        "?wallet=$([uri]::EscapeDataString($Wallet))&tid=$Tid&sourceTs=$SourceTs" +
        "&coin=$([uri]::EscapeDataString($Coin))" +
        "&price=$([uri]::EscapeDataString($Price))" +
        "&quantity=$([uri]::EscapeDataString($Quantity))&side=$Side"
    $response = Invoke-RestMethod -Method Post -Uri $uri -TimeoutSec 10
    if (-not $response.accepted) { throw "Sentinel rejected $Tid" }
    $submittedIdentities.Add((Source-Identity $Wallet $Tid)) | Out-Null
    return $response
}

function Activate-Fill([long]$Tid) {
    $activation = Invoke-RestMethod -Method Post `
        -Uri "http://127.0.0.1:$fixturePort/activate?tid=$Tid" `
        -TimeoutSec 5
    if ([long]$activation.activated -ne $Tid) {
        throw "Fixture did not activate fill $Tid"
    }
}

function Invoke-Live(
    [int]$Port,
    [string]$Wallet,
    [long]$Tid,
    [long]$SourceTs,
    [string]$Coin,
    [string]$Price,
    [string]$Quantity,
    [string]$Side
) {
    Post-Live $Port $Wallet $Tid $SourceTs $Coin $Price $Quantity $Side | Out-Null
    Activate-Fill $Tid
}

function Start-Signals([int]$Port, [string]$InstanceId) {
    $json = & (Join-Path $PSScriptRoot 'start-hot-live-packaged-signals.ps1') `
        -ArtifactPath $signalsJar -ExpectedSha256 $signalsSha -JavaPath $java `
        -Port $Port -InstanceId $InstanceId -DatabasePort $postgresPort `
        -DatabaseName $database -DatabaseUser $databaseUser `
        -DatabasePassword $databasePassword -KafkaPort $kafkaPort `
        -OutputDirectory $output -StartupTimeoutSeconds $TimeoutSeconds
    $descriptor = $json | ConvertFrom-Json
    $processes.Add($descriptor) | Out-Null
    return $descriptor
}

function Start-Sentinel([int]$Port, [string]$InstanceId, [int]$SignalsPort) {
    $json = & (Join-Path $PSScriptRoot 'start-hot-live-packaged-sentinel.ps1') `
        -ArtifactPath $sentinelJar -TestClassesPath $sentinelTestClasses `
        -ExpectedSha256 $sentinelSha -JavaPath $java -Port $Port `
        -InstanceId $InstanceId -FixtureBaseUrl "http://127.0.0.1:$fixturePort" `
        -SignalsBaseUrl "http://127.0.0.1:$SignalsPort" `
        -DatabasePort $postgresPort -DatabaseName $database `
        -DatabaseUser $databaseUser -DatabasePassword $databasePassword `
        -OutputDirectory $output -StartupTimeoutSeconds $TimeoutSeconds
    $descriptor = $json | ConvertFrom-Json
    $processes.Add($descriptor) | Out-Null
    return $descriptor
}

function Start-Etl([int]$Port) {
    $json = & (Join-Path $PSScriptRoot 'start-hot-live-packaged-etl.ps1') `
        -ArtifactPath $etlJar -ExpectedSha256 $etlSha -JavaPath $java `
        -Port $Port -ConsumerGroup $etlConsumerGroup `
        -DatabasePort $postgresPort -DatabaseName $database `
        -DatabaseUser $databaseUser -DatabasePassword $databasePassword `
        -KafkaPort $kafkaPort -OutputDirectory $output `
        -StartupTimeoutSeconds $TimeoutSeconds
    $descriptor = $json | ConvertFrom-Json
    $processes.Add($descriptor) | Out-Null
    return $descriptor
}

$sentinelSha = (Get-FileHash $sentinelJar -Algorithm SHA256).Hash.ToUpperInvariant()
$signalsSha = (Get-FileHash $signalsJar -Algorithm SHA256).Hash.ToUpperInvariant()
$etlSha = (Get-FileHash $etlJar -Algorithm SHA256).Hash.ToUpperInvariant()

$walletLifecycle = '0x1000000000000000000000000000000000000001'
$walletBoundary = '0x29998ebd5be758fdaa06f0ef48d6c890978b65de'
$walletShort = '0xa9b95f0000000000000000000000000000000000'
$walletResolvable = '0x2000000000000000000000000000000000000002'
$walletAmbiguous = '0x3000000000000000000000000000000000000003'
$walletReplica = '0x4000000000000000000000000000000000000004'
$walletInfrastructureOutage = '0x6000000000000000000000000000000000000006'
$walletShortLifecycle = '0x7000000000000000000000000000000000000007'
$walletFlipLongShort = '0x8000000000000000000000000000000000000008'
$walletFlipShortLong = '0x9000000000000000000000000000000000000009'
$walletSameOrder = '0xa00000000000000000000000000000000000000a'
$walletOutOfOrder = '0xb00000000000000000000000000000000000000b'
$walletLate = '0xc00000000000000000000000000000000000000c'
$walletFalseConflict = '0xd00000000000000000000000000000000000000d'
$walletTrueConflict = '0xe00000000000000000000000000000000000000e'
$walletRateLimited = '0xf00000000000000000000000000000000000000f'
$walletSentinelCrash = '0x1100000000000000000000000000000000000011'
$walletSignalsReplica = '0x1200000000000000000000000000000000000012'
$walletEtlRestart = '0x1300000000000000000000000000000000000013'
$baseTs = [DateTimeOffset]::UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds()
$baseTid = 990000000000000L + [Math]::Abs([DateTime]::UtcNow.Ticks % 1000000L)
$fills = [Collections.Generic.List[object]]::new()

function Add-Fill([string]$Wallet, [string]$Coin, [string]$Price,
                  [string]$Size, [string]$Side, [long]$Time,
                  [string]$StartPosition, [string]$Direction,
                  [string]$ClosedPnl, [string]$Fee, [long]$Tid,
                  [long]$OrderId = 0) {
    $resolvedOrderId = if ($OrderId -gt 0) { $OrderId } else { $Tid }
    $fills.Add([ordered]@{
        wallet=$Wallet; coin=$Coin; px=$Price; sz=$Size; side=$Side
        time=$Time; startPosition=$StartPosition; dir=$Direction
        closedPnl=$ClosedPnl; fee=$Fee; hash="0xb2b$Tid"; tid=$Tid
        oid=$resolvedOrderId
    })
}

Add-Fill $walletLifecycle HYPE 20 10 B ($baseTs + 1000) 0 'Open Long' 0 0.10 ($baseTid + 1)
Add-Fill $walletLifecycle HYPE 21 5 B ($baseTs + 2000) 10 'Increase Long' 0 0.05 ($baseTid + 2)
Add-Fill $walletLifecycle HYPE 22 5 A ($baseTs + 3000) 15 'Close Long' 5 0.05 ($baseTid + 3)
Add-Fill $walletLifecycle HYPE 23 10 A ($baseTs + 4000) 10 'Close Long' 20 0.10 ($baseTid + 4)
Add-Fill $walletBoundary ZEC 745 50 A ($baseTs + 4500) 259.57 'Close Long' 2312.385 15.924375 970505954175203
Add-Fill $walletShort BTC 70018 0.0002 B ($baseTs + 5000) -171.572390 'Close Short' 0.01 0.00001 ($baseTid + 5)
Add-Fill $walletResolvable HYPE 25 1 B ($baseTs + 6000) 0 'Open Long' 0 0.01 ($baseTid + 6)
Add-Fill $walletResolvable APT 8 1 B ($baseTs + 7000) 0 'Open Long' 0 0.01 ($baseTid + 60)
Add-Fill $walletResolvable APT 8 1 B ($baseTs + 7000) 0 'Open Long' 0 0.01 ($baseTid + 61)
Add-Fill $walletAmbiguous HYPE 26 1 B ($baseTs + 8000) 0 'Open Long' 0 0.01 ($baseTid + 7)
Add-Fill $walletAmbiguous HYPE 26 1 B ($baseTs + 8000) 0 'Open Long' 0 0.01 ($baseTid + 70)
Add-Fill $walletReplica SOL 150 2 B ($baseTs + 9000) 0 'Open Long' 0 0.02 ($baseTid + 8)
Add-Fill $walletInfrastructureOutage HYPE 24 1 B ($baseTs + 9500) 0 'Open Long' 0 0.01 ($baseTid + 9)

Add-Fill $walletShortLifecycle BTC 70000 10 A ($baseTs + 20000) 0 'Open Short' 0 0.10 ($baseTid + 201)
Add-Fill $walletShortLifecycle BTC 69900 5 A ($baseTs + 20100) -10 'Increase Short' 0 0.05 ($baseTid + 202)
Add-Fill $walletShortLifecycle BTC 69800 5 B ($baseTs + 20200) -15 'Close Short' 5 0.05 ($baseTid + 203)
Add-Fill $walletShortLifecycle BTC 69700 10 B ($baseTs + 20300) -10 'Close Short' 20 0.10 ($baseTid + 204)

Add-Fill $walletFlipLongShort HYPE 30 5 B ($baseTs + 21000) 0 'Open Long' 0 0.05 ($baseTid + 211)
Add-Fill $walletFlipLongShort HYPE 29 8 A ($baseTs + 21100) 5 'Close Long' -5 0.08 ($baseTid + 212)
Add-Fill $walletFlipShortLong SOL 150 5 A ($baseTs + 22000) 0 'Open Short' 0 0.05 ($baseTid + 213)
Add-Fill $walletFlipShortLong SOL 151 8 B ($baseTs + 22100) -5 'Close Short' -5 0.08 ($baseTid + 214)

$sameOrderId = $baseTid + 220
Add-Fill $walletSameOrder HYPE 31 1 B ($baseTs + 23000) 0 'Open Long' 0 0.01 ($baseTid + 221) $sameOrderId
Add-Fill $walletSameOrder HYPE 32 2 B ($baseTs + 23100) 1 'Increase Long' 0 0.02 ($baseTid + 222) $sameOrderId
Add-Fill $walletOutOfOrder HYPE 33 1 B ($baseTs + 24000) 0 'Open Long' 0 0.01 ($baseTid + 231)
Add-Fill $walletOutOfOrder HYPE 34 1 B ($baseTs + 24100) 1 'Increase Long' 0 0.01 ($baseTid + 232)
Add-Fill $walletLate HYPE 35 1 B ($baseTs + 25000) 0 'Open Long' 0 0.01 ($baseTid + 241)
Add-Fill $walletFalseConflict HYPE 36 1 B ($baseTs + 26000) 0 'Open Long' 0 0.01 ($baseTid + 251)
Add-Fill $walletTrueConflict HYPE 37 1 B ($baseTs + 27000) 0 'Open Long' 0 0.01 ($baseTid + 261)
Add-Fill $walletRateLimited HYPE 38 1 B ($baseTs + 28000) 0 'Open Long' 0 0.01 ($baseTid + 271)
Add-Fill $walletSentinelCrash HYPE 39 1 B ($baseTs + 29000) 0 'Open Long' 0 0.01 ($baseTid + 281)
Add-Fill $walletSignalsReplica HYPE 40 1 B ($baseTs + 30000) 0 'Open Long' 0 0.01 ($baseTid + 291)
Add-Fill $walletEtlRestart HYPE 41 1 B ($baseTs + 31000) 0 'Open Long' 0 0.01 ($baseTid + 301)
Add-Fill $walletEtlRestart HYPE 42 1 B ($baseTs + 31100) 1 'Increase Long' 0 0.01 ($baseTid + 302)

$saturation = [Collections.Generic.List[object]]::new()
for ($index = 0; $index -lt 20; $index++) {
    $wallet = '0x5' + ($index.ToString('x').PadLeft(39, '0'))
    $tid = $baseTid + 100 + $index
    $time = $baseTs + 10000 + $index
    Add-Fill $wallet HYPE 24 1 B $time 0 'Open Long' 0 0.01 $tid
    $saturation.Add([ordered]@{wallet=$wallet; tid=$tid; time=$time})
}
$fillsPath = Join-Path $output "fills-$stamp.json"
$fills | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $fillsPath -Encoding utf8

try {
    $dockerVersion = Invoke-Docker @('version', '--format', '{{.Server.Version}}')
    Invoke-Docker @('image', 'inspect', 'postgres:16-alpine') | Out-Null
    Invoke-Docker @('image', 'inspect', 'apache/kafka:3.9.1') | Out-Null
    Invoke-Docker @('run', '-d', '--name', $postgresContainer,
        '-e', "POSTGRES_USER=$databaseUser", '-e', "POSTGRES_PASSWORD=$databasePassword",
        '-e', "POSTGRES_DB=$database", '-p', "127.0.0.1:${postgresPort}:5432",
        'postgres:16-alpine') | Out-Null
    Wait-Until {
        try {
            Invoke-Docker @('exec', $postgresContainer, 'pg_isready', '-U', $databaseUser, '-d', $database) | Out-Null
            return $true
        } catch { return $false }
    } 'PostgreSQL 16 did not become ready'
    Invoke-Docker @('cp', (Convert-ToWslPath $signalsBaselineSchema),
        "${postgresContainer}:/tmp/signals-schema.sql") | Out-Null
    Invoke-Docker @('cp', (Convert-ToWslPath $signalsBaselineHistory),
        "${postgresContainer}:/tmp/signals-history.sql") | Out-Null
    Invoke-Docker @('cp', (Convert-ToWslPath $etlBaselineHistory),
        "${postgresContainer}:/tmp/etl-history.sql") | Out-Null
    Invoke-Docker @('exec', $postgresContainer, 'psql',
        '-v', 'ON_ERROR_STOP=1', '-U', $databaseUser, '-d', $database,
        '-f', '/tmp/signals-schema.sql') | Out-Null
    Invoke-Docker @('exec', $postgresContainer, 'psql',
        '-v', 'ON_ERROR_STOP=1', '-U', $databaseUser, '-d', $database,
        '-f', '/tmp/signals-history.sql') | Out-Null
    Invoke-Docker @('exec', $postgresContainer, 'psql',
        '-v', 'ON_ERROR_STOP=1', '-U', $databaseUser, '-d', $database,
        '-f', '/tmp/etl-history.sql') | Out-Null
    $baselineProof = Query-Sql "SELECT concat_ws('|', to_regclass('futuros_operaciones.detail_user'), to_regclass('futuros_operaciones.flyway_schema_history'), coalesce((SELECT max(version) FROM futuros_operaciones.flyway_schema_history WHERE success),'NONE'))"
    $expectedBaselineProof = 'futuros_operaciones.detail_user|' +
        'futuros_operaciones.flyway_schema_history|' +
        $SignalsBaselineVersion
    if ($baselineProof -ne $expectedBaselineProof) {
        throw "Signals historical baseline restore failed: $baselineProof"
    }
    $etlBaselineProof = Query-Sql "SELECT concat_ws('|', to_regclass('futuros_operaciones.flyway_schema_history_wallet_metric_v2'), coalesce((SELECT max(version) FROM futuros_operaciones.flyway_schema_history_wallet_metric_v2 WHERE success),'NONE'))"
    $expectedEtlBaselineProof = 'futuros_operaciones.flyway_schema_history_wallet_metric_v2|' +
        $EtlBaselineVersion
    if ($etlBaselineProof -ne $expectedEtlBaselineProof) {
        throw "ETL historical baseline restore failed: $etlBaselineProof"
    }

    Invoke-Docker @('run', '-d', '--name', $kafkaContainer,
        '-p', "127.0.0.1:${kafkaPort}:9092",
        '-e', 'KAFKA_NODE_ID=1', '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
        '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093',
        '-e', "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$kafkaPort",
        '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
        '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093',
        '-e', 'KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT',
        '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
        '-e', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1',
        '-e', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1',
        '-e', 'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0',
        '-e', 'CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk',
        'apache/kafka:3.9.1') | Out-Null
    Wait-Until {
        try {
            $kafkaLogs = Invoke-Docker @('logs', '--tail', '80', $kafkaContainer)
            return $kafkaLogs -match 'Kafka Server started'
        } catch { return $false }
    } 'Kafka did not become ready'

    $signals = Start-Signals $signalsPort 'signals-a'

    $migrationLog = Join-Path $output "etl-migration-$stamp.log"
    $env:DB_URL = "jdbc:postgresql://127.0.0.1:$postgresPort/$database"
    $env:DB_USER = $databaseUser
    $env:DB_PASSWORD = $databasePassword
    $env:SPRING_FLYWAY_ENABLED = 'true'
    $env:METRIC_ETL_EXPECTED_ARTIFACT_SHA256 = $etlSha
    & $java -jar $etlJar '--spring.profiles.active=migration' *> $migrationLog
    if ($LASTEXITCODE -ne 0) { throw "ETL migration-only failed; see $migrationLog" }

    $accounts = @($walletLifecycle, $walletBoundary, $walletShort,
        $walletResolvable, $walletAmbiguous, $walletReplica,
        $walletInfrastructureOutage, $walletShortLifecycle,
        $walletFlipLongShort, $walletFlipShortLong, $walletSameOrder,
        $walletOutOfOrder, $walletLate, $walletFalseConflict,
        $walletTrueConflict, $walletRateLimited, $walletSentinelCrash,
        $walletSignalsReplica, $walletEtlRestart) +
        @($saturation | ForEach-Object { $_.wallet })
    $accountIndex = 0
    foreach ($wallet in $accounts) {
        $accountIndex++
        Query-Sql "INSERT INTO futuros_operaciones.cuenta(idcuenta,nombreplataforma,nombrecuenta,wallet,activa,fecha_creacion) VALUES ('economic-b2b-$accountIndex','HYPERLIQUID','economic-b2b-$accountIndex','$wallet',1,clock_timestamp()) ON CONFLICT (idcuenta) DO NOTHING" | Out-Null
    }

    $fixtureJson = & (Join-Path $PSScriptRoot 'start-hot-live-hyperliquid-fixture.ps1') `
        -Port $fixturePort -Wallet $walletLifecycle -Tid ($baseTid + 1) `
        -SourceTimestamp ($baseTs + 1000) -FillsPath $fillsPath `
        -PythonPath $python -OutputDirectory $output
    $fixture = $fixtureJson | ConvertFrom-Json
    $processes.Add($fixture) | Out-Null

    $etl = Start-Etl $etlPort
    $sentinel = Start-Sentinel $sentinelPort 'sentinel-a' $signalsPort

    Invoke-Live $sentinelPort $walletLifecycle ($baseTid + 1) ($baseTs + 1000) HYPE 20 10 B
    Await-Economic (Source-Identity $walletLifecycle ($baseTid + 1))
    $results.CLEAN_OPEN = 'GREEN'

    Stop-LocalProcess $signals
    Invoke-Live $sentinelPort $walletLifecycle ($baseTid + 2) ($baseTs + 2000) HYPE 21 5 B
    $pendingIdentity = Source-Identity $walletLifecycle ($baseTid + 2)
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$pendingIdentity'") -eq 1
    } 'Sentinel did not journal the fill during Signals outage'
    $outageStatus = Query-Sql "SELECT concat_ws('|',status,last_reason_code,authoritative_fill IS NOT NULL) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$pendingIdentity'"
    if ($outageStatus -notmatch '^(PENDING_SOURCE_EVIDENCE|READY_TO_PUBLISH|RESOLVING|RETRY)\|') {
        throw "Signals outage did not leave a recoverable durable fill: $outageStatus"
    }
    $signals = Start-Signals $signalsPort 'signals-a-restarted'
    Await-Economic $pendingIdentity
    Invoke-Live $sentinelPort $walletLifecycle ($baseTid + 3) ($baseTs + 3000) HYPE 22 5 A
    Invoke-Live $sentinelPort $walletLifecycle ($baseTid + 4) ($baseTs + 4000) HYPE 23 10 A
    Await-Economic (Source-Identity $walletLifecycle ($baseTid + 3))
    Await-Economic (Source-Identity $walletLifecycle ($baseTid + 4))
    $lifecycleProof = Query-Sql "SELECT concat_ws('|', count(*), count(DISTINCT f.position_cycle_id), count(*) FILTER (WHERE c.status='CLOSED')) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id JOIN futuros_operaciones.wallet_position_cycle_v2 c ON c.position_cycle_id=f.position_cycle_id WHERE e.payload->>'wallet'='$walletLifecycle'"
    if ($lifecycleProof -ne '4|1|4') { throw "Lifecycle proof failed: $lifecycleProof" }
    $results.FULL_LIFECYCLE = 'GREEN'
    $results.CRASH_RECOVERY = 'GREEN'

    Invoke-Live $sentinelPort $walletBoundary 970505954175203 ($baseTs + 4500) ZEC 745 50 A
    $boundaryIdentity = Source-Identity $walletBoundary 970505954175203
    Await-Economic $boundaryIdentity
    $boundaryProof = Query-Sql "SELECT concat_ws('|', economic_basis_status, metric_eligible, raw->>'economicBasisReason') FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$boundaryIdentity'"
    if ($boundaryProof -notmatch '^COMPLETE\|t\|AUTHORITATIVE_PREEXISTING_POSITION_BOUNDARY$') {
        throw "Production boundary remained non-economic: $boundaryProof"
    }
    $results.PREEXISTING_POSITION = 'GREEN'
    $results.RESIZE_PRODUCTION_FIXTURE = 'GREEN'

    Invoke-Live $sentinelPort $walletShort ($baseTid + 5) ($baseTs + 5000) BTC 70018 0.0002 B
    $shortIdentity = Source-Identity $walletShort ($baseTid + 5)
    Await-Economic $shortIdentity
    $qtyProof = Query-Sql "SELECT concat_ws('|', previous_size_qty >= 0, resulting_size_qty >= 0, source_previous_position_quantity = -171.572390, source_resulting_position_quantity = -171.572190) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$shortIdentity'"
    if ($qtyProof -ne 't|t|t|t') {
        throw "SHORT magnitude/signed quantity contract failed: $qtyProof"
    }
    $results.QTY_FAILURE_FIXTURE = 'GREEN'

    foreach ($event in @(
        @{tid=$baseTid + 201; ts=$baseTs + 20000; price='70000'; qty='10'; side='A'},
        @{tid=$baseTid + 202; ts=$baseTs + 20100; price='69900'; qty='5'; side='A'},
        @{tid=$baseTid + 203; ts=$baseTs + 20200; price='69800'; qty='5'; side='B'},
        @{tid=$baseTid + 204; ts=$baseTs + 20300; price='69700'; qty='10'; side='B'}
    )) {
        Invoke-Live $sentinelPort $walletShortLifecycle $event.tid $event.ts BTC $event.price $event.qty $event.side
        Await-Economic (Source-Identity $walletShortLifecycle $event.tid)
    }
    $shortLifecycleProof = Query-Sql "SELECT concat_ws('|', count(*), count(DISTINCT f.position_cycle_id), count(*) FILTER (WHERE c.status='CLOSED')) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id JOIN futuros_operaciones.wallet_position_cycle_v2 c ON c.position_cycle_id=f.position_cycle_id WHERE e.payload->>'wallet'='$walletShortLifecycle'"
    if ($shortLifecycleProof -ne '4|1|4') {
        throw "SHORT lifecycle proof failed: $shortLifecycleProof"
    }
    $results.SHORT_LIFECYCLE = 'GREEN'

    Invoke-Live $sentinelPort $walletFlipLongShort ($baseTid + 211) ($baseTs + 21000) HYPE 30 5 B
    Await-Economic (Source-Identity $walletFlipLongShort ($baseTid + 211))
    Invoke-Live $sentinelPort $walletFlipLongShort ($baseTid + 212) ($baseTs + 21100) HYPE 29 8 A
    Await-EconomicFacts (Source-Identity $walletFlipLongShort ($baseTid + 212)) 2 2
    $longShortFlipProof = Query-Sql "SELECT concat_ws('|', count(*), count(DISTINCT f.position_cycle_id), count(*) FILTER (WHERE c.status='CLOSED')) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id JOIN futuros_operaciones.wallet_position_cycle_v2 c ON c.position_cycle_id=f.position_cycle_id WHERE e.payload->>'wallet'='$walletFlipLongShort'"
    if ($longShortFlipProof -ne '3|2|2') {
        throw "LONG-to-SHORT flip proof failed: $longShortFlipProof"
    }
    $results.FLIP_LONG_TO_SHORT = 'GREEN'

    Invoke-Live $sentinelPort $walletFlipShortLong ($baseTid + 213) ($baseTs + 22000) SOL 150 5 A
    Await-Economic (Source-Identity $walletFlipShortLong ($baseTid + 213))
    Invoke-Live $sentinelPort $walletFlipShortLong ($baseTid + 214) ($baseTs + 22100) SOL 151 8 B
    Await-EconomicFacts (Source-Identity $walletFlipShortLong ($baseTid + 214)) 2 2
    $shortLongFlipProof = Query-Sql "SELECT concat_ws('|', count(*), count(DISTINCT f.position_cycle_id), count(*) FILTER (WHERE c.status='CLOSED')) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id JOIN futuros_operaciones.wallet_position_cycle_v2 c ON c.position_cycle_id=f.position_cycle_id WHERE e.payload->>'wallet'='$walletFlipShortLong'"
    if ($shortLongFlipProof -ne '3|2|2') {
        throw "SHORT-to-LONG flip proof failed: $shortLongFlipProof"
    }
    $results.FLIP_SHORT_TO_LONG = 'GREEN'

    foreach ($event in @(
        @{tid=$baseTid + 221; ts=$baseTs + 23000; price='31'; qty='1'},
        @{tid=$baseTid + 222; ts=$baseTs + 23100; price='32'; qty='2'}
    )) {
        Invoke-Live $sentinelPort $walletSameOrder $event.tid $event.ts HYPE $event.price $event.qty B
        Await-Economic (Source-Identity $walletSameOrder $event.tid)
    }
    $sameOrderProof = Query-Sql "SELECT concat_ws('|', count(DISTINCT e.payload->>'sourceEventId'), count(DISTINCT f.canonical_event_id), count(DISTINCT f.position_cycle_id)) FROM futuros_operaciones.wallet_metric_canonical_event_v2 e JOIN futuros_operaciones.wallet_execution_fact_v2 f ON f.canonical_event_id=e.canonical_event_id WHERE e.payload->>'wallet'='$walletSameOrder'"
    if ($sameOrderProof -ne '2|2|1') {
        throw "Multiple fills from the same source order failed: $sameOrderProof"
    }
    $results.MULTIPLE_FILLS_SAME_ORDER = 'GREEN'

    Post-Live $sentinelPort $walletOutOfOrder ($baseTid + 232) ($baseTs + 24100) HYPE 34 1 B | Out-Null
    Post-Live $sentinelPort $walletOutOfOrder ($baseTid + 231) ($baseTs + 24000) HYPE 33 1 B | Out-Null
    Activate-Fill ($baseTid + 232)
    Activate-Fill ($baseTid + 231)
    $outOfOrderFirst = Source-Identity $walletOutOfOrder ($baseTid + 231)
    $outOfOrderSecond = Source-Identity $walletOutOfOrder ($baseTid + 232)
    Await-Economic $outOfOrderFirst
    Await-Economic $outOfOrderSecond
    $outOfOrderProof = Query-Sql "SELECT string_agg(e.payload->>'sourceEventId', ',' ORDER BY f.executed_at, f.execution_fact_id) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'wallet'='$walletOutOfOrder'"
    if ($outOfOrderProof -ne "$outOfOrderFirst,$outOfOrderSecond") {
        throw "Out-of-order causal processing failed: $outOfOrderProof"
    }
    $results.OUT_OF_ORDER = 'GREEN'

    $lateTid = $baseTid + 241
    $lateIdentity = Source-Identity $walletLate $lateTid
    Post-Live $sentinelPort $walletLate $lateTid ($baseTs + 25000) HYPE 35 1 B | Out-Null
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$lateIdentity' AND attempt_count > 0") -eq 1
    } 'Late fill was not durably retained while evidence was absent'
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$lateIdentity'") -ne 0) {
        throw 'Late fill reached Signals before authoritative evidence arrived'
    }
    Activate-Fill $lateTid
    Await-Economic $lateIdentity
    $results.LATE_AUTHORITATIVE_EVENT = 'GREEN'

    $falseConflictTid = $baseTid + 251
    $falseConflictIdentity = Source-Identity $walletFalseConflict $falseConflictTid
    Post-Live $sentinelPort $walletFalseConflict $falseConflictTid ($baseTs + 26000) HYPE 36 1 B | Out-Null
    Post-Live $sentinelPort $walletFalseConflict $falseConflictTid ($baseTs + 26000) HYPE 36.00 1.000 B | Out-Null
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$falseConflictIdentity'") -ne 1) {
        throw 'Equivalent representation created more than one journal identity'
    }
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$falseConflictIdentity' AND status='BLOCKED_MANUAL_REVIEW'") -ne 0) {
        throw 'Equivalent numeric representation incorrectly failed closed'
    }
    Activate-Fill $falseConflictTid
    Await-Economic $falseConflictIdentity
    $results.FALSE_REPRESENTATION_CONFLICT = 'GREEN'

    $rateLimitedTid = $baseTid + 271
    $rateLimitedIdentity = Source-Identity $walletRateLimited $rateLimitedTid
    $rateLimit = Invoke-RestMethod -Method Post `
        -Uri "http://127.0.0.1:$fixturePort/rate-limit?count=2" `
        -TimeoutSec 5
    if ([int]$rateLimit.rateLimitRemaining -ne 2) {
        throw 'Fixture did not arm the controlled 429 responses'
    }
    Invoke-Live $sentinelPort $walletRateLimited $rateLimitedTid ($baseTs + 28000) HYPE 38 1 B
    Await-Economic $rateLimitedIdentity
    $rateLimitHealth = Invoke-RestMethod -Method Get `
        -Uri "http://127.0.0.1:$fixturePort/health" -TimeoutSec 5
    $rateLimitProof = Query-Sql "SELECT concat_ws('|', status, (SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$rateLimitedIdentity'), (SELECT count(*) FROM futuros_operaciones.wallet_metric_canonical_event_v2 WHERE payload->>'sourceEventId'='$rateLimitedIdentity'), (SELECT count(*) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$rateLimitedIdentity')) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$rateLimitedIdentity'"
    if ([int]$rateLimitHealth.rateLimitRemaining -ne 0 `
            -or $rateLimitProof -ne 'PUBLISHED|1|1|1') {
        throw "429 recovery proof failed: remaining=$($rateLimitHealth.rateLimitRemaining) durable=$rateLimitProof"
    }
    $results.HYPERLIQUID_429 = 'GREEN'
    $results.HYPERLIQUID_429_RESPONSES_CONSUMED = 2

    $sentinelCrashTid = $baseTid + 281
    $sentinelCrashIdentity = Source-Identity $walletSentinelCrash $sentinelCrashTid
    Post-Live $sentinelPort $walletSentinelCrash $sentinelCrashTid `
        ($baseTs + 29000) HYPE 39 1 B | Out-Null
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$sentinelCrashIdentity'") -ne 1) {
        throw 'Sentinel crash fixture was not durable before process termination'
    }
    Stop-LocalProcess $sentinel
    Activate-Fill $sentinelCrashTid
    $sentinel = Start-Sentinel $sentinelPort 'sentinel-a-after-crash' $signalsPort
    Await-Economic $sentinelCrashIdentity
    $sentinelRestartProof = Query-Sql "SELECT concat_ws('|', count(*), max(status), (SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$sentinelCrashIdentity')) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$sentinelCrashIdentity'"
    if ($sentinelRestartProof -ne '1|PUBLISHED|1') {
        throw "Sentinel restart/replay proof failed: $sentinelRestartProof"
    }
    $results.SENTINEL_CRASH_RESTART = 'GREEN'

    foreach ($decoyTid in @(($baseTid + 60), ($baseTid + 61))) {
        Invoke-RestMethod -Method Post `
            -Uri "http://127.0.0.1:$fixturePort/activate?tid=$decoyTid" `
            -TimeoutSec 5 | Out-Null
    }
    Invoke-Live $sentinelPort $walletResolvable ($baseTid + 6) ($baseTs + 6000) HYPE 25 1 B
    Await-Economic (Source-Identity $walletResolvable ($baseTid + 6))
    Invoke-RestMethod -Method Post `
        -Uri "http://127.0.0.1:$fixturePort/activate?tid=$($baseTid + 70)" `
        -TimeoutSec 5 | Out-Null
    Invoke-Live $sentinelPort $walletAmbiguous ($baseTid + 7) ($baseTs + 8000) HYPE 26 1 B
    $ambiguousIdentity = Source-Identity $walletAmbiguous ($baseTid + 7)
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$ambiguousIdentity' AND status='BLOCKED_MANUAL_REVIEW' AND last_reason_code='AUTHORITATIVE_CHAIN_AMBIGUOUS'") -eq 1
    } 'Truly ambiguous target did not fail closed'
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$ambiguousIdentity'") -ne 0) {
        throw 'Truly ambiguous target reached Signals'
    }
    $expectedFailClosedIdentities.Add($ambiguousIdentity) | Out-Null
    $results.AMBIGUOUS_CHAIN_FIXTURES = 'GREEN'

    $sentinelReplica = Start-Sentinel $sentinelReplicaPort 'sentinel-b' $signalsPort
    $replicaTid = $baseTid + 8
    $client = [Net.Http.HttpClient]::new()
    try {
        $query = "wallet=$walletReplica&tid=$replicaTid&sourceTs=$($baseTs + 9000)&coin=SOL&price=150&quantity=2&side=B"
        $taskA = $client.PostAsync("http://127.0.0.1:$sentinelPort/__artifact_smoke/live?$query", [Net.Http.StringContent]::new(''))
        $taskB = $client.PostAsync("http://127.0.0.1:$sentinelReplicaPort/__artifact_smoke/live?$query", [Net.Http.StringContent]::new(''))
        [Threading.Tasks.Task]::WaitAll(
            @($taskA, $taskB),
            [TimeSpan]::FromSeconds(10)) | Out-Null
        if (-not $taskA.Result.IsSuccessStatusCode -or -not $taskB.Result.IsSuccessStatusCode) {
            throw 'Concurrent Sentinel requests failed'
        }
        Invoke-RestMethod -Method Post `
            -Uri "http://127.0.0.1:$fixturePort/activate?tid=$replicaTid" `
            -TimeoutSec 5 | Out-Null
    } finally { $client.Dispose() }
    $replicaIdentity = Source-Identity $walletReplica $replicaTid
    $submittedIdentities.Add($replicaIdentity) | Out-Null
    Await-Economic $replicaIdentity
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$replicaIdentity'") -ne 1) {
        throw 'Two Sentinel replicas created more than one durable identity'
    }
    $results.TWO_REPLICA = 'GREEN'

    $rawReplay = Query-Sql "SELECT (raw->'request')::text FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$replicaIdentity'"
    1..2 | ForEach-Object {
        try {
            Invoke-WebRequest -UseBasicParsing -Method Post -ContentType 'application/json' `
                -Body $rawReplay -Uri "http://127.0.0.1:$signalsPort/internal/v1/hyperliquid/deltas" `
                -TimeoutSec 10 | Out-Null
        } catch {
            if ($_.Exception.Response.StatusCode.value__ -notin @(200, 202, 409)) { throw }
        }
    }
    Start-Sleep -Seconds 2
    Await-Economic $replicaIdentity
    $results.REPLAY = 'GREEN'

    $signalsReplica = Start-Signals $signalsReplicaPort 'signals-b'
    $signalsReplicaTid = $baseTid + 291
    $signalsReplicaIdentity = Source-Identity `
        $walletSignalsReplica $signalsReplicaTid
    Invoke-Live $sentinelPort $walletSignalsReplica $signalsReplicaTid `
        ($baseTs + 30000) HYPE 40 1 B
    Await-Economic $signalsReplicaIdentity
    $signalsReplicaRaw = Query-Sql "SELECT (raw->'request')::text FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$signalsReplicaIdentity'"
    $signalsClient = [Net.Http.HttpClient]::new()
    try {
        $signalsTaskA = $signalsClient.PostAsync(
            "http://127.0.0.1:$signalsPort/internal/v1/hyperliquid/deltas",
            [Net.Http.StringContent]::new(
                $signalsReplicaRaw,
                [Text.Encoding]::UTF8,
                'application/json'))
        $signalsTaskB = $signalsClient.PostAsync(
            "http://127.0.0.1:$signalsReplicaPort/internal/v1/hyperliquid/deltas",
            [Net.Http.StringContent]::new(
                $signalsReplicaRaw,
                [Text.Encoding]::UTF8,
                'application/json'))
        [Threading.Tasks.Task]::WaitAll(
            @($signalsTaskA, $signalsTaskB),
            [TimeSpan]::FromSeconds(10)) | Out-Null
        $signalsStatuses = @(
            [int]$signalsTaskA.Result.StatusCode,
            [int]$signalsTaskB.Result.StatusCode)
        $invalidSignalsStatuses = @(
            $signalsStatuses | Where-Object { $_ -notin @(200, 202, 409) })
        if ($invalidSignalsStatuses.Count -gt 0) {
            throw "Signals replica replay failed: $($signalsStatuses -join ',')"
        }
    } finally { $signalsClient.Dispose() }
    Await-Economic $signalsReplicaIdentity
    $signalsReplicaProof = Query-Sql "SELECT concat_ws('|', (SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$signalsReplicaIdentity'), (SELECT count(*) FROM futuros_operaciones.metric_event_outbox WHERE payload->>'sourceEventId'='$signalsReplicaIdentity'), (SELECT count(*) FROM futuros_operaciones.wallet_metric_canonical_event_v2 WHERE payload->>'sourceEventId'='$signalsReplicaIdentity'))"
    if ($signalsReplicaProof -ne '1|1|1') {
        throw "Signals multi-replica exactly-once proof failed: $signalsReplicaProof"
    }
    $results.MULTI_REPLICA_SIGNALS = 'GREEN'

    $etlRestartTid = $baseTid + 301
    $etlRestartIdentity = Source-Identity $walletEtlRestart $etlRestartTid
    Stop-LocalProcess $etl
    Invoke-Live $sentinelPort $walletEtlRestart $etlRestartTid `
        ($baseTs + 31000) HYPE 41 1 B
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.metric_event_outbox WHERE payload->>'sourceEventId'='$etlRestartIdentity' AND published_at IS NOT NULL") -eq 1
    } 'Signals did not publish the ETL restart fixture to Kafka'
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.wallet_metric_canonical_event_v2 WHERE payload->>'sourceEventId'='$etlRestartIdentity'") -ne 0) {
        throw 'ETL-stopped fixture was canonicalized before ETL restart'
    }
    $etl = Start-Etl $etlPort
    Await-Economic $etlRestartIdentity
    $results.ETL_CRASH_RESTART = 'GREEN'

    $etlReplica = Start-Etl $etlReplicaPort
    $etlReplicaTid = $baseTid + 302
    $etlReplicaIdentity = Source-Identity $walletEtlRestart $etlReplicaTid
    Invoke-Live $sentinelPort $walletEtlRestart $etlReplicaTid `
        ($baseTs + 31100) HYPE 42 1 B
    Await-Economic $etlReplicaIdentity
    if ($null -eq (Get-Process -Id $etl.processId -ErrorAction SilentlyContinue) `
            -or $null -eq (Get-Process -Id $etlReplica.processId -ErrorAction SilentlyContinue)) {
        throw 'Both ETL replicas were not alive during the exactly-once proof'
    }
    $results.MULTI_REPLICA_ETL = 'GREEN'

    foreach ($event in $saturation) {
        Invoke-Live $sentinelPort $event.wallet $event.tid $event.time HYPE 24 1 B
    }
    foreach ($event in $saturation) {
        Await-Economic (Source-Identity $event.wallet $event.tid)
    }
    $saturationIds = ($saturation | ForEach-Object { "'$(Source-Identity $_.wallet $_.tid)'" }) -join ','
    $saturationProof = Query-Sql "SELECT concat_ws('|', count(DISTINCT e.payload->>'sourceEventId'), count(DISTINCT f.canonical_event_id), count(DISTINCT f.position_cycle_id)) FROM futuros_operaciones.wallet_metric_canonical_event_v2 e JOIN futuros_operaciones.wallet_execution_fact_v2 f ON f.canonical_event_id=e.canonical_event_id WHERE e.payload->>'sourceEventId' IN ($saturationIds)"
    if ($saturationProof -ne '20|20|20') { throw "Saturation economic proof failed: $saturationProof" }
    $results.SATURATION_ECONOMIC = 'GREEN'

    # Keep Kafka unavailable while the authoritative fill is durably accepted.
    # Then make PostgreSQL unavailable while Kafka resumes. The economic effect
    # must appear exactly once after both dependencies recover.
    $outageTid = $baseTid + 9
    $infrastructureOutageIdentity = Source-Identity $walletInfrastructureOutage $outageTid
    $kafkaPaused = $false
    $postgresPaused = $false
    try {
        Invoke-Docker @('pause', $kafkaContainer) | Out-Null
        $kafkaPaused = $true
        Invoke-Live $sentinelPort $walletInfrastructureOutage $outageTid ($baseTs + 9500) HYPE 24 1 B
        Wait-Until {
            (Count-Sql "SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$infrastructureOutageIdentity'") -eq 1
        } 'Signals did not durably persist the fill during Kafka outage'
        if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$infrastructureOutageIdentity'") -ne 0) {
            throw 'Kafka-paused fill reached Fact before Kafka recovery'
        }

        Invoke-Docker @('pause', $postgresContainer) | Out-Null
        $postgresPaused = $true
        Invoke-Docker @('unpause', $kafkaContainer) | Out-Null
        $kafkaPaused = $false
        Start-Sleep -Seconds 2
    } finally {
        if ($postgresPaused) {
            Invoke-Docker @('unpause', $postgresContainer) | Out-Null
            $postgresPaused = $false
        }
        if ($kafkaPaused) {
            Invoke-Docker @('unpause', $kafkaContainer) | Out-Null
            $kafkaPaused = $false
        }
    }
    Await-Economic $infrastructureOutageIdentity
    $results.KAFKA_OUTAGE = 'GREEN'
    $results.POSTGRES_OUTAGE = 'GREEN'

    $auditFixture = Join-Path $repo 'src\test\resources\fixtures\production\anomaly-f-position-delta-pnl.json'
    $auditRequest = (Get-Content -Raw -LiteralPath $auditFixture | ConvertFrom-Json).request
    $auditIdentity = $auditRequest.sourceEventId
    $auditJson = $auditRequest | ConvertTo-Json -Depth 12 -Compress
    Invoke-WebRequest -UseBasicParsing -Method Post -ContentType 'application/json' `
        -Body $auditJson -Uri "http://127.0.0.1:$signalsPort/internal/v1/hyperliquid/deltas" `
        -TimeoutSec 10 | Out-Null
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.wallet_metric_canonical_event_v2 WHERE payload->>'sourceEventId'='$auditIdentity' AND status='PROCESSED'") -eq 1
    } 'POSITION_DELTA did not reach canonical audit'
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.wallet_execution_fact_v2 f JOIN futuros_operaciones.wallet_metric_canonical_event_v2 e ON e.canonical_event_id=f.canonical_event_id WHERE e.payload->>'sourceEventId'='$auditIdentity'") -ne 0) {
        throw 'POSITION_DELTA incorrectly created a Fact'
    }
    $results.POSITION_DELTA_AUDIT_ONLY = 'GREEN'

    # A real economic contradiction intentionally blocks global publication
    # readiness. Run this terminal proof after every scenario that needs a
    # ready Sentinel, then verify that the fail-closed state survives restart.
    $trueConflictTid = $baseTid + 261
    $trueConflictIdentity = Source-Identity $walletTrueConflict $trueConflictTid
    Post-Live $sentinelPort $walletTrueConflict $trueConflictTid `
        ($baseTs + 27000) HYPE 37 1 B | Out-Null
    $conflictUri = "http://127.0.0.1:$sentinelPort/__artifact_smoke/live" +
        "?wallet=$walletTrueConflict&tid=$trueConflictTid" +
        "&sourceTs=$($baseTs + 27000)&coin=HYPE&price=37.5" +
        '&quantity=1&side=B'
    $conflictClient = [Net.Http.HttpClient]::new()
    try {
        $conflictResponse = $conflictClient.PostAsync(
            $conflictUri,
            [Net.Http.StringContent]::new('')).GetAwaiter().GetResult()
        if ($conflictResponse.IsSuccessStatusCode) {
            throw 'Contradictory raw evidence was incorrectly accepted'
        }
    } finally { $conflictClient.Dispose() }
    Stop-LocalProcess $sentinel
    Activate-Fill $trueConflictTid

    $unexpectedReadyRestart = $null
    $restartFailure = $null
    try {
        $unexpectedReadyRestart = Start-Sentinel $sentinelPort `
            'sentinel-a-after-conflict' $signalsPort
    } catch {
        $restartFailure = $_.Exception.Message
    }
    if ($null -ne $unexpectedReadyRestart) {
        Stop-LocalProcess $unexpectedReadyRestart
        throw 'Contradictory journal incorrectly allowed a ready restart'
    }
    if ($restartFailure -notmatch 'journal_economic_contradiction') {
        throw "Contradictory journal restart failed for an unexpected reason: $restartFailure"
    }
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity='$trueConflictIdentity' AND status='BLOCKED_MANUAL_REVIEW' AND raw_conflict_count > 0 AND last_reason_code='RAW_SOURCE_EVIDENCE_CONFLICT'") -eq 1
    } 'Contradictory raw evidence was not durably failed closed'
    if ((Count-Sql "SELECT count(*) FROM futuros_operaciones.operation_movement_event WHERE source_event_id='$trueConflictIdentity'") -ne 0) {
        throw 'Contradictory raw evidence reached Signals'
    }
    $expectedFailClosedIdentities.Add($trueConflictIdentity) | Out-Null
    $results.TRUE_IDEMPOTENCY_CONFLICT = 'GREEN'

    $results.DOCKER_ENVIRONMENT = "WSL:$WslDistribution"
    $results.DOCKER_VERSION = $dockerVersion
    $results.POSTGRES_VERSION = Query-Sql 'SHOW server_version'
    $results.KAFKA_IMAGE = 'apache/kafka:3.9.1'
    $results.SIGNALS_BASELINE_SCHEMA_SHA = (Get-FileHash $signalsBaselineSchema -Algorithm SHA256).Hash.ToUpperInvariant()
    $results.SIGNALS_BASELINE_HISTORY_SHA = (Get-FileHash $signalsBaselineHistory -Algorithm SHA256).Hash.ToUpperInvariant()
    $results.SENTINEL_SHA = $sentinelSha
    $results.SIGNALS_SHA = $signalsSha
    $results.ETL_SHA = $etlSha

    if ($submittedIdentities.Count -eq 0) {
        throw 'No authoritative identities were submitted to the B2B chain'
    }
    $submittedSql = (@($submittedIdentities) | Sort-Object | ForEach-Object {
        "'$($_.Replace("'", "''"))'"
    }) -join ','
    $expectedFailClosedSql = (@($expectedFailClosedIdentities) |
        Sort-Object | ForEach-Object { "'$($_.Replace("'", "''"))'" }) -join ','
    Wait-Until {
        (Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity IN ($submittedSql) AND status IN ('PUBLISHED','BLOCKED_MANUAL_REVIEW')") -eq $submittedIdentities.Count
    } 'Not every accepted identity reached a terminal or valid fail-closed state'

    $journalRows = Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity IN ($submittedSql)"
    $journalIdentities = Count-Sql "SELECT count(DISTINCT source_identity) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity IN ($submittedSql)"
    $publishedIdentities = Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity IN ($submittedSql) AND status='PUBLISHED'"
    $pendingRecoverable = Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity IN ($submittedSql) AND status NOT IN ('PUBLISHED','BLOCKED_MANUAL_REVIEW')"
    $rejectedIdentities = Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity IN ($submittedSql) AND status='BLOCKED_MANUAL_REVIEW'"
    $expectedRejectedIdentities = Count-Sql "SELECT count(*) FROM futuros_operaciones.hyperliquid_source_trade_journal WHERE source_identity IN ($expectedFailClosedSql) AND status='BLOCKED_MANUAL_REVIEW'"
    $processedIdentities = Count-Sql "SELECT count(DISTINCT source_event_id) FROM futuros_operaciones.operation_movement_event WHERE source_event_id IN ($submittedSql)"
    $canonicalIdentities = Count-Sql "SELECT count(DISTINCT payload->>'sourceEventId') FROM futuros_operaciones.wallet_metric_canonical_event_v2 WHERE payload->>'sourceEventId' IN ($submittedSql)"
    $duplicateOperationEffects = Count-Sql "SELECT COALESCE(sum(duplicate_count),0) FROM (SELECT GREATEST(count(*)-1,0) AS duplicate_count FROM futuros_operaciones.operation_movement_event WHERE source_event_id IN ($submittedSql) GROUP BY source_event_id) duplicates"
    $duplicateCanonicalEffects = Count-Sql "SELECT COALESCE(sum(duplicate_count),0) FROM (SELECT GREATEST(count(*)-1,0) AS duplicate_count FROM futuros_operaciones.wallet_metric_canonical_event_v2 WHERE payload->>'sourceEventId' IN ($submittedSql) GROUP BY payload->>'sourceEventId') duplicates"
    $duplicateOutboxEffects = Count-Sql "SELECT COALESCE(sum(duplicate_count),0) FROM (SELECT GREATEST(count(*)-1,0) AS duplicate_count FROM futuros_operaciones.metric_event_outbox WHERE payload->>'sourceEventId' IN ($submittedSql) GROUP BY payload->>'sourceEventId') duplicates"
    $duplicateEconomicEffects = $duplicateOperationEffects +
        $duplicateCanonicalEffects + $duplicateOutboxEffects
    $unprotectedAccepted = $submittedIdentities.Count - $journalIdentities
    $silentLoss = $submittedIdentities.Count - $processedIdentities -
        $pendingRecoverable - $rejectedIdentities
    $falseFailClosed = $rejectedIdentities - $expectedRejectedIdentities

    if ($journalRows -ne $submittedIdentities.Count `
            -or $journalIdentities -ne $submittedIdentities.Count `
            -or $publishedIdentities -ne $processedIdentities `
            -or $canonicalIdentities -ne $processedIdentities `
            -or $expectedRejectedIdentities -ne $expectedFailClosedIdentities.Count `
            -or $pendingRecoverable -ne 0 `
            -or $silentLoss -ne 0 `
            -or $unprotectedAccepted -ne 0 `
            -or $falseFailClosed -ne 0 `
            -or $duplicateEconomicEffects -ne 0) {
        throw "B2B conservation failed: input=$($submittedIdentities.Count) journalRows=$journalRows journalIdentities=$journalIdentities published=$publishedIdentities processed=$processedIdentities canonical=$canonicalIdentities pending=$pendingRecoverable rejected=$rejectedIdentities expectedRejected=$expectedRejectedIdentities silentLoss=$silentLoss falseFailClosed=$falseFailClosed duplicateEffects=$duplicateEconomicEffects"
    }

    $results.INPUT_AUTHORITATIVE_IDENTITIES = $submittedIdentities.Count
    $results.VALID_AUTHORITATIVE_FILLS = $submittedIdentities.Count
    $results.ECONOMICALLY_PROCESSED = $processedIdentities
    $results.PUBLISHED_IDENTITIES = $publishedIdentities
    $results.PENDING_RECOVERABLE_IDENTITIES = $pendingRecoverable
    $results.VALID_PRE_ACCEPTANCE_REJECTIONS = $rejectedIdentities
    $results.TRUE_FAIL_CLOSED = $expectedRejectedIdentities
    $results.FALSE_FAIL_CLOSED = $falseFailClosed
    $results.PERMANENT_DATA_LOSS = $silentLoss
    $results.UNPROTECTED_ACCEPTED_EVENTS = $unprotectedAccepted
    $results.DUPLICATE_JOURNAL_IDENTITIES = $journalRows - $journalIdentities
    $results.UNRESOLVED_SENTINEL_IDENTITY_GAPS = $silentLoss
    $results.DUPLICATE_ECONOMIC_EFFECT = $duplicateEconomicEffects
    $results.CONSERVATION_PROOF = "$($submittedIdentities.Count)=$processedIdentities+$pendingRecoverable+$rejectedIdentities"
    $results.PACKAGED_ECONOMIC_B2B = 'GREEN'
    $report = Join-Path $output "result-$stamp.json"
    $results | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $report -Encoding utf8
    Get-Content -Raw -LiteralPath $report
} finally {
    $orderedProcesses = @($processes)
    [array]::Reverse($orderedProcesses)
    foreach ($descriptor in $orderedProcesses) {
        Stop-LocalProcess $descriptor
    }
    if (-not $KeepLocalInfrastructure) {
        foreach ($container in @($kafkaContainer, $postgresContainer)) {
            try { Invoke-Docker @('rm', '-f', $container) | Out-Null } catch { }
        }
    }
}
