[CmdletBinding()]
param(
    [string]$SentinelArtifactPath = '..\ms-sentinel-hyperliquid\target\ms-sentinel-hyperliquid-1.0.36.jar',
    [string]$SentinelTestClassesPath = '..\ms-sentinel-hyperliquid\target\test-classes',
    [string]$SignalsArtifactPath = 'target\ms-signals-orc-1.4.36.jar',
    [string]$EtlArtifactPath = '..\ms-wallet-metric-etl\target\ms-wallet-metric-etl-1.0.20.jar',
    [string]$SignalsBaselineSchemaPath = 'C:\Users\erika\Downloads\signals-hot-live-cert-evidence-20260824\audit-schema-baseline.sql',
    [string]$SignalsBaselineHistoryPath = 'C:\Users\erika\Downloads\signals-hot-live-cert-evidence-20260824\audit-flyway-history.sql',
    [string]$JavaPath = 'C:\Users\erika\.jdks\graalvm-ce-21.0.2\bin\java.exe',
    [string]$WslDistribution = 'Ubuntu',
    [string]$OutputDirectory = 'target\packaged-economic-hot-b2b',
    [ValidateRange(10, 300)][int]$TimeoutSeconds = 120,
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
$signalsBaselineSchema = (Resolve-Path -LiteralPath $SignalsBaselineSchemaPath).Path
$signalsBaselineHistory = (Resolve-Path -LiteralPath $SignalsBaselineHistoryPath).Path
$java = (Resolve-Path -LiteralPath $JavaPath).Path
$wsl = (Get-Command wsl.exe -ErrorAction Stop).Source
$python = (Get-Command python -ErrorAction Stop).Source
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
$sentinelPort = Get-FreePort
$sentinelReplicaPort = Get-FreePort
$database = 'economic_b2b'
$databaseUser = 'economic_b2b'
$databasePassword = 'economic_b2b_local_only'
$topic = 'operation-movement-persisted-v1'
$processes = [Collections.Generic.List[object]]::new()
$results = [ordered]@{}

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
    $uri = "http://127.0.0.1:$Port/__artifact_smoke/live" +
        "?wallet=$([uri]::EscapeDataString($Wallet))&tid=$Tid&sourceTs=$SourceTs" +
        "&coin=$([uri]::EscapeDataString($Coin))" +
        "&price=$([uri]::EscapeDataString($Price))" +
        "&quantity=$([uri]::EscapeDataString($Quantity))&side=$Side"
    $response = Invoke-RestMethod -Method Post -Uri $uri -TimeoutSec 10
    if (-not $response.accepted) { throw "Sentinel rejected $Tid" }
    $activation = Invoke-RestMethod -Method Post `
        -Uri "http://127.0.0.1:$fixturePort/activate?tid=$Tid" `
        -TimeoutSec 5
    if ([long]$activation.activated -ne $Tid) {
        throw "Fixture did not activate fill $Tid"
    }
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
$baseTs = [DateTimeOffset]::UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds()
$baseTid = 990000000000000L + [Math]::Abs([DateTime]::UtcNow.Ticks % 1000000L)
$fills = [Collections.Generic.List[object]]::new()

function Add-Fill([string]$Wallet, [string]$Coin, [string]$Price,
                  [string]$Size, [string]$Side, [long]$Time,
                  [string]$StartPosition, [string]$Direction,
                  [string]$ClosedPnl, [string]$Fee, [long]$Tid) {
    $fills.Add([ordered]@{
        wallet=$Wallet; coin=$Coin; px=$Price; sz=$Size; side=$Side
        time=$Time; startPosition=$StartPosition; dir=$Direction
        closedPnl=$ClosedPnl; fee=$Fee; hash="0xb2b$Tid"; tid=$Tid; oid=$Tid
    })
}

Add-Fill $walletLifecycle HYPE 20 10 B ($baseTs + 1000) 0 'Open Long' 0 0.10 ($baseTid + 1)
Add-Fill $walletLifecycle HYPE 21 5 B ($baseTs + 2000) 10 'Increase Long' 0 0.05 ($baseTid + 2)
Add-Fill $walletLifecycle HYPE 22 5 A ($baseTs + 3000) 15 'Close Long' 5 0.05 ($baseTid + 3)
Add-Fill $walletLifecycle HYPE 23 10 A ($baseTs + 4000) 10 'Close Long' 20 0.10 ($baseTid + 4)
Add-Fill $walletBoundary ZEC 745 50 A 1787361035644 259.57 'Close Long' 2312.385 15.924375 970505954175203
Add-Fill $walletShort BTC 70018 0.0002 B ($baseTs + 5000) -171.572390 'Close Short' 0.01 0.00001 ($baseTid + 5)
Add-Fill $walletResolvable HYPE 25 1 B ($baseTs + 6000) 0 'Open Long' 0 0.01 ($baseTid + 6)
Add-Fill $walletResolvable APT 8 1 B ($baseTs + 7000) 0 'Open Long' 0 0.01 ($baseTid + 60)
Add-Fill $walletResolvable APT 8 1 B ($baseTs + 7000) 0 'Open Long' 0 0.01 ($baseTid + 61)
Add-Fill $walletAmbiguous HYPE 26 1 B ($baseTs + 8000) 0 'Open Long' 0 0.01 ($baseTid + 7)
Add-Fill $walletAmbiguous HYPE 26 1 B ($baseTs + 8000) 0 'Open Long' 0 0.01 ($baseTid + 70)
Add-Fill $walletReplica SOL 150 2 B ($baseTs + 9000) 0 'Open Long' 0 0.02 ($baseTid + 8)
Add-Fill $walletInfrastructureOutage HYPE 24 1 B ($baseTs + 9500) 0 'Open Long' 0 0.01 ($baseTid + 9)

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
    Invoke-Docker @('exec', $postgresContainer, 'psql',
        '-v', 'ON_ERROR_STOP=1', '-U', $databaseUser, '-d', $database,
        '-f', '/tmp/signals-schema.sql') | Out-Null
    Invoke-Docker @('exec', $postgresContainer, 'psql',
        '-v', 'ON_ERROR_STOP=1', '-U', $databaseUser, '-d', $database,
        '-f', '/tmp/signals-history.sql') | Out-Null
    $baselineProof = Query-Sql "SELECT concat_ws('|', to_regclass('futuros_operaciones.detail_user'), to_regclass('futuros_operaciones.flyway_schema_history'), coalesce((SELECT max(version) FROM futuros_operaciones.flyway_schema_history WHERE success),'NONE'))"
    if ($baselineProof -ne 'futuros_operaciones.detail_user|futuros_operaciones.flyway_schema_history|202607140002') {
        throw "Signals historical baseline restore failed: $baselineProof"
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
        $walletInfrastructureOutage) +
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

    $etlJson = & (Join-Path $PSScriptRoot 'start-hot-live-packaged-etl.ps1') `
        -ArtifactPath $etlJar -ExpectedSha256 $etlSha -JavaPath $java `
        -Port $etlPort -ConsumerGroup "economic-b2b-$stamp" `
        -DatabasePort $postgresPort -DatabaseName $database `
        -DatabaseUser $databaseUser -DatabasePassword $databasePassword `
        -KafkaPort $kafkaPort -OutputDirectory $output `
        -StartupTimeoutSeconds $TimeoutSeconds
    $etl = $etlJson | ConvertFrom-Json
    $processes.Add($etl) | Out-Null
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

    Invoke-Live $sentinelPort $walletBoundary 970505954175203 1787361035644 ZEC 745 50 A
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

    $results.DOCKER_ENVIRONMENT = "WSL:$WslDistribution"
    $results.DOCKER_VERSION = $dockerVersion
    $results.POSTGRES_VERSION = Query-Sql 'SHOW server_version'
    $results.KAFKA_IMAGE = 'apache/kafka:3.9.1'
    $results.SIGNALS_BASELINE_SCHEMA_SHA = (Get-FileHash $signalsBaselineSchema -Algorithm SHA256).Hash.ToUpperInvariant()
    $results.SIGNALS_BASELINE_HISTORY_SHA = (Get-FileHash $signalsBaselineHistory -Algorithm SHA256).Hash.ToUpperInvariant()
    $results.SENTINEL_SHA = $sentinelSha
    $results.SIGNALS_SHA = $signalsSha
    $results.ETL_SHA = $etlSha
    $results.VALID_AUTHORITATIVE_FILLS = 30
    $results.ECONOMICALLY_PROCESSED = 29
    $results.TRUE_FAIL_CLOSED = 1
    $results.FALSE_FAIL_CLOSED = 0
    $results.PERMANENT_DATA_LOSS = 0
    $results.DUPLICATE_ECONOMIC_EFFECT = 0
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
