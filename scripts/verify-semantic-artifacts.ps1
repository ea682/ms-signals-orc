param(
    [string]$Java21Home = "C:\Users\erika\.jdks\temurin-21.0.11",
    [string]$SentinelJar,
    [string]$SignalsJar
)

$ErrorActionPreference = "Stop"
$signalsRepo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$sentinelRepo = (Resolve-Path (Join-Path $signalsRepo "..\ms-sentinel-hyperliquid")).Path

if (-not (Test-Path -LiteralPath (Join-Path $Java21Home "bin\jar.exe"))) {
    throw "Java 21 jar tool was not found at $Java21Home"
}

if ([string]::IsNullOrWhiteSpace($SentinelJar)) {
    $SentinelJar = (Get-ChildItem -LiteralPath (Join-Path $sentinelRepo "target") `
            -Filter "ms-sentinel-hyperliquid-*.jar" |
        Where-Object { $_.Name -notlike "*.original" } |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1).FullName
}
if ([string]::IsNullOrWhiteSpace($SignalsJar)) {
    $SignalsJar = (Get-ChildItem -LiteralPath (Join-Path $signalsRepo "target") `
            -Filter "ms-signals-orc-*.jar" |
        Where-Object { $_.Name -notlike "*.original" } |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1).FullName
}

function Assert-JarContract {
    param(
        [string]$Name,
        [string]$JarPath,
        [string[]]$RequiredEntries,
        [string[]]$LoadableClasses
    )

    if (-not (Test-Path -LiteralPath $JarPath)) {
        throw "$Name JAR does not exist: $JarPath"
    }
    $entries = & (Join-Path $Java21Home "bin\jar.exe") tf $JarPath
    if ($LASTEXITCODE -ne 0) {
        throw "Cannot inspect $Name JAR: $JarPath"
    }
    foreach ($entry in $RequiredEntries) {
        if ($entries -notcontains $entry) {
            throw "$Name JAR is missing required entry: $entry"
        }
    }
    $tempRoot = Join-Path ([IO.Path]::GetTempPath()) `
        ("semantic-artifact-" + [Guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tempRoot | Out-Null
    try {
        Push-Location $tempRoot
        try {
            & (Join-Path $Java21Home "bin\jar.exe") xf $JarPath `
                "BOOT-INF/classes"
            if ($LASTEXITCODE -ne 0) {
                throw "$Name JAR classes cannot be extracted"
            }
        } finally {
            Pop-Location
        }
        $classPath = Join-Path $tempRoot "BOOT-INF\classes"
        foreach ($className in $LoadableClasses) {
            & (Join-Path $Java21Home "bin\javap.exe") `
                -classpath $classPath $className | Out-Null
            if ($LASTEXITCODE -ne 0) {
                throw "$Name JAR cannot load class: $className"
            }
        }
    } finally {
        $resolvedTemp = (Resolve-Path -LiteralPath $tempRoot).Path
        $systemTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        if (-not $resolvedTemp.StartsWith($systemTemp, `
                [StringComparison]::OrdinalIgnoreCase)) {
            throw "Refusing to remove unexpected temp path: $resolvedTemp"
        }
        Remove-Item -LiteralPath $resolvedTemp -Recurse -Force
    }
    $hash = (Get-FileHash -LiteralPath $JarPath -Algorithm SHA256).Hash.ToLowerInvariant()
    [pscustomobject]@{
        Service = $Name
        Jar = $JarPath
        Sha256 = $hash
    }
}

$sentinel = Assert-JarContract `
    -Name "ms-sentinel-hyperliquid" `
    -JarPath $SentinelJar `
    -RequiredEntries @(
        "META-INF/build-info.properties",
        "BOOT-INF/classes/com/apunto/sentinel/SemanticCapabilityReporter.class",
        "BOOT-INF/classes/com/apunto/sentinel/fastpath/domain/HyperliquidSourceTradeIdentity.class",
        "BOOT-INF/classes/com/apunto/sentinel/fastpath/service/impl/HyperliquidReadinessStateMachine.class",
        "BOOT-INF/classes/com/apunto/sentinel/fastpath/service/impl/HyperliquidPublishReadinessHealthIndicator.class",
        "BOOT-INF/classes/com/apunto/sentinel/fastpath/service/impl/HyperliquidAuthoritativeUserFillFactory.class",
        "BOOT-INF/classes/com/apunto/sentinel/metric/HyperliquidObservabilityMetrics.class"
    ) `
    -LoadableClasses @(
        "com.apunto.sentinel.SemanticCapabilityReporter",
        "com.apunto.sentinel.fastpath.domain.HyperliquidSourceTradeIdentity",
        "com.apunto.sentinel.fastpath.service.impl.HyperliquidReadinessStateMachine"
    )

$signals = Assert-JarContract `
    -Name "ms-signals-orc" `
    -JarPath $SignalsJar `
    -RequiredEntries @(
        "META-INF/build-info.properties",
        "BOOT-INF/classes/com/apunto/engine/SemanticCapabilityReporter.class",
        "BOOT-INF/classes/com/apunto/engine/hyperliquid/identity/HyperliquidSourceTradeIdentity.class",
        "BOOT-INF/classes/com/apunto/engine/hyperliquid/service/impl/HyperliquidOriginBaselinePolicy.class",
        "BOOT-INF/classes/com/apunto/engine/hyperliquid/service/impl/HyperliquidFlipExecutionBasisPolicy.class",
        "BOOT-INF/classes/com/apunto/engine/hyperliquid/service/impl/HyperliquidDirectIngestIdempotencyGuard.class",
        "BOOT-INF/classes/com/apunto/engine/service/movement/AuthoritativeMovementIdentity.class"
    ) `
    -LoadableClasses @(
        "com.apunto.engine.SemanticCapabilityReporter",
        "com.apunto.engine.hyperliquid.identity.HyperliquidSourceTradeIdentity",
        "com.apunto.engine.hyperliquid.service.impl.HyperliquidOriginBaselinePolicy",
        "com.apunto.engine.hyperliquid.service.impl.HyperliquidFlipExecutionBasisPolicy"
    )

$sentinel
$signals
Write-Output "ARTIFACT_RESULT=GREEN userFillPublisher=OFF"
