[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][ValidateRange(1024, 65535)][int]$Port,
    [Parameter(Mandatory = $true)][string]$Wallet,
    [Parameter(Mandatory = $true)][long]$Tid,
    [Parameter(Mandatory = $true)][long]$SourceTimestamp,
    [string]$StartPosition = '0',
    [string]$PythonPath = 'python',
    [string]$OutputDirectory = 'target/hot-live-cert'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
    throw "Port $Port already has a listener"
}
$resolvedPython = (Get-Command $PythonPath -ErrorAction Stop).Source
$fixturePath = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot 'hot-live-hyperliquid-fixture.py')).Path
$resolvedOutputDirectory = if ([IO.Path]::IsPathRooted($OutputDirectory)) {
    $OutputDirectory
} else { Join-Path (Split-Path -Parent $PSScriptRoot) $OutputDirectory }
[IO.Directory]::CreateDirectory($resolvedOutputDirectory) | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$stdoutPath = Join-Path $resolvedOutputDirectory "fixture-$stamp.out.log"
$stderrPath = Join-Path $resolvedOutputDirectory "fixture-$stamp.err.log"
$process = Start-Process -FilePath $resolvedPython -ArgumentList @(
    $fixturePath, '--port', $Port.ToString(), '--wallet', $Wallet,
    '--tid', $Tid.ToString(), '--source-timestamp', $SourceTimestamp.ToString(),
    '--start-position', $StartPosition
) -WorkingDirectory (Split-Path -Parent $fixturePath) -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
$deadline = (Get-Date).AddSeconds(10)
$healthy = $false
do {
    Start-Sleep -Milliseconds 200
    if ($process.HasExited) { break }
    try {
        $healthy = (Invoke-RestMethod -Uri "http://127.0.0.1:${Port}/health" -TimeoutSec 1).status -eq 'UP'
    } catch { $healthy = $false }
} while (-not $healthy -and (Get-Date) -lt $deadline)
if (-not $healthy) {
    if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force }
    throw 'Hyperliquid fixture did not become healthy'
}
[ordered]@{
    processId = $process.Id; port = $Port; wallet = $Wallet.ToLowerInvariant()
    tid = $Tid; sourceTimestamp = $SourceTimestamp; healthy = $healthy
    stdoutPath = $stdoutPath; stderrPath = $stderrPath
} | ConvertTo-Json -Compress
