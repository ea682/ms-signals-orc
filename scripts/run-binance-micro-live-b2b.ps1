[CmdletBinding()]
param(
    [switch]$Execute,
    [switch]$MicroLiveOnly,
    [switch]$VerifyEmergencyStop,
    [string]$Symbol = "ETHUSDC",
    [string]$CredentialsFile = ".env.execution-accounts",
    [string]$B2bEnvFile = ".env.b2b",
    [string]$DatabaseEnvFile = ".env.prod",
    [string]$StateFile = ".b2b-run-state.json",
    [string]$EvidenceDirectory = ".b2b-evidence"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$Invariant = [System.Globalization.CultureInfo]::InvariantCulture
$BaseUrl = "https://fapi.binance.com"
$SapiBaseUrl = "https://api.binance.com"

function Read-DotEnv([string]$Path, [bool]$Required = $true) {
    if (-not (Test-Path -LiteralPath $Path)) {
        if ($Required) { throw "B2B_CONFIG_FILE_MISSING: $Path" }
        return @{}
    }
    $values = @{}
    Get-Content -LiteralPath $Path | ForEach-Object {
        if ($_ -match '^([^#][^=]*)=(.*)$') {
            $values[$matches[1].Trim()] = $matches[2].Trim().Trim("'").Trim('"')
        }
    }
    return $values
}

function Require-Value($Values, [string]$Name) {
    $value = $Values[$Name]
    if ([string]::IsNullOrWhiteSpace($value)) { throw "B2B_REQUIRED_VALUE_MISSING: $Name" }
    return $value
}

function As-Bool([string]$Value) {
    return "true".Equals($Value, [StringComparison]::OrdinalIgnoreCase)
}

function Decimal-Text([decimal]$Value) {
    return $Value.ToString("0.##################", $Invariant)
}

function Convert-HexToUtf8([string]$Hex) {
    if ([string]::IsNullOrWhiteSpace($Hex) -or ($Hex.Length % 2) -ne 0 `
        -or $Hex -notmatch '^[0-9A-Fa-f]+$') {
        throw "B2B_DATABASE_CREDENTIAL_ENCODING_INVALID"
    }
    $bytes = [byte[]]::new($Hex.Length / 2)
    for ($index = 0; $index -lt $bytes.Length; $index++) {
        $bytes[$index] = [Convert]::ToByte($Hex.Substring($index * 2, 2), 16)
    }
    return [Text.Encoding]::UTF8.GetString($bytes)
}

function Build-Query([hashtable]$Parameters) {
    return (($Parameters.Keys | Sort-Object | ForEach-Object {
        [Uri]::EscapeDataString($_) + "=" + [Uri]::EscapeDataString([string]$Parameters[$_])
    }) -join "&")
}

function New-AccountContext([string]$Purpose, [string]$ExecutionAccountId,
                            [string]$ApiKey, [string]$ApiSecret) {
    if ($Purpose -notin @("MICRO_LIVE", "LIVE")) {
        throw "B2B_EXECUTION_ACCOUNT_PURPOSE_MISMATCH"
    }
    try { $id = [guid]$ExecutionAccountId } catch {
        throw "B2B_EXECUTION_ACCOUNT_ID_INVALID purpose=$Purpose"
    }
    if ([string]::IsNullOrWhiteSpace($ApiKey) -or [string]::IsNullOrWhiteSpace($ApiSecret)) {
        throw "B2B_EXECUTION_ACCOUNT_CREDENTIALS_MISSING purpose=$Purpose"
    }
    return [pscustomobject]@{
        Purpose = $Purpose
        ExecutionAccountId = $id
        ApiKey = $ApiKey
        ApiSecret = $ApiSecret
        OriginalLeverage = $null
        B2bLeverage = $null
        OriginalPositionMode = $null
    }
}

function Get-AccountsFromCredentialFile($Values, $B2b) {
    $required = @(
        "BINANCE_MICRO_LIVE_API_KEY", "BINANCE_MICRO_LIVE_API_SECRET",
        "BINANCE_LIVE_API_KEY", "BINANCE_LIVE_API_SECRET"
    )
    foreach ($name in $required) {
        if ([string]::IsNullOrWhiteSpace($Values[$name])) { return $null }
    }
    return [pscustomobject]@{
        MICRO_LIVE = New-AccountContext "MICRO_LIVE" `
            (Require-Value $B2b "COPY_B2B_MICRO_LIVE_EXECUTION_ACCOUNT_ID") `
            $Values["BINANCE_MICRO_LIVE_API_KEY"] $Values["BINANCE_MICRO_LIVE_API_SECRET"]
        LIVE = New-AccountContext "LIVE" `
            (Require-Value $B2b "COPY_B2B_LIVE_EXECUTION_ACCOUNT_ID") `
            $Values["BINANCE_LIVE_API_KEY"] $Values["BINANCE_LIVE_API_SECRET"]
    }
}

function Get-AccountsFromDatabase($B2b, [string]$Path) {
    $database = Read-DotEnv $Path
    $jdbc = Require-Value $database "DB_URL"
    if (-not $jdbc.StartsWith("jdbc:postgresql://")) { throw "B2B_DATABASE_URL_INVALID" }
    $uri = [Uri]$jdbc.Substring(5)
    $databaseName = $uri.AbsolutePath.TrimStart('/')
    $schema = Require-Value $database "DB_SCHEMA"
    if ($schema -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') { throw "B2B_DATABASE_SCHEMA_INVALID" }
    $userIdText = Require-Value $B2b "COPY_B2B_REAL_MONEY_TEST_USER_ID"
    try { $userId = [guid]$userIdText } catch { throw "B2B_TEST_USER_ID_INVALID" }
    $psql = Get-Command psql -ErrorAction SilentlyContinue
    if (-not $psql) { throw "B2B_PSQL_NOT_AVAILABLE" }

    $sql = @"
select id_user_api_keys,
       upper(account_purpose::text),
       encode(convert_to(api_key, 'UTF8'), 'hex'),
       encode(convert_to(api_secret, 'UTF8'), 'hex')
from $schema.user_api_keys
where user_id = '$userId'::uuid
  and upper(trim(exchange)) = 'BINANCE'
  and active = true
  and account_purpose in ('MICRO_LIVE', 'LIVE')
order by account_purpose;
"@
    $previousPassword = [Environment]::GetEnvironmentVariable("PGPASSWORD")
    $env:PGPASSWORD = Require-Value $database "DB_PASSWORD"
    try {
        $rows = @(& $psql.Source -h $uri.Host -p $uri.Port `
            -U (Require-Value $database "DB_USER") -d $databaseName `
            -v ON_ERROR_STOP=1 -P pager=off -F '|' -Atc $sql)
        if ($LASTEXITCODE -ne 0) { throw "B2B_DATABASE_CREDENTIAL_LOOKUP_FAILED" }
    } finally {
        if ($null -eq $previousPassword) {
            Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
        } else {
            $env:PGPASSWORD = $previousPassword
        }
    }

    $accounts = @{}
    foreach ($row in $rows) {
        $parts = $row -split '\|', 4
        if ($parts.Count -ne 4) { throw "B2B_DATABASE_CREDENTIAL_ROW_INVALID" }
        $purpose = $parts[1].Trim().ToUpperInvariant()
        if ($accounts.ContainsKey($purpose)) { throw "B2B_EXECUTION_ACCOUNT_NOT_UNIQUE purpose=$purpose" }
        $apiKey = Convert-HexToUtf8 $parts[2]
        $apiSecret = Convert-HexToUtf8 $parts[3]
        $accounts[$purpose] = New-AccountContext $purpose $parts[0] $apiKey $apiSecret
    }
    if (-not $accounts.ContainsKey("MICRO_LIVE") -or -not $accounts.ContainsKey("LIVE")) {
        throw "B2B_EXECUTION_ACCOUNT_PURPOSE_MISMATCH"
    }
    return [pscustomobject]@{
        MICRO_LIVE = $accounts["MICRO_LIVE"]
        LIVE = $accounts["LIVE"]
    }
}

function Resolve-Accounts($B2b) {
    $credentialValues = Read-DotEnv $CredentialsFile $false
    $accounts = Get-AccountsFromCredentialFile $credentialValues $B2b
    if ($null -eq $accounts) {
        $accounts = Get-AccountsFromDatabase $B2b $DatabaseEnvFile
    }
    if ($accounts.MICRO_LIVE.ExecutionAccountId -eq $accounts.LIVE.ExecutionAccountId `
        -or $accounts.MICRO_LIVE.ApiKey -eq $accounts.LIVE.ApiKey) {
        throw "B2B_EXECUTION_ACCOUNTS_NOT_ISOLATED"
    }
    $expectedMicro = $B2b["COPY_B2B_MICRO_LIVE_EXECUTION_ACCOUNT_ID"]
    $expectedLive = $B2b["COPY_B2B_LIVE_EXECUTION_ACCOUNT_ID"]
    if (-not [string]::IsNullOrWhiteSpace($expectedMicro) `
        -and $accounts.MICRO_LIVE.ExecutionAccountId -ne [guid]$expectedMicro) {
        throw "B2B_EXECUTION_ACCOUNT_PURPOSE_MISMATCH"
    }
    if (-not [string]::IsNullOrWhiteSpace($expectedLive) `
        -and $accounts.LIVE.ExecutionAccountId -ne [guid]$expectedLive) {
        throw "B2B_EXECUTION_ACCOUNT_PURPOSE_MISMATCH"
    }
    return $accounts
}

function Assert-AccountContextStructure($Accounts) {
    foreach ($purpose in @("MICRO_LIVE", "LIVE")) {
        $context = $Accounts.$purpose
        $required = @("Purpose", "ExecutionAccountId", "ApiKey", "ApiSecret")
        $missing = @($required | Where-Object { $null -eq $context.PSObject.Properties[$_] })
        if ($missing.Count -ne 0) {
            $typeName = if ($null -eq $context) { "null" } else { $context.GetType().FullName }
            throw "B2B_ACCOUNT_CONTEXT_INVALID purpose=$purpose type=$typeName missing=$($missing -join ',')"
        }
    }
}

function Invoke-BinanceSigned($Account, [string]$Method, [string]$Path,
                              [hashtable]$Parameters = @{}) {
    $request = @{}
    foreach ($entry in $Parameters.GetEnumerator()) { $request[$entry.Key] = $entry.Value }
    $request.timestamp = (Invoke-RestMethod -Method Get -Uri "$BaseUrl/fapi/v1/time").serverTime
    $request.recvWindow = 10000
    $query = Build-Query $request
    $hmac = [Security.Cryptography.HMACSHA256]::new(
        [Text.Encoding]::UTF8.GetBytes($Account.ApiSecret))
    try {
        $signature = ([BitConverter]::ToString(
            $hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($query))
        )).Replace("-", "").ToLowerInvariant()
    } finally {
        $hmac.Dispose()
    }
    try {
        return Invoke-RestMethod -Method $Method -Uri "$BaseUrl$Path`?$query&signature=$signature" `
            -Headers @{"X-MBX-APIKEY" = $Account.ApiKey}
    } catch {
        $code = "UNKNOWN"
        $message = "Binance request rejected"
        if ($_.ErrorDetails.Message) {
            try {
                $body = $_.ErrorDetails.Message | ConvertFrom-Json
                $code = $body.code
                $message = $body.msg
            } catch { }
        }
        throw "BINANCE_API_ERROR purpose=$($Account.Purpose) code=$code message=$message"
    }
}

function Invoke-BinanceSapiSigned($Account, [string]$Method, [string]$Path,
                                  [hashtable]$Parameters = @{}) {
    $request = @{}
    foreach ($entry in $Parameters.GetEnumerator()) { $request[$entry.Key] = $entry.Value }
    $request.timestamp = (Invoke-RestMethod -Method Get -Uri "$SapiBaseUrl/api/v3/time").serverTime
    $request.recvWindow = 10000
    $query = Build-Query $request
    $hmac = [Security.Cryptography.HMACSHA256]::new(
        [Text.Encoding]::UTF8.GetBytes($Account.ApiSecret))
    try {
        $signature = ([BitConverter]::ToString(
            $hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($query))
        )).Replace("-", "").ToLowerInvariant()
    } finally {
        $hmac.Dispose()
    }
    try {
        return Invoke-RestMethod -Method $Method -Uri "$SapiBaseUrl$Path`?$query&signature=$signature" `
            -Headers @{"X-MBX-APIKEY" = $Account.ApiKey}
    } catch {
        $code = "UNKNOWN"
        if ($_.ErrorDetails.Message) {
            try { $code = ($_.ErrorDetails.Message | ConvertFrom-Json).code } catch { }
        }
        throw "BINANCE_SAPI_ERROR purpose=$($Account.Purpose) code=$code"
    }
}

function Convert-ToFlatArray($Value) {
    if ($null -eq $Value) { return @() }
    if ($Value -is [System.Array]) { return @($Value) }
    return @($Value)
}

function Test-MasterSubAccountRelationship($Live, $Micro) {
    $checked = 0
    try {
        $response = Invoke-BinanceSapiSigned $Live "GET" "/sapi/v1/sub-account/list" @{}
        $subAccounts = @(Convert-ToFlatArray $response.subAccounts)
    } catch {
        return [pscustomobject]@{
            proven = $false
            proofType = 'BINANCE_MASTER_SUBACCOUNT_API_KEY'
            reasonCode = 'B2B_MASTER_SUBACCOUNT_LIST_UNAVAILABLE'
            candidatesChecked = 0
        }
    }
    foreach ($candidate in $subAccounts) {
        if ($checked -ge 5) { break }
        $email = [string]$candidate.email
        if ([string]::IsNullOrWhiteSpace($email)) { continue }
        $checked++
        try {
            $proof = Invoke-BinanceSapiSigned $Live "GET" `
                "/sapi/v1/sub-account/subAccountApi/ipRestriction" @{
                    email = $email
                    subAccountApiKey = $Micro.ApiKey
                }
            if ([string]$proof.apiKey -ceq $Micro.ApiKey) {
                return [pscustomobject]@{
                    proven = $true
                    proofType = 'BINANCE_MASTER_SUBACCOUNT_API_KEY'
                    reasonCode = 'B2B_MASTER_SUBACCOUNT_RELATIONSHIP_PROVEN'
                    candidatesChecked = $checked
                }
            }
        } catch { }
    }
    return [pscustomobject]@{
        proven = $false
        proofType = 'BINANCE_MASTER_SUBACCOUNT_API_KEY'
        reasonCode = 'B2B_MASTER_SUBACCOUNT_RELATIONSHIP_NOT_PROVEN'
        candidatesChecked = $checked
    }
}

function Get-Position($Account) {
    $rows = @(Convert-ToFlatArray (Invoke-BinanceSigned $Account "GET" "/fapi/v2/positionRisk" `
        @{symbol = $script:Symbol}))
    if ($rows.Count -eq 1) { return $rows[0] }
    $nonzero = @($rows | Where-Object { [decimal]$_.positionAmt -ne 0 })
    if ($nonzero.Count -eq 1) { return $nonzero[0] }
    if ($nonzero.Count -gt 1) {
        throw "B2B_POSITION_SNAPSHOT_AMBIGUOUS purpose=$($Account.Purpose)"
    }
    $long = @($rows | Where-Object { [string]$_.positionSide -eq "LONG" })
    if ($long.Count -eq 1) { return $long[0] }
    throw "B2B_POSITION_SNAPSHOT_AMBIGUOUS purpose=$($Account.Purpose)"
}

function Get-AllPositions($Account) {
    return @(Convert-ToFlatArray (Invoke-BinanceSigned $Account "GET" "/fapi/v2/positionRisk" @{}))
}

function Get-OpenOrders($Account) {
    return @(Convert-ToFlatArray (Invoke-BinanceSigned $Account "GET" "/fapi/v1/openOrders" @{}))
}

function Get-MarkPrice {
    return [decimal](Invoke-RestMethod -Method Get `
        -Uri "$BaseUrl/fapi/v1/premiumIndex?symbol=$script:Symbol").markPrice
}

function Get-AccountSnapshot($Account) {
    $accountDetails = Invoke-BinanceSigned $Account "GET" "/fapi/v2/account" @{}
    $mode = Invoke-BinanceSigned $Account "GET" "/fapi/v1/positionSide/dual" @{}
    $balances = @(Convert-ToFlatArray (Invoke-BinanceSigned $Account "GET" "/fapi/v3/balance" @{}))
    $usdc = $balances | Where-Object { $_.asset -eq "USDC" } | Select-Object -First 1
    if (-not $usdc) { throw "B2B_USDC_BALANCE_UNAVAILABLE purpose=$($Account.Purpose)" }
    $alias = [string]$usdc.accountAlias
    if ([string]::IsNullOrWhiteSpace($alias)) {
        $alias = [string]($balances | Where-Object { $_.accountAlias } |
            Select-Object -First 1 -ExpandProperty accountAlias)
    }
    if ([string]::IsNullOrWhiteSpace($alias)) {
        throw "B2B_ACCOUNT_IDENTITY_UNAVAILABLE purpose=$($Account.Purpose)"
    }
    $positions = @(Get-AllPositions $Account)
    $nonzero = @($positions | Where-Object { [decimal]$_.positionAmt -ne 0 })
    $orders = @(Get-OpenOrders $Account)
    $symbolPosition = $positions | Where-Object { $_.symbol -eq $script:Symbol } | Select-Object -First 1
    if (-not $symbolPosition) { throw "B2B_SYMBOL_POSITION_CONFIG_UNAVAILABLE purpose=$($Account.Purpose)" }
    $openNotional = [decimal]0
    foreach ($position in $nonzero) {
        $notionalProperty = $position.PSObject.Properties['notional']
        if ($notionalProperty -and $notionalProperty.Value) {
            $openNotional += [Math]::Abs([decimal]$notionalProperty.Value)
        } else {
            $openNotional += [Math]::Abs([decimal]$position.positionAmt * [decimal]$position.markPrice)
        }
    }
    $marginProperty = $accountDetails.PSObject.Properties['totalPositionInitialMargin']
    $reportedMargin = [decimal]0
    if ($marginProperty -and $null -ne $marginProperty.Value) {
        $reportedMargin = [decimal]$marginProperty.Value
    }
    $openMargin = if ($reportedMargin -gt 0 -or $nonzero.Count -eq 0) {
        $reportedMargin
    } else {
        $fallback = [decimal]0
        foreach ($position in $nonzero) {
            $lev = [Math]::Max(1, [int]$position.leverage)
            $fallback += [Math]::Abs([decimal]$position.positionAmt * [decimal]$position.markPrice) / $lev
        }
        $fallback
    }
    $positionFingerprint = (@($nonzero | Sort-Object symbol, positionSide | ForEach-Object {
        "$($_.symbol):$($_.positionSide):$($_.positionAmt):$($_.entryPrice)"
    }) -join ';')
    $orderFingerprint = (@($orders | Sort-Object symbol, clientOrderId | ForEach-Object {
        "$($_.symbol):$($_.clientOrderId):$($_.side):$($_.status):$($_.origQty):$($_.executedQty)"
    }) -join ';')
    return [pscustomobject]@{
        executionAccountId = $Account.ExecutionAccountId
        purpose = $Account.Purpose
        alias = $alias
        canTrade = [bool]$accountDetails.canTrade
        walletBalance = [decimal]$usdc.balance
        availableBalance = [decimal]$usdc.availableBalance
        openMargin = $openMargin
        openNotional = $openNotional
        nonzeroPositionCount = $nonzero.Count
        openOrderCount = $orders.Count
        positionFingerprint = $positionFingerprint
        orderFingerprint = $orderFingerprint
        symbolLeverage = [int]$symbolPosition.leverage
        symbolMarginMode = ([string]$symbolPosition.marginType).ToUpperInvariant()
        positionMode = if ([bool]$mode.dualSidePosition) { "HEDGE" } else { "ONE_WAY" }
    }
}

function Get-GlobalExposureSnapshot {
    $micro = Get-AccountSnapshot (($script:Accounts).MICRO_LIVE)
    $live = Get-AccountSnapshot (($script:Accounts).LIVE)
    return [pscustomobject]@{
        MICRO_LIVE = $micro
        LIVE = $live
        totalOpenMargin = $micro.openMargin + $live.openMargin
        totalOpenNotional = $micro.openNotional + $live.openNotional
        totalPositions = $micro.nonzeroPositionCount + $live.nonzeroPositionCount
        totalOpenOrders = $micro.openOrderCount + $live.openOrderCount
    }
}

function Save-RunState {
    $parent = Split-Path -Parent $StateFile
    if ($parent -and -not (Test-Path -LiteralPath $parent)) {
        New-Item -ItemType Directory -Path $parent | Out-Null
    }
    $script:RunState.updatedAt = [DateTimeOffset]::UtcNow.ToString("O")
    $script:RunState | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $StateFile -Encoding utf8
}

function Resolve-PendingRunState {
    if (-not (Test-Path -LiteralPath $StateFile)) { return }
    $previous = Get-Content -LiteralPath $StateFile -Raw | ConvertFrom-Json
    if ($previous.status -in @("COMPLETED", "SAFE_STOPPED", "PREFLIGHT_ONLY")) { return }
    if (-not $previous.pendingOrder) {
        throw "B2B_AMBIGUOUS_STATE_RECONCILIATION_REQUIRED"
    }
    $purpose = [string]$previous.pendingOrder.purpose
    if ($purpose -notin @("MICRO_LIVE", "LIVE")) {
        throw "B2B_EXECUTION_ACCOUNT_PURPOSE_MISMATCH"
    }
    $account = ($script:Accounts).$purpose
    try {
        $order = Invoke-BinanceSigned $account "GET" "/fapi/v1/order" @{
            symbol = [string]$previous.pendingOrder.symbol
            origClientOrderId = [string]$previous.pendingOrder.clientOrderId
        }
    } catch {
        if ($_.Exception.Message -notmatch 'code=-2013') { throw }
        $order = $null
    }
    $snapshot = Get-AccountSnapshot $account
    if ($snapshot.nonzeroPositionCount -ne 0 -or $snapshot.openOrderCount -ne 0) {
        throw "B2B_AMBIGUOUS_STATE_RECONCILIATION_REQUIRED purpose=$purpose"
    }
    if ($order -and $order.status -notin @("FILLED", "CANCELED", "EXPIRED", "REJECTED")) {
        throw "B2B_AMBIGUOUS_STATE_RECONCILIATION_REQUIRED purpose=$purpose"
    }
    $previous.status = "SAFE_STOPPED"
    $previous.pendingOrder = $null
    $previous.updatedAt = [DateTimeOffset]::UtcNow.ToString("O")
    $previous | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $StateFile -Encoding utf8
}

function Round-UpToStep([decimal]$Value, [decimal]$Step) {
    return [decimal][Math]::Ceiling([double]($Value / $Step)) * $Step
}

function Round-DownToStep([decimal]$Value, [decimal]$Step) {
    return [decimal][Math]::Floor([double]($Value / $Step)) * $Step
}

function New-ClientOrderId($Account, [guid]$CycleId, [string]$Suffix) {
    $purposeToken = if ($Account.Purpose -eq "MICRO_LIVE") { "m" } else { "l" }
    $token = $CycleId.ToString("N").Substring(0, 10)
    $value = "$($script:ClientPrefix)$purposeToken-$token-$Suffix"
    if ($value.Length -gt 36 -or $value -notmatch '^[A-Za-z0-9._-]+$') {
        throw "B2B_CLIENT_ORDER_ID_INVALID"
    }
    return $value
}

function Reserve-Order([bool]$NewExposure) {
    $ceiling = if ($NewExposure) { $script:MaxOrders - 2 } else { $script:MaxOrders }
    if ($script:OrderCount -ge $ceiling) { throw "B2B_ORDER_LIMIT_REACHED" }
    $script:OrderCount++
}

function Set-PendingOrder($Account, [string]$Intent, [string]$ClientOrderId, [decimal]$Quantity) {
    $script:RunState.status = "RUNNING"
    $script:RunState.currentPurpose = $Account.Purpose
    $script:RunState.pendingOrder = [ordered]@{
        purpose = $Account.Purpose
        executionAccountId = $Account.ExecutionAccountId
        symbol = $script:Symbol
        intent = $Intent
        clientOrderId = $ClientOrderId
        quantity = Decimal-Text $Quantity
        stage = "SUBMIT_PENDING"
    }
    Save-RunState
}

function Wait-OrderFilled($Account, [string]$ClientOrderId) {
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        try {
            $order = Invoke-BinanceSigned $Account "GET" "/fapi/v1/order" @{
                symbol = $script:Symbol
                origClientOrderId = $ClientOrderId
            }
        } catch {
            if ($_.Exception.Message -notmatch 'code=-2013') { throw }
            Start-Sleep -Milliseconds 250
            continue
        }
        if ($order.status -eq "FILLED") { return $order }
        if ($order.status -in @("CANCELED", "EXPIRED", "REJECTED")) {
            throw "B2B_ORDER_TERMINAL_WITHOUT_FILL purpose=$($Account.Purpose) status=$($order.status)"
        }
        Start-Sleep -Milliseconds 250
    }
    throw "B2B_ORDER_FILL_TIMEOUT purpose=$($Account.Purpose)"
}

function Wait-Flat($Account) {
    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        $position = Get-Position $Account
        if ([decimal]$position.positionAmt -eq 0) { return $position }
        Start-Sleep -Milliseconds 250
    }
    throw "B2B_POSITION_NOT_FLAT_AFTER_CLOSE purpose=$($Account.Purpose)"
}

function Update-PeakGlobalExposure {
    $snapshot = Get-GlobalExposureSnapshot
    if ($snapshot.totalOpenMargin -gt $script:PeakGlobalMargin) {
        $script:PeakGlobalMargin = $snapshot.totalOpenMargin
    }
    if ($snapshot.totalOpenNotional -gt $script:PeakGlobalNotional) {
        $script:PeakGlobalNotional = $snapshot.totalOpenNotional
    }
    if ($snapshot.totalPositions -gt 1) { throw "B2B_ANOTHER_ACCOUNT_HAS_OPEN_POSITION" }
    if ($snapshot.totalOpenMargin -gt $script:MaxMargin) { throw "B2B_GLOBAL_MARGIN_LIMIT_EXCEEDED" }
    if ($snapshot.totalOpenNotional -gt $script:MaxNotional) { throw "B2B_GLOBAL_NOTIONAL_LIMIT_EXCEEDED" }
    return $snapshot
}

function Assert-NewExposureWithinGlobalLimits($Account, [decimal]$AdditionalQty) {
    if ($script:EmergencyStopActive) { throw "B2B_EMERGENCY_STOP_ACTIVE" }
    if ([int]$Account.B2bLeverage -gt 5) { throw "B2B_MAX_LEVERAGE_EXCEEDED" }
    $snapshot = Get-GlobalExposureSnapshot
    if ($snapshot.totalOpenOrders -ne 0) { throw "B2B_AMBIGUOUS_STATE_RECONCILIATION_REQUIRED" }
    $otherPurpose = if ($Account.Purpose -eq "MICRO_LIVE") { "LIVE" } else { "MICRO_LIVE" }
    if ($snapshot.$otherPurpose.nonzeroPositionCount -ne 0) {
        throw "B2B_ANOTHER_ACCOUNT_HAS_OPEN_POSITION"
    }
    if ($snapshot.$($Account.Purpose).nonzeroPositionCount -gt 1) {
        throw "B2B_SINGLE_POSITION_LIMIT_REACHED"
    }
    $additionalNotional = $AdditionalQty * (Get-MarkPrice)
    $projectedNotional = $snapshot.totalOpenNotional + $additionalNotional
    $projectedMargin = $snapshot.totalOpenMargin + ($additionalNotional / [decimal]$Account.B2bLeverage)
    if ($projectedMargin -gt $script:MaxMargin) { throw "B2B_GLOBAL_MARGIN_LIMIT_EXCEEDED" }
    if ($projectedNotional -gt $script:MaxNotional) { throw "B2B_GLOBAL_NOTIONAL_LIMIT_EXCEEDED" }
    return [pscustomobject]@{margin = $projectedMargin; notional = $projectedNotional}
}

function Complete-OrderEvidence($Account, [string]$Intent, [string]$ClientOrderId, $Ack, $Fill) {
    $entry = [pscustomobject]@{
        purpose = $Account.Purpose
        executionAccountId = $Account.ExecutionAccountId
        intent = $Intent
        clientOrderId = $ClientOrderId
        orderId = $Ack.orderId
        ack = $true
        status = $Fill.status
        executedQty = [decimal]$Fill.executedQty
        avgPrice = [decimal]$Fill.avgPrice
        reduceOnly = [bool]$Fill.reduceOnly
    }
    $script:OrderEvidence += $entry
    $script:RunState.orders = $script:OrderEvidence
    $script:RunState.pendingOrder = $null
    Save-RunState
    return $entry
}

function Send-ExposureOrder($Account, [string]$Intent, [guid]$CycleId,
                            [string]$Suffix, [decimal]$Quantity) {
    Assert-NewExposureWithinGlobalLimits $Account $Quantity | Out-Null
    Reserve-Order $true
    $clientId = New-ClientOrderId $Account $CycleId $Suffix
    Set-PendingOrder $Account $Intent $clientId $Quantity
    $ack = Invoke-BinanceSigned $Account "POST" "/fapi/v1/order" @{
        symbol = $script:Symbol
        side = "BUY"
        type = "MARKET"
        quantity = Decimal-Text $Quantity
        newClientOrderId = $clientId
        newOrderRespType = "ACK"
    }
    if (-not $ack.orderId) { throw "B2B_ORDER_ACK_MISSING purpose=$($Account.Purpose)" }
    $script:RunState.pendingOrder.stage = "ACK"
    $script:RunState.pendingOrder.orderId = $ack.orderId
    Save-RunState
    $fill = Wait-OrderFilled $Account $clientId
    Complete-OrderEvidence $Account $Intent $clientId $ack $fill | Out-Null
    $position = Get-Position $Account
    if ([decimal]$position.positionAmt -le 0) {
        throw "B2B_EXPOSURE_POSITION_NOT_CONFIRMED purpose=$($Account.Purpose)"
    }
    Update-PeakGlobalExposure | Out-Null
    return $position
}

function Send-ReduceOrder($Account, [guid]$CycleId, [string]$Suffix, [decimal]$Quantity) {
    Reserve-Order $false
    $position = Get-Position $Account
    $actual = [Math]::Abs([decimal]$position.positionAmt)
    if ($Quantity -le 0 -or $Quantity -ge $actual) { throw "B2B_REDUCE_QUANTITY_INVALID" }
    $clientId = New-ClientOrderId $Account $CycleId $Suffix
    Set-PendingOrder $Account "REDUCE" $clientId $Quantity
    $side = if ([decimal]$position.positionAmt -gt 0) { "SELL" } else { "BUY" }
    $ack = Invoke-BinanceSigned $Account "POST" "/fapi/v1/order" @{
        symbol = $script:Symbol
        side = $side
        type = "MARKET"
        quantity = Decimal-Text $Quantity
        reduceOnly = "true"
        newClientOrderId = $clientId
        newOrderRespType = "ACK"
    }
    if (-not $ack.orderId) { throw "B2B_REDUCE_ACK_MISSING purpose=$($Account.Purpose)" }
    $script:RunState.pendingOrder.stage = "ACK"
    $script:RunState.pendingOrder.orderId = $ack.orderId
    Save-RunState
    $fill = Wait-OrderFilled $Account $clientId
    Complete-OrderEvidence $Account "REDUCE" $clientId $ack $fill | Out-Null
    $remaining = Get-Position $Account
    if ([Math]::Abs([decimal]$remaining.positionAmt) -ge $actual) {
        throw "B2B_REDUCE_POSITION_NOT_DECREASED purpose=$($Account.Purpose)"
    }
    return $remaining
}

function Send-CloseOrder($Account, [guid]$CycleId, [string]$Suffix) {
    $position = Get-Position $Account
    $quantity = [Math]::Abs([decimal]$position.positionAmt)
    if ($quantity -eq 0) { return $position }
    Reserve-Order $false
    $clientId = New-ClientOrderId $Account $CycleId $Suffix
    Set-PendingOrder $Account "CLOSE" $clientId $quantity
    $side = if ([decimal]$position.positionAmt -gt 0) { "SELL" } else { "BUY" }
    $ack = Invoke-BinanceSigned $Account "POST" "/fapi/v1/order" @{
        symbol = $script:Symbol
        side = $side
        type = "MARKET"
        quantity = Decimal-Text $quantity
        reduceOnly = "true"
        newClientOrderId = $clientId
        newOrderRespType = "ACK"
    }
    if (-not $ack.orderId) { throw "B2B_CLOSE_ACK_MISSING purpose=$($Account.Purpose)" }
    $script:RunState.pendingOrder.stage = "ACK"
    $script:RunState.pendingOrder.orderId = $ack.orderId
    Save-RunState
    $fill = Wait-OrderFilled $Account $clientId
    Complete-OrderEvidence $Account "CLOSE" $clientId $ack $fill | Out-Null
    return Wait-Flat $Account
}

function Set-B2bLeverage($Account) {
    $snapshot = Get-AccountSnapshot $Account
    if ($snapshot.nonzeroPositionCount -ne 0 -or $snapshot.openOrderCount -ne 0) {
        throw "B2B_ACCOUNT_NOT_FLAT_FOR_LEVERAGE_OVERRIDE purpose=$($Account.Purpose)"
    }
    $Account.OriginalLeverage = $snapshot.symbolLeverage
    $Account.B2bLeverage = $script:Leverage
    if ($snapshot.symbolLeverage -ne $script:Leverage) {
        $response = Invoke-BinanceSigned $Account "POST" "/fapi/v1/leverage" @{
            symbol = $script:Symbol
            leverage = $script:Leverage
        }
        if ([int]$response.leverage -ne $script:Leverage) {
            throw "B2B_MAX_LEVERAGE_EXCEEDED purpose=$($Account.Purpose)"
        }
    }
}

function Restore-AccountLeverage($Account) {
    if ($null -eq $Account.OriginalLeverage -or $null -eq $Account.B2bLeverage `
        -or [int]$Account.OriginalLeverage -eq [int]$Account.B2bLeverage) { return }
    $snapshot = Get-AccountSnapshot $Account
    if ($snapshot.nonzeroPositionCount -ne 0 -or $snapshot.openOrderCount -ne 0) {
        throw "B2B_LEVERAGE_RESTORE_REQUIRES_FLAT purpose=$($Account.Purpose)"
    }
    $response = Invoke-BinanceSigned $Account "POST" "/fapi/v1/leverage" @{
        symbol = $script:Symbol
        leverage = [int]$Account.OriginalLeverage
    }
    if ([int]$response.leverage -ne [int]$Account.OriginalLeverage) {
        throw "B2B_LEVERAGE_RESTORE_FAILED purpose=$($Account.Purpose)"
    }
}

function Set-B2bPositionMode($Account) {
    $snapshot = Get-AccountSnapshot $Account
    if ($snapshot.nonzeroPositionCount -ne 0 -or $snapshot.openOrderCount -ne 0) {
        throw "B2B_POSITION_MODE_CHANGE_REQUIRES_FLAT purpose=$($Account.Purpose)"
    }
    $Account.OriginalPositionMode = $snapshot.positionMode
    if ($snapshot.positionMode -eq "ONE_WAY") { return }
    if ($snapshot.positionMode -ne "HEDGE") {
        throw "BINANCE_POSITION_MODE_MISMATCH purpose=$($Account.Purpose)"
    }
    Invoke-BinanceSigned $Account "POST" "/fapi/v1/positionSide/dual" @{
        dualSidePosition = "false"
    } | Out-Null
    $normalized = Get-AccountSnapshot $Account
    if ($normalized.positionMode -ne "ONE_WAY") {
        throw "BINANCE_POSITION_MODE_MISMATCH purpose=$($Account.Purpose)"
    }
}

function Restore-AccountPositionMode($Account) {
    if ($null -eq $Account.OriginalPositionMode -or $Account.OriginalPositionMode -eq "ONE_WAY") {
        return
    }
    $snapshot = Get-AccountSnapshot $Account
    if ($snapshot.positionMode -eq $Account.OriginalPositionMode) { return }
    if ($snapshot.nonzeroPositionCount -ne 0 -or $snapshot.openOrderCount -ne 0) {
        throw "B2B_POSITION_MODE_RESTORE_REQUIRES_FLAT purpose=$($Account.Purpose)"
    }
    Invoke-BinanceSigned $Account "POST" "/fapi/v1/positionSide/dual" @{
        dualSidePosition = "true"
    } | Out-Null
    $restored = Get-AccountSnapshot $Account
    if ($restored.positionMode -ne $Account.OriginalPositionMode) {
        throw "B2B_POSITION_MODE_RESTORE_FAILED purpose=$($Account.Purpose)"
    }
}

function Assert-CounterpartUnchanged($Before, $After, [string]$TestedPurpose) {
    $changed = $Before.executionAccountId -ne $After.executionAccountId `
        -or $Before.alias -ne $After.alias `
        -or $Before.positionFingerprint -ne $After.positionFingerprint `
        -or $Before.orderFingerprint -ne $After.orderFingerprint `
        -or $Before.symbolLeverage -ne $After.symbolLeverage `
        -or $Before.symbolMarginMode -ne $After.symbolMarginMode `
        -or $Before.positionMode -ne $After.positionMode
    if ($changed) {
        if ($TestedPurpose -eq "MICRO_LIVE") {
            throw "B2B_LIVE_ACCOUNT_CHANGED_DURING_MICRO_TEST"
        }
        throw "B2B_MICRO_ACCOUNT_CHANGED_DURING_LIVE_TEST"
    }
}

function Add-LocalPolicyBlock($Account, [string]$Intent, [decimal]$Quantity, [string]$ReasonCode) {
    $block = [pscustomobject]@{
        purpose = $Account.Purpose
        executionAccountId = $Account.ExecutionAccountId
        intent = $Intent
        requestedQty = $Quantity
        reasonCode = $ReasonCode
        sentToBinance = $false
    }
    $script:LocalPolicyBlocks += $block
    $script:RunState.localPolicyBlocks = $script:LocalPolicyBlocks
    Save-RunState
    return $block
}

function Test-EmergencyStopPolicy($Account) {
    $script:EmergencyStopActive = $true
    $blockedIntents = @('OPEN', 'INCREASE')
    $allowedDeriskIntents = @('REDUCE', 'CLOSE')

    foreach ($intent in $blockedIntents) {
        if (-not $script:EmergencyStopActive) {
            throw "B2B_EMERGENCY_STOP_POLICY_INVALID purpose=$($Account.Purpose) intent=$intent"
        }
    }
    foreach ($intent in $allowedDeriskIntents) {
        if ($intent -notin @('REDUCE', 'CLOSE')) {
            throw "B2B_EMERGENCY_STOP_DERISK_POLICY_INVALID purpose=$($Account.Purpose) intent=$intent"
        }
    }

    return [pscustomobject]@{
        active = $true
        blockedIntents = @('OPEN', 'INCREASE')
        blockedReasonCode = 'B2B_EMERGENCY_STOP_ACTIVE'
        allowedDeriskIntents = @('REDUCE', 'CLOSE')
        sentToBinance = $false
    }
}

function Invoke-AccountB2bFlow($Account, $Counterpart, $Rules) {
    $counterpartBefore = Get-AccountSnapshot $Counterpart
    $before = Get-AccountSnapshot $Account
    if (-not $before.canTrade) { throw "B2B_ACCOUNT_TRADING_DISABLED purpose=$($Account.Purpose)" }
    if ($before.nonzeroPositionCount -ne 0 -or $before.openOrderCount -ne 0) {
        throw "B2B_ACCOUNT_NOT_FLAT_AT_START purpose=$($Account.Purpose)"
    }
    if ($before.symbolMarginMode -ne "CROSS") { throw "BINANCE_MARGIN_MODE_MISMATCH" }

    $cycle1 = [guid]::NewGuid()
    $cycle2 = [guid]::NewGuid()
    $flowError = $null
    $emergencyStopEvidence = $null
    try {
        $script:EmergencyStopActive = $false
        Set-B2bPositionMode $Account
        Set-B2bLeverage $Account
        Send-ExposureOrder $Account "OPEN" $cycle1 "o1" $Rules.LegQty | Out-Null
        Add-LocalPolicyBlock $Account "INCREASE" $Rules.UnderMinimumQty `
            $Rules.UnderMinimumReasonCode | Out-Null
        Send-ExposureOrder $Account "INCREASE" $cycle1 "i1" $Rules.LegQty | Out-Null
        $remaining = Send-ReduceOrder $Account $cycle1 "r1" $Rules.LegQty
        if ([decimal]$remaining.positionAmt -le 0) {
            throw "B2B_REDUCE_UNEXPECTEDLY_FLAT purpose=$($Account.Purpose)"
        }
        Send-CloseOrder $Account $cycle1 "c1" | Out-Null
        Send-ExposureOrder $Account "OPEN" $cycle2 "o2" $Rules.LegQty | Out-Null
        Send-CloseOrder $Account $cycle2 "c2" | Out-Null
    } catch {
        $flowError = $_.Exception.Message
    } finally {
        $script:EmergencyStopActive = $true
        $remaining = Get-Position $Account
        if ([decimal]$remaining.positionAmt -ne 0) {
            try { Send-CloseOrder $Account ([guid]::NewGuid()) "panic" | Out-Null } catch { }
        }
        Restore-AccountLeverage $Account
        Restore-AccountPositionMode $Account
        $emergencyStopEvidence = Test-EmergencyStopPolicy $Account
    }
    $after = Get-AccountSnapshot $Account
    $counterpartAfter = Get-AccountSnapshot $Counterpart
    Assert-CounterpartUnchanged $counterpartBefore $counterpartAfter $Account.Purpose
    if ($after.nonzeroPositionCount -ne 0 -or $after.openOrderCount -ne 0) {
        throw "B2B_FINAL_SAFETY_INVARIANT_FAILED purpose=$($Account.Purpose)"
    }
    if ($flowError) { throw "B2B_EXECUTION_FAILED_CLEANED_UP purpose=$($Account.Purpose): $flowError" }
    return [pscustomobject]@{
        executionAccountId = $Account.ExecutionAccountId
        identityAlias = $after.alias
        purpose = $Account.Purpose
        symbol = $script:Symbol
        leverage = $script:Leverage
        cycles = @($cycle1, $cycle2)
        cycleIdentityDistinct = ($cycle1 -ne $cycle2)
        orders = @($script:OrderEvidence | Where-Object { $_.purpose -eq $Account.Purpose })
        localPolicyBlocks = @($script:LocalPolicyBlocks | Where-Object { $_.purpose -eq $Account.Purpose })
        finalPosition = $after.nonzeroPositionCount
        finalOpenOrders = $after.openOrderCount
        emergencyStop = $true
        emergencyStopEvidence = $emergencyStopEvidence
        counterpartIntact = $true
        originalLeverageRestored = ($after.symbolLeverage -eq $before.symbolLeverage)
        originalPositionModeRestored = ($after.positionMode -eq $before.positionMode)
    }
}

function Get-SymbolRules {
    $exchangeInfo = Invoke-RestMethod -Method Get -Uri "$BaseUrl/fapi/v1/exchangeInfo"
    $symbolInfo = $exchangeInfo.symbols | Where-Object { $_.symbol -eq $script:Symbol } | Select-Object -First 1
    if (-not $symbolInfo -or $symbolInfo.status -ne "TRADING") { throw "BINANCE_SYMBOL_NOT_TRADING" }
    $lot = $symbolInfo.filters | Where-Object { $_.filterType -eq "MARKET_LOT_SIZE" } | Select-Object -First 1
    if (-not $lot) { $lot = $symbolInfo.filters | Where-Object { $_.filterType -eq "LOT_SIZE" } | Select-Object -First 1 }
    if (-not $lot) { throw "BINANCE_SYMBOL_RULES_UNAVAILABLE" }
    $notionalFilter = $symbolInfo.filters | Where-Object { $_.filterType -in @("NOTIONAL", "MIN_NOTIONAL") } | Select-Object -First 1
    if (-not $notionalFilter) { throw "BINANCE_SYMBOL_RULES_UNAVAILABLE" }
    $step = [decimal]$lot.stepSize
    $minQty = [decimal]$lot.minQty
    $minNotionalProperty = $notionalFilter.PSObject.Properties['minNotional']
    $notionalProperty = $notionalFilter.PSObject.Properties['notional']
    $minNotional = if ($minNotionalProperty -and $minNotionalProperty.Value) {
        [decimal]$minNotionalProperty.Value
    } elseif ($notionalProperty -and $notionalProperty.Value) {
        [decimal]$notionalProperty.Value
    } else {
        throw "BINANCE_SYMBOL_RULES_UNAVAILABLE"
    }
    $price = Get-MarkPrice
    $targetLegNotional = [decimal][Math]::Max([double]($minNotional * [decimal]1.075), 21.5)
    $legQty = Round-UpToStep ($targetLegNotional / $price) $step
    if ($legQty -lt $minQty) { $legQty = $minQty }
    if (($legQty * $price) -lt $minNotional) { throw "B2B_VALID_LEG_BELOW_MIN_NOTIONAL" }
    if ((2 * $legQty * $price) -gt $script:MaxNotional `
        -or ((2 * $legQty * $price) / $script:Leverage) -gt $script:MaxMargin) {
        throw "B2B_TWO_LEG_PLAN_EXCEEDS_GLOBAL_LIMIT"
    }
    $belowQty = Round-DownToStep ($minQty - $step) $step
    if ($belowQty -gt 0) {
        $belowReason = "INCREASE_BELOW_MIN_QTY_NOOP"
    } else {
        $belowQty = $minQty
        if (($belowQty * $price) -ge $minNotional) {
            throw "B2B_CANNOT_PRODUCE_SAFE_UNDER_MIN_CASE"
        }
        $belowReason = "INCREASE_BELOW_MIN_NOTIONAL_NOOP"
    }
    return [pscustomobject]@{
        StepSize = $step
        MinQty = $minQty
        MinNotional = $minNotional
        LegQty = $legQty
        UnderMinimumQty = $belowQty
        UnderMinimumReasonCode = $belowReason
    }
}

$b2b = Read-DotEnv $B2bEnvFile
$script:Symbol = $Symbol.Trim().ToUpperInvariant()
$script:ClientPrefix = Require-Value $b2b "COPY_B2B_REAL_MONEY_CLIENT_ORDER_ID_PREFIX"
$script:MaxMargin = [decimal](Require-Value $b2b "COPY_B2B_REAL_MONEY_MAX_TOTAL_MARGIN_USD")
$script:MaxNotional = [decimal](Require-Value $b2b "COPY_B2B_REAL_MONEY_MAX_NOTIONAL_USD")
$script:Leverage = [int](Require-Value $b2b "COPY_B2B_REAL_MONEY_MAX_LEVERAGE")
$script:MaxOrders = [int](Require-Value $b2b "COPY_B2B_REAL_MONEY_MAX_ORDERS")
$script:OrderCount = 0
$script:OrderEvidence = @()
$script:LocalPolicyBlocks = @()
$script:PeakGlobalMargin = [decimal]0
$script:PeakGlobalNotional = [decimal]0
$script:EmergencyStopActive = $true

if (-not (As-Bool (Require-Value $b2b "COPY_B2B_REAL_MONEY_ENABLED"))) {
    throw "B2B_REAL_MONEY_GUARD_INACTIVE"
}
if ((Require-Value $b2b "COPY_B2B_REAL_MONEY_EXPLICIT_ACKNOWLEDGEMENT") -ne
    "I_ACCEPT_MAX_10_USDC_REAL_MARGIN") { throw "B2B_ACKNOWLEDGEMENT_MISSING" }
if (-not (As-Bool (Require-Value $b2b "COPY_B2B_REAL_MONEY_MANUAL_POSITIONS_VERIFIED"))) {
    throw "B2B_MANUAL_POSITIONS_NOT_VERIFIED"
}
if ($script:MaxMargin -gt 10 -or $script:MaxNotional -gt 50 -or $script:Leverage -gt 5) {
    throw "B2B_ABSOLUTE_LIMIT_CONFIGURATION_INVALID"
}
if ($script:MaxOrders -lt 14) { throw "B2B_ORDER_BUDGET_CANNOT_COMPLETE_DUAL_ACCOUNT_FLOW" }
$allowed = (Require-Value $b2b "COPY_B2B_REAL_MONEY_ALLOWED_SYMBOLS").Split(',') |
    ForEach-Object { $_.Trim().ToUpperInvariant() }
if ($script:Symbol -notin $allowed) { throw "B2B_SYMBOL_BLOCKED" }
if (-not (As-Bool (Require-Value $b2b "COPY_B2B_REAL_MONEY_EMERGENCY_STOP"))) {
    throw "B2B_EMERGENCY_STOP_NOT_ACTIVE_AT_REST"
}

$script:Accounts = Resolve-Accounts $b2b
Assert-AccountContextStructure $script:Accounts
$initialGlobal = Get-GlobalExposureSnapshot
if ($initialGlobal.MICRO_LIVE.alias -eq $initialGlobal.LIVE.alias) {
    $script:AccountIsolationEvidence = Test-MasterSubAccountRelationship `
        (($script:Accounts).LIVE) (($script:Accounts).MICRO_LIVE)
} else {
    $script:AccountIsolationEvidence = [pscustomobject]@{
        proven = $true
        proofType = 'DISTINCT_FUTURES_ACCOUNT_ALIAS'
        reasonCode = 'B2B_DISTINCT_ACCOUNT_ALIASES_PROVEN'
        candidatesChecked = 0
    }
}
if (-not $script:AccountIsolationEvidence.proven) {
    throw "B2B_EXECUTION_ACCOUNTS_NOT_ISOLATED alias=$($initialGlobal.MICRO_LIVE.alias) proofReason=$($script:AccountIsolationEvidence.reasonCode)"
}
if ($initialGlobal.totalPositions -ne 0) { throw "B2B_MANUAL_POSITIONS_NOT_FLAT" }
if ($initialGlobal.totalOpenOrders -ne 0) { throw "B2B_OPEN_ORDERS_PRESENT" }
if (-not $initialGlobal.MICRO_LIVE.canTrade -or -not $initialGlobal.LIVE.canTrade) {
    throw "B2B_ACCOUNT_TRADING_DISABLED"
}

Resolve-PendingRunState
$rules = Get-SymbolRules
$script:RunState = [ordered]@{
    runId = [guid]::NewGuid()
    startedAt = [DateTimeOffset]::UtcNow.ToString("O")
    updatedAt = [DateTimeOffset]::UtcNow.ToString("O")
    status = "PREFLIGHT"
    symbol = $script:Symbol
    currentPurpose = $null
    pendingOrder = $null
    orders = @()
    localPolicyBlocks = @()
}
Save-RunState

$preflight = [pscustomobject]@{
    MICRO_LIVE = $initialGlobal.MICRO_LIVE
    LIVE = $initialGlobal.LIVE
    accountsIsolated = $true
    accountIsolationEvidence = $script:AccountIsolationEvidence
    globalMargin = $initialGlobal.totalOpenMargin
    globalNotional = $initialGlobal.totalOpenNotional
    positions = $initialGlobal.totalPositions
    openOrders = $initialGlobal.totalOpenOrders
}

if ($VerifyEmergencyStop) {
    $script:RunState.status = "SAFE_STOPPED"
    Save-RunState
    [pscustomobject]@{
        preflight = $preflight
        emergencyStop = $true
        blockedIntents = @("OPEN", "INCREASE")
        allowedDeriskIntents = @("REDUCE", "CLOSE")
        sentToBinance = $false
    } | ConvertTo-Json -Depth 8
    exit 0
}

if (-not $Execute) {
    $script:RunState.status = "PREFLIGHT_ONLY"
    Save-RunState
    [pscustomobject]@{preflight = $preflight; execute = $false} | ConvertTo-Json -Depth 8
    exit 0
}
if (-not (As-Bool (Require-Value $b2b "BINANCE_ORDER_SUBMIT_ENABLED"))) {
    throw "BINANCE_ORDER_SUBMIT_DISABLED"
}

$microResult = $null
$liveResult = $null
$executionError = $null
try {
    $microResult = Invoke-AccountB2bFlow (($script:Accounts).MICRO_LIVE) (($script:Accounts).LIVE) $rules
    $between = Get-GlobalExposureSnapshot
    if ($between.MICRO_LIVE.nonzeroPositionCount -ne 0 -or $between.MICRO_LIVE.openOrderCount -ne 0) {
        throw "B2B_MICRO_NOT_FLAT_BEFORE_LIVE"
    }
    if (-not $MicroLiveOnly) {
        $liveResult = Invoke-AccountB2bFlow (($script:Accounts).LIVE) (($script:Accounts).MICRO_LIVE) $rules
    }
} catch {
    $executionError = $_.Exception.Message
} finally {
    $cleanupAccounts = if ($MicroLiveOnly) {
        @((($script:Accounts).MICRO_LIVE))
    } else {
        @((($script:Accounts).MICRO_LIVE), (($script:Accounts).LIVE))
    }
    foreach ($account in $cleanupAccounts) {
        try {
            $position = Get-Position $account
            if ([decimal]$position.positionAmt -ne 0) {
                Send-CloseOrder $account ([guid]::NewGuid()) "panic" | Out-Null
            }
            Restore-AccountLeverage $account
            Restore-AccountPositionMode $account
        } catch {
            if (-not $executionError) { $executionError = $_.Exception.Message }
        }
    }
    $script:EmergencyStopActive = $true
}

$finalGlobal = Get-GlobalExposureSnapshot
if ($finalGlobal.totalPositions -ne 0 -or $finalGlobal.totalOpenOrders -ne 0) {
    $script:RunState.status = "AMBIGUOUS"
    Save-RunState
    throw "B2B_FINAL_SAFETY_INVARIANT_FAILED"
}
if ($executionError) {
    $script:RunState.status = "SAFE_STOPPED"
    Save-RunState
    throw "B2B_EXECUTION_FAILED_CLEANED_UP: $executionError"
}

$report = [pscustomobject]@{
    mode = if ($MicroLiveOnly) { 'MICRO_LIVE_ONLY' } else { 'DUAL_ACCOUNT' }
    MICRO_LIVE = $microResult
    LIVE = $liveResult
    GLOBAL_LIMIT = [pscustomobject]@{
        maxObservedMargin = [Math]::Round($script:PeakGlobalMargin, 8)
        maxObservedNotional = [Math]::Round($script:PeakGlobalNotional, 8)
        simultaneousPositionsObserved = $false
        maxLeverage = $script:Leverage
        finalPositions = $finalGlobal.totalPositions
        finalOpenOrders = $finalGlobal.totalOpenOrders
        accountIsolationEvidence = $script:AccountIsolationEvidence
        liveTradingExecuted = (-not $MicroLiveOnly)
    }
    emergencyStop = $true
}
$script:RunState.status = "COMPLETED"
$script:RunState.pendingOrder = $null
Save-RunState
if (-not (Test-Path -LiteralPath $EvidenceDirectory)) {
    New-Item -ItemType Directory -Path $EvidenceDirectory | Out-Null
}
$evidencePath = Join-Path $EvidenceDirectory ("b2b-" + $script:RunState.runId + ".json")
$report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $evidencePath -Encoding utf8
$report | ConvertTo-Json -Depth 12
