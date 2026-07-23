package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DualAccountB2bHarnessContractTest {

    private static final Path HARNESS = Path.of("scripts", "run-binance-micro-live-b2b.ps1");

    @Test
    void harnessLoadsAndExecutesMicroLiveThenLiveWithoutCredentialFallback() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("BINANCE_MICRO_LIVE_API_KEY"));
        assertTrue(script.contains("BINANCE_MICRO_LIVE_API_SECRET"));
        assertTrue(script.contains("BINANCE_LIVE_API_KEY"));
        assertTrue(script.contains("BINANCE_LIVE_API_SECRET"));
        assertTrue(script.contains("Invoke-AccountB2bFlow (($script:Accounts).MICRO_LIVE)"));
        assertTrue(script.contains("Invoke-AccountB2bFlow (($script:Accounts).LIVE)"));
        assertTrue(script.indexOf("Invoke-AccountB2bFlow (($script:Accounts).MICRO_LIVE)")
                < script.indexOf("Invoke-AccountB2bFlow (($script:Accounts).LIVE)"));
    }

    @Test
    void harnessReconcilesBothAccountsBeforeExposureAndPersistsAmbiguousState() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("function Get-GlobalExposureSnapshot"));
        assertTrue(script.contains("Get-GlobalExposureSnapshot"));
        assertTrue(script.contains("B2B_GLOBAL_MARGIN_LIMIT_EXCEEDED"));
        assertTrue(script.contains("B2B_GLOBAL_NOTIONAL_LIMIT_EXCEEDED"));
        assertTrue(script.contains("B2B_ANOTHER_ACCOUNT_HAS_OPEN_POSITION"));
        assertTrue(script.contains("B2B_EXECUTION_ACCOUNT_PURPOSE_MISMATCH"));
        assertTrue(script.contains("B2B_EXECUTION_ACCOUNTS_NOT_ISOLATED"));
        assertTrue(script.contains("Save-RunState"));
        assertTrue(script.contains("Resolve-PendingRunState"));
    }

    @Test
    void harnessCapturesCrossAccountSnapshotsAndRestoresLeverage() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("B2B_LIVE_ACCOUNT_CHANGED_DURING_MICRO_TEST"));
        assertTrue(script.contains("B2B_MICRO_ACCOUNT_CHANGED_DURING_LIVE_TEST"));
        assertTrue(script.contains("Assert-CounterpartUnchanged"));
        assertTrue(script.contains("Restore-AccountLeverage"));
        assertTrue(script.contains("finalOpenOrders"));
        assertTrue(script.contains("emergencyStop"));
    }

    @Test
    void databaseCredentialLookupKeepsEveryPsqlArgumentInOneInvocation() throws Exception {
        String script = Files.readString(HARNESS).replace("\r\n", "\n");

        assertTrue(script.contains("-h $uri.Host -p $uri.Port `\n"
                + "            -U (Require-Value $database \"DB_USER\") -d $databaseName `\n"
                + "            -v ON_ERROR_STOP=1"));
    }

    @Test
    void databaseCredentialTransportUsesSingleLineHexEncoding() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("encode(convert_to(api_key, 'UTF8'), 'hex')"));
        assertTrue(script.contains("encode(convert_to(api_secret, 'UTF8'), 'hex')"));
        assertTrue(script.contains("Convert-HexToUtf8"));
    }

    @Test
    void scriptScopedAccountContainerIsDereferencedBeforeSelectingPurpose() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("Get-AccountSnapshot (($script:Accounts).MICRO_LIVE)"));
        assertTrue(script.contains("Get-AccountSnapshot (($script:Accounts).LIVE)"));
    }

    @Test
    void accountSnapshotDoesNotShadowItsCredentialContextCaseInsensitively() throws Exception {
        String script = Files.readString(HARNESS);

        assertFalse(script.contains("$account = Invoke-BinanceSigned $Account"));
        assertTrue(script.contains("$accountDetails = Invoke-BinanceSigned $Account"));
    }

    @Test
    void eachAccountFlowVerifiesEmergencyStopBeforeTheNextAccountStarts() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("function Test-EmergencyStopPolicy"));
        assertTrue(script.contains("$emergencyStopEvidence = Test-EmergencyStopPolicy $Account"));
        assertTrue(script.contains("blockedIntents = @('OPEN', 'INCREASE')"));
        assertTrue(script.contains("allowedDeriskIntents = @('REDUCE', 'CLOSE')"));
        assertTrue(script.contains("sentToBinance = $false"));
        assertTrue(script.contains("emergencyStopEvidence = $emergencyStopEvidence"));
    }

    @Test
    void equalFuturesAliasesRequireSignedMasterToSubAccountProof() throws Exception {
        String script = Files.readString(HARNESS).replace("\r\n", "\n");

        assertTrue(script.contains("function Test-MasterSubAccountRelationship"));
        assertTrue(script.contains("/sapi/v1/sub-account/list"));
        assertTrue(script.contains("/sapi/v1/sub-account/subAccountApi/ipRestriction"));
        assertTrue(script.contains("subAccountApiKey = $Micro.ApiKey"));
        assertTrue(script.contains("proofType = 'BINANCE_MASTER_SUBACCOUNT_API_KEY'"));
        assertTrue(script.contains("if (-not $script:AccountIsolationEvidence.proven)"));
        assertFalse(script.contains("if ($initialGlobal.MICRO_LIVE.alias -eq $initialGlobal.LIVE.alias) {\n"
                + "    throw \"B2B_EXECUTION_ACCOUNTS_NOT_ISOLATED"));
    }

    @Test
    void microLiveOnlyModeNeverInvokesTheLiveTradingFlow() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("[switch]$MicroLiveOnly"));
        assertTrue(script.contains("if (-not $MicroLiveOnly)"));
        assertTrue(script.contains("Invoke-AccountB2bFlow (($script:Accounts).LIVE)"));
        assertTrue(script.contains("mode = if ($MicroLiveOnly) { 'MICRO_LIVE_ONLY' } else { 'DUAL_ACCOUNT' }"));
        assertTrue(script.contains("liveTradingExecuted = (-not $MicroLiveOnly)"));
    }

    @Test
    void postAckOrderLookupRetriesTransientBinanceMinus2013() throws Exception {
        String script = Files.readString(HARNESS).replace("\r\n", "\n");

        assertTrue(script.contains("function Wait-OrderFilled"));
        assertTrue(script.contains("if ($_.Exception.Message -notmatch 'code=-2013') { throw }"));
        assertTrue(script.contains("Start-Sleep -Milliseconds 250\n            continue"));
    }

    @Test
    void nonzeroPositionsFallbackToNotionalOverLeverageWhenBinanceReportsZeroMargin() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("$reportedMargin = [decimal]$marginProperty.Value"));
        assertTrue(script.contains("if ($reportedMargin -gt 0 -or $nonzero.Count -eq 0)"));
        assertTrue(script.contains("$fallback += [Math]::Abs([decimal]$position.positionAmt * "
                + "[decimal]$position.markPrice) / $lev"));
    }

    @Test
    void hedgeAccountIsTemporarilyNormalizedAndRestoredOnlyWhileFlat() throws Exception {
        String script = Files.readString(HARNESS);

        assertTrue(script.contains("OriginalPositionMode"));
        assertTrue(script.contains("function Set-B2bPositionMode"));
        assertTrue(script.contains("dualSidePosition = \"false\""));
        assertTrue(script.contains("function Restore-AccountPositionMode"));
        assertTrue(script.contains("dualSidePosition = \"true\""));
        assertTrue(script.contains("B2B_POSITION_MODE_CHANGE_REQUIRES_FLAT"));
        assertTrue(script.contains("if ($snapshot.positionMode -eq $Account.OriginalPositionMode) { return }"));
        assertTrue(script.contains("originalPositionModeRestored"));
        assertFalse(script.contains("if ($before.positionMode -ne \"ONE_WAY\") { throw \"BINANCE_POSITION_MODE_MISMATCH\" }"));
    }
}
