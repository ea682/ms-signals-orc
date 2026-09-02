package com.apunto.copytarget;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CopyPolicyV1SizingStepRunnerTest {

    private static final Instant NOW = Instant.parse("2026-08-28T12:00:00Z");
    private final CopyPolicyV1SizingStepRunner runner = new CopyPolicyV1SizingStepRunner();

    @Test
    void historicalStepUsesEightyOneDollarEnvelopeAndFixedFiveX() {
        CopyPolicyV1SizingStepResult result = runner.run(step(bd("100"), bd("5")));

        assertEquals(0, bd("100").compareTo(result.defensiveEquityUsd()));
        assertEquals(0, bd("81").compareTo(result.authorizedSizingCapitalUsd()));
        assertEquals(0, bd("4.05").compareTo(result.portfolio().totalTargetMarginUsd()));
        assertEquals(0, bd("20.25").compareTo(result.portfolio().totalTargetNotionalUsd()));
    }

    @Test
    void gainsDoNotCompoundButDrawdownReducesTheNextStep() {
        CopyPolicyV1SizingStepResult gain = runner.run(step(bd("150"), bd("5")));
        CopyPolicyV1SizingStepResult drawdown = runner.run(step(bd("80"), bd("5")));

        assertEquals(0, bd("81").compareTo(gain.authorizedSizingCapitalUsd()));
        assertEquals(0, bd("64.8").compareTo(drawdown.authorizedSizingCapitalUsd()));
        assertEquals(0, bd("3.24").compareTo(drawdown.portfolio().totalTargetMarginUsd()));
    }

    @Test
    void sameStepProducesSameInputDigestAndLogicalResult() {
        CopyPolicyV1SizingStepResult first = runner.run(step(bd("80"), bd("5")));
        CopyPolicyV1SizingStepResult second = runner.run(step(bd("80.0"), bd("5.00")));

        assertEquals(first.inputDigest(), second.inputDigest());
        assertEquals(first.portfolio(), second.portfolio());
    }

    @Test
    void historicalCannotDriftFromFixedFiveX() {
        assertThrows(IllegalArgumentException.class,
                () -> runner.run(step(bd("100"), bd("4"))));
    }

    private CopyPolicyV1SizingStepRequest step(BigDecimal mtmEquity, BigDecimal leverage) {
        SourcePosition source = new SourcePosition(
                "leg-1", "BTC", "BTCUSDT", SourceSide.LONG,
                bd("0.01"), bd("500"), bd("50"), bd("50000"), bd("50000"), bd("10"),
                42L, bd("100"));
        TargetPortfolioRequest sizingInput = TargetPortfolioRequest.builder()
                .calculatedAt(NOW)
                .sourceAccountEquityUsd(bd("1000"))
                .equityObservedAt(NOW.minusSeconds(1))
                .equitySource("CERTIFIED_ETL")
                .maximumEquityAge(Duration.ofSeconds(30))
                .sourceSnapshotVersion(42L)
                .sourcePositions(List.of(source))
                .targetAllocatedCapitalUsd(bd("999"))
                .targetLeverage(leverage)
                .availableMarginUsd(bd("999"))
                .usedMarginUsd(BigDecimal.ZERO)
                .reservedMarginUsd(BigDecimal.ZERO)
                .filters(List.of(new BinanceSymbolFilter(
                        "BTCUSDT", true, "USDT", bd("0.00000001"), bd("1000"), bd("0.00000001"),
                        bd("1"), bd("0.01"), bd("20"), bd("100"))))
                .quoteAsset("USDT")
                .versions(new CalculationVersions("strategy-v1", "copy-policy-v1", "binance-rules-v1"))
                .build();
        return new CopyPolicyV1SizingStepRequest(
                "wallet-1", "MOVEMENT_ALL", CopyPolicyWorld.CORE,
                CopyExecutionMode.HISTORICAL, bd("100"), mtmEquity, sizingInput);
    }

    private static BigDecimal bd(String value) {
        return new BigDecimal(value);
    }
}
