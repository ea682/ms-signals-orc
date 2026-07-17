package com.apunto.engine.service.copy.simulation;

import com.apunto.copytarget.LiquiditySimulationAssumptions;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;

@Data
@Component
@ConfigurationProperties(prefix = "copy.simulation.liquidity-worker")
public class CopyLiquiditySimulationProperties {
    private boolean enabled = false;
    private int batchSize = 1;
    private long fixedDelayMs = 5000L;
    private Duration staleLock = Duration.ofMinutes(10);
    private Duration retryDelay = Duration.ofMinutes(1);
    private int depthLimit = 100;
    private BigDecimal minimumCapitalUsd = new BigDecimal("5000");
    private BigDecimal maximumDepthConsumptionPct = new BigDecimal("0.30");
    private BigDecimal participationCapPct = new BigDecimal("0.05");
    private int fragmentCount = 10;
    private long intervalMillis = 1000L;
    private BigDecimal disappearingLiquidityPct = new BigDecimal("0.10");
    private BigDecimal adverseSelectionBps = new BigDecimal("2");
    private long networkLatencyMillis = 100L;
    private BigDecimal takerFeeBps = new BigDecimal("4");
    private BigDecimal fundingBpsPerEightHours = BigDecimal.ONE;

    public LiquiditySimulationAssumptions assumptions() {
        return new LiquiditySimulationAssumptions(
                maximumDepthConsumptionPct,
                participationCapPct,
                fragmentCount,
                intervalMillis,
                disappearingLiquidityPct,
                adverseSelectionBps,
                networkLatencyMillis,
                null,
                takerFeeBps,
                fundingBpsPerEightHours
        );
    }
}
