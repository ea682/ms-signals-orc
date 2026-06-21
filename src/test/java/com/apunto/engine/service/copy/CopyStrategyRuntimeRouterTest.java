package com.apunto.engine.service.copy;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyStrategyRuntimeRouterTest {

    private final CopyStrategyRuntimeRouter router = new CopyStrategyRuntimeRouter();

    @Test
    void realAdvancedProfilesAreLiveEligibleButRecentWindowsAreNot() {
        assertTrue(router.isLiveEligibleJoyasCandidate(metric("TOP_SYMBOLS_ONLY")));
        assertTrue(router.isLiveEligibleJoyasCandidate(metric("MAJORS_ONLY")));
        assertTrue(router.isLiveEligibleJoyasCandidate(metric("LOW_LEVERAGE_ONLY")));

        assertFalse(router.isLiveEligibleJoyasCandidate(metric("RECENT_7D")));
        assertFalse(router.isShadowEligibleJoyasCandidate(metric("RECENT_14D")));
        assertFalse(router.isLiveEligibleJoyasCandidate(metric("ROBUST_EX_TOP_1")));
        assertFalse(router.isLiveEligibleJoyasCandidate(metric("PARTIAL_REDUCE")));
    }

    @Test
    void majorsAndQualityProfilesFilterSymbolsAtRuntime() {
        UserCopyAllocationEntity majors = allocation("MAJORS_ONLY", "MAJORS");
        UserCopyAllocationEntity quality = allocation("HIGH_QUALITY_SYMBOLS_ONLY", "HIGH_QUALITY");

        assertTrue(router.allocationAppliesToEvent(majors, CopyJobAction.OPEN, HyperliquidDeltaType.OPEN, "LONG", "BTCUSDT"));
        assertFalse(router.allocationAppliesToEvent(majors, CopyJobAction.OPEN, HyperliquidDeltaType.OPEN, "LONG", "FARTCOINUSDT"));

        assertTrue(router.allocationAppliesToEvent(quality, CopyJobAction.OPEN, HyperliquidDeltaType.OPEN, "LONG", "BTCUSDT"));
        assertFalse(router.allocationAppliesToEvent(quality, CopyJobAction.OPEN, HyperliquidDeltaType.OPEN, "LONG", "FARTCOINUSDT"));
    }

    @Test
    void scoringWindowsNeverApplyToNewLiveOpenEvents() {
        UserCopyAllocationEntity recent = allocation("RECENT_30D", "30D");

        assertFalse(router.allocationAppliesToEvent(recent, CopyJobAction.OPEN, HyperliquidDeltaType.OPEN, "LONG", "BTCUSDT"));
        assertTrue(router.allocationAppliesToEvent(recent, CopyJobAction.CLOSE, HyperliquidDeltaType.CLOSE, "LONG", "BTCUSDT"));
    }

    private static MetricaWalletDto metric(String strategyCode) {
        return MetricaWalletDto.builder()
                .wallet(MetricaWalletDto.WalletDto.builder().idWallet("0xabc").build())
                .strategy(MetricaWalletDto.StrategyDto.builder().strategyCode(strategyCode).build())
                .build();
    }

    private static UserCopyAllocationEntity allocation(String strategyCode, String scopeValue) {
        return UserCopyAllocationEntity.builder()
                .walletId("0xabc")
                .copyStrategyCode(strategyCode)
                .scopeType("strategy")
                .scopeValue(scopeValue)
                .executionMode("LIVE")
                .isActive(true)
                .build();
    }
}
