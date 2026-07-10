package com.apunto.engine.service.impl;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LiveBinanceExecutionSafetyTest {

    @Test
    void defaultApplicationKeepsLiveExecutionBehindExplicitOptIn() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yml"));
        Properties properties = yaml.getObject();

        assertNotNull(properties);
        assertEquals("${BINANCE_ORDER_SUBMIT_ENABLED:false}", properties.getProperty("binance.order-submit-enabled"));
        assertEquals("${COPY_LIVE_ENABLED:false}", properties.getProperty("copy.live.enabled"));
        assertEquals("${COPY_LIVE_CANARY_ENABLED:false}", properties.getProperty("copy.live.canary-enabled"));
        assertEquals("${COPY_LIVE_DRY_RUN:true}", properties.getProperty("copy.live.dry-run"));
        assertEquals("${COPY_NEW_DISPATCH_ENABLED:false}", properties.getProperty("copy.new-dispatch.enabled"));
        assertEquals("${COPY_MICRO_LIVE_ENABLED:false}", properties.getProperty("copy.micro-live.enabled"));
        assertEquals("${COPY_LIVE_WHITELIST_USER_IDS:}", properties.getProperty("copy.live.whitelist.user-ids"));
        assertEquals("${COPY_LIVE_WHITELIST_WALLET_IDS:}", properties.getProperty("copy.live.whitelist.wallet-ids"));
        assertEquals("${COPY_LIVE_WHITELIST_SYMBOLS:}", properties.getProperty("copy.live.whitelist.symbols"));
        assertEquals("${COPY_LIVE_WHITELIST_ALLOCATION_IDS:}", properties.getProperty("copy.live.whitelist.allocation-ids"));
        assertEquals("${COPY_LIVE_WHITELIST_STRATEGY_CODES:}", properties.getProperty("copy.live.whitelist.strategy-codes"));
        assertEquals("${BINANCE_ORDER_READ_TIMEOUT_MS:8000}", properties.getProperty("rest-client.binance-service.read-ms"));
        assertEquals("${BINANCE_CLOSE_READ_TIMEOUT_MS:8000}", properties.getProperty("rest-client.binance-service.close-read-ms"));
        assertEquals("${METRIC_WALLET_ALLOCATION_DEFAULT_EXECUTION_MODE:SHADOW}", properties.getProperty("metric-wallet.allocation.default-execution-mode"));
    }

    @Test
    void blockedRealGateNeverFabricatesShadowFill() throws IOException {
        String source = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/service/impl/BinanceEngineServiceImpl.java"));
        String gate = Files.readString(Path.of(
                "src/main/java/com/apunto/engine/service/copy/dispatch/CopyRealExecutionGate.java"));

        assertTrue(source.contains("copyRealExecutionGate.evaluate"));
        assertTrue(gate.contains("COPY_NEW_DISPATCH_DISABLED"));
        assertTrue(source.contains("throw new SkipExecutionException"));
        assertTrue(!source.contains("copyImpact=mock_fill_no_real_binance"));
    }

    @Test
    void testProfileDisablesPrivateOrderSubmitAndRealBinanceUrls() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application-test.yml"));
        Properties properties = yaml.getObject();

        assertNotNull(properties);
        assertEquals("false", properties.getProperty("binance.order-submit-enabled"));
        assertEquals("mock", properties.getProperty("binance.client-mode"));
        assertEquals("true", properties.getProperty("binance.live-safe-validation-mode"));
        assertEquals("false", properties.getProperty("copy.live.enabled"));
        assertEquals("false", properties.getProperty("copy.live.canary-enabled"));
        assertEquals("true", properties.getProperty("copy.live.dry-run"));
        assertEquals("SHADOW", properties.getProperty("metric-wallet.allocation.default-execution-mode"));
        assertLoopbackDisabled(properties.getProperty("rest-client.binance-service.info-base"));
        assertLoopbackDisabled(properties.getProperty("hyperliquid.direct-ingest.origin-store.binance-price-base-url"));
        assertLoopbackDisabled(properties.getProperty("futures.capital-maintenance.bnb-price-base-url"));
    }

    private void assertLoopbackDisabled(String value) {
        assertNotNull(value);
        assertTrue(value.startsWith("http://127.0.0.1:9/"), value);
    }
}
