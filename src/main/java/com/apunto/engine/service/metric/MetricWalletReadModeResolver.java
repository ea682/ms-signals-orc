package com.apunto.engine.service.metric;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Locale;

@Component
@Slf4j
public class MetricWalletReadModeResolver {

    private final MetricWalletReadMode configuredMode;

    public MetricWalletReadModeResolver(
            @Value("${metric-wallet.read-mode:COMPARE}") String configuredMode
    ) {
        this.configuredMode = parse(configuredMode);
        log.info("event=metric_wallet_read_mode.config configuredMode={} effectiveMode={} fallbackEnabled=false",
                this.configuredMode, this.configuredMode);
    }

    public MetricWalletReadMode configuredMode() {
        return configuredMode;
    }

    public MetricWalletReadMode effectiveMode() {
        return configuredMode;
    }

    public boolean readsV2() {
        return configuredMode == MetricWalletReadMode.V2;
    }

    public boolean comparesV2() {
        return configuredMode == MetricWalletReadMode.COMPARE;
    }

    private static MetricWalletReadMode parse(String raw) {
        String value = raw == null ? "COMPARE" : raw.trim().toUpperCase(Locale.ROOT);
        if (value.isEmpty()) value = "COMPARE";
        try {
            return MetricWalletReadMode.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new IllegalStateException("INVALID_METRIC_WALLET_READ_MODE:" + raw, ex);
        }
    }
}
