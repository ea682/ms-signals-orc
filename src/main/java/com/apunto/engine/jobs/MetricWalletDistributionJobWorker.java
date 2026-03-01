package com.apunto.engine.jobs;

import com.apunto.engine.service.MetricWalletService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(
        name = "engine.metric-wallet.distribution-worker.enabled",
        havingValue = "true"
)
public class MetricWalletDistributionJobWorker {

    private final MetricWalletService metricWalletService;

    @Scheduled(fixedDelayString = "${metric-wallet.distribution.refresh-job:5m}")
    public void refreshDistribution() {
        try {
            metricWalletService.getMetricWallets();
            log.info("event=metric_wallets.distribution_refreshed maxWallets=50");
        } catch (Exception e) {
            log.warn("event=metric_wallets.distribution_refresh_failed err={}", e.toString(), e);
        }
    }
}
