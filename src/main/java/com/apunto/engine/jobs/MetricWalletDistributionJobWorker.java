package com.apunto.engine.jobs;

import com.apunto.engine.service.MetricWalletService;
import com.apunto.engine.shared.exception.EngineException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

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
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException e) {
            log.warn("event=metric_wallets.distribution_refresh_failed errClass={} errMsg=\"{}\"",
                    e.getClass().getSimpleName(), safeLog(e.getMessage()));
        }
    }

    private String safeLog(String value) {
        if (value == null) return "";
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }
}
