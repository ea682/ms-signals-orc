package com.apunto.engine.service.metric;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.config.RestClientConfig;
import com.apunto.engine.dto.client.CopyDecisionDto;
import com.apunto.engine.dto.client.MetricStrategySnapshotDto;
import com.apunto.engine.service.copy.decision.CopyDecisionRequest;
import com.apunto.engine.service.copy.decision.MetricCopyDecisionGateway;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MetricV2HttpContractIntegrationTest {

    private static final String WINDOWS = "1d,3d,1w,2w,3w,1mo,2mo,3mo,6mo,9mo,1y,2y,all";

    @Test
    void realNestContractPublishesCoherentStrategyLevelSnapshotsAndFailsClosed() {
        String baseUrl = System.getProperty("metric.v2.http.base", "").trim();
        assumeTrue(!baseUrl.isEmpty(), "set -Dmetric.v2.http.base to run the real Nest contract test");

        MetricWalletsInfoClient client = client(baseUrl);
        MetricWalletReadModeResolver resolver = new MetricWalletReadModeResolver("V2");
        MetricV2SnapshotStore store = new MetricV2SnapshotStore(
                client,
                new MetricStrategyShadowProjectionMapper(),
                resolver,
                new SimpleMeterRegistry(),
                100,
                100,
                30,
                Duration.ofMinutes(10),
                Duration.ofMinutes(2),
                Duration.ofHours(2),
                false,
                true,
                WINDOWS
        );

        store.refreshNow();
        MetricV2SnapshotStore.Snapshot snapshot = store.snapshot();

        assertEquals(5, snapshot.summaryByKey().size());
        assertEquals(5, snapshot.fullByKey().size());
        assertEquals(5, snapshot.guardByKey().size());
        assertEquals(snapshot.fullByKey().keySet(), snapshot.guardByKey().keySet());
        assertTrue(snapshot.summaryByKey().keySet().containsAll(snapshot.fullByKey().keySet()));
        assertEquals(5, snapshot.summaryByKey().keySet().stream().distinct().count());
        assertTrue(snapshot.summaryByKey().values().stream().allMatch(item ->
                Integer.valueOf(2).equals(item.getMetricVersion())
                        && item.getEvaluationMode() == MetricStrategySnapshotDto.EvaluationMode.SUMMARY
                        && !item.isDecisionFinal()
                        && !item.isAllowNewEntries()
        ));
        assertTrue(snapshot.fullByKey().values().stream().allMatch(item ->
                item.getEvaluationMode() == MetricStrategySnapshotDto.EvaluationMode.FULL
                        && item.contractErrors().isEmpty()
        ));
        assertTrue(snapshot.guardByKey().values().stream().allMatch(item ->
                item.getWindows() != null
                        && item.getWindows().keySet().containsAll(Set.of(WINDOWS.split(",")))
        ));

        MetricStrategySnapshotDto exactUnit = snapshot.fullByKey().values().iterator().next();
        CopyDecisionDto decision = new MetricCopyDecisionGateway(client, resolver, store).getFullDecisionExact(
                new CopyDecisionRequest(
                        exactUnit.getWalletId(),
                        exactUnit.getStrategyCode(),
                        exactUnit.getScopeType(),
                        exactUnit.getScopeValue(),
                        "shadow-entry",
                        "full",
                        30,
                        90,
                        50_000,
                        55_000,
                        false
                )
        );

        assertNotNull(decision.getReasonCode());
        assertFalse(decision.isCanMicroLive());
        assertFalse(decision.isCanLive());
        assertFalse(decision.isAllowNewEntries());
        assertFalse(decision.isCanShadow(), "the isolated incomplete fixture must remain blocked");
    }

    private static MetricWalletsInfoClient client(String baseUrl) {
        RestClientConfig config = new RestClientConfig();
        ClientHttpRequestFactory requestFactory = config.clientHttpRequestFactory(1_000, 60_000);
        RestClient restClient = config.metricWalletRestClient(
                RestClient.builder(),
                requestFactory,
                baseUrl
        );
        return config.metricWalletsInfoClient(restClient);
    }
}
