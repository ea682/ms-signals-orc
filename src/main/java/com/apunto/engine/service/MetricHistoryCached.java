package com.apunto.engine.service;

import com.apunto.engine.client.MetricWalletsInfoClient;
import com.apunto.engine.dto.client.MetricaWalletDto;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class MetricHistoryCached {
    private final MetricWalletsInfoClient client;

    @Cacheable(cacheNames = "metricsAllPositionHistory", key = "#limit", sync = true)
    public List<MetricaWalletDto> allPositionHistory(int limit) {
        return client.allPositionHistory(limit);
    }
}
