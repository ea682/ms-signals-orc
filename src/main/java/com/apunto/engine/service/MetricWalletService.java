package com.apunto.engine.service;

import com.apunto.engine.dto.client.MetricaWalletDto;

import java.util.List;

public interface MetricWalletService {
    List<MetricaWalletDto> getMetricWallets(int maxWallets);
}
