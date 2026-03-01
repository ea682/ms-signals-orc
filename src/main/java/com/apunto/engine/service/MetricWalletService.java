package com.apunto.engine.service;

import com.apunto.engine.dto.client.MetricaWalletDto;

import java.util.List;
import java.util.UUID;

public interface MetricWalletService {
    List<MetricaWalletDto> getMetricWallets();
    List<MetricaWalletDto> getCandidatesUser(UUID idUser);
}
