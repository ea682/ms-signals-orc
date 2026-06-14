package com.apunto.engine.service;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.service.copy.CopyStrategyGuardDecision;

import java.util.List;
import java.util.UUID;

public interface MetricWalletService {
    List<MetricaWalletDto> getMetricWallets();
    List<MetricaWalletDto> getCandidatesUser(UUID idUser);

    /**
     * Runtime safety gate used by copy trading before copying a new source event.
     * It evaluates the latest metric snapshot for wallet + strategy and returns false
     * when recent simulation PnL windows are below the configured threshold.
     */
    boolean isCopyStrategyHealthyForCopy(String walletId, String strategyCode);

    CopyStrategyGuardDecision evaluateCopyStrategyForCopy(String walletId, String strategyCode);
}
