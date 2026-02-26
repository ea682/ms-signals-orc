package com.apunto.engine.service;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;

import java.util.List;

public interface UserCopyAllocationService {

    void syncDistribution(int maxWallet, List<MetricaWalletDto> candidates);

    List<UserCopyAllocationEntity> getActiveDistribution(int maxWallet);

}
