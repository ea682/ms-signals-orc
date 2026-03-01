package com.apunto.engine.service;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;

import java.util.List;
import java.util.UUID;

public interface UserCopyAllocationService {

    void syncDistribution(List<MetricaWalletDto> candidates);

    List<UserCopyAllocationEntity> getWalletUserId(UUID idUser);



}
