package com.apunto.engine.service;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.time.OffsetDateTime;
import java.util.UUID;

public interface UserCopyAllocationService {

    void syncDistribution(List<MetricaWalletDto> candidates);

    List<UserCopyAllocationEntity> getWalletUserId(UUID idUser);

    Set<UUID> getActiveUserIdsByWallet(String walletId);

    List<UserCopyAllocationEntity> getActiveAllocationsByWallet(String walletId);

    Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId);

    Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId, String strategyCode);

    Optional<UserCopyAllocationEntity> findOpenAllocation(UUID idUser, String walletId, String strategyCode);

    void markGuardBlocked(UUID idUser, String walletId, String strategyCode, String targetStatus, String reason, OffsetDateTime cooldownUntil);
}
