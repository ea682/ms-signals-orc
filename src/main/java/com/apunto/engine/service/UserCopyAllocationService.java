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

    default void syncDistribution(List<MetricaWalletDto> liveCandidates, List<MetricaWalletDto> shadowCandidates) {
        syncDistribution(liveCandidates);
    }

    List<UserCopyAllocationEntity> getWalletUserId(UUID idUser);

    Set<UUID> getActiveUserIdsByWallet(String walletId);

    List<UserCopyAllocationEntity> getActiveAllocationsByWallet(String walletId);

    default List<UserCopyAllocationEntity> getActiveAllocationsByWalletCachedOnly(String walletId) {
        return List.of();
    }

    List<UserCopyAllocationEntity> getActiveAllocationsForUserWallet(UUID idUser, String walletId);

    default List<UserCopyAllocationEntity> getActiveAllocationsForUserWalletCachedOnly(UUID idUser, String walletId) {
        if (idUser == null) {
            return List.of();
        }
        return getActiveAllocationsByWalletCachedOnly(walletId).stream()
                .filter(java.util.Objects::nonNull)
                .filter(allocation -> idUser.equals(allocation.getIdUser()))
                .toList();
    }

    Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId);

    Optional<UserCopyAllocationEntity> findActiveAllocation(UUID idUser, String walletId, String strategyCode);

    Optional<UserCopyAllocationEntity> findOpenAllocation(UUID idUser, String walletId, String strategyCode);

    default Optional<UserCopyAllocationEntity> findOpenAllocation(UUID idUser, String walletId, String strategyCode, String scopeType, String scopeValue) {
        return findOpenAllocation(idUser, walletId, strategyCode);
    }

    void markGuardBlocked(UUID idUser, String walletId, String strategyCode, String targetStatus, String reason, OffsetDateTime cooldownUntil);
}
