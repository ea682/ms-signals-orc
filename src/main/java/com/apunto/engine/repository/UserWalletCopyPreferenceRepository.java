package com.apunto.engine.repository;

import com.apunto.engine.entity.UserWalletCopyPreferenceEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface UserWalletCopyPreferenceRepository
        extends JpaRepository<UserWalletCopyPreferenceEntity, UUID> {

    @Query("""
            select p
            from UserWalletCopyPreferenceEntity p
            where p.userId = :userId
              and lower(p.walletId) = lower(:walletId)
            """)
    Optional<UserWalletCopyPreferenceEntity> findByUserAndWallet(
            @Param("userId") UUID userId,
            @Param("walletId") String walletId);

    default boolean isBlocked(UUID userId, String walletId) {
        return findByUserAndWallet(userId, walletId)
                .map(UserWalletCopyPreferenceEntity::isWalletBlocked)
                .orElse(false);
    }
}

