package com.apunto.engine.repository;

import com.apunto.engine.entity.UserWalletCopyPlanEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface UserWalletCopyPlanRepository extends JpaRepository<UserWalletCopyPlanEntity, Long> {

    Optional<UserWalletCopyPlanEntity> findByIdUserAndWalletLc(UUID idUser, String walletLc);
}
