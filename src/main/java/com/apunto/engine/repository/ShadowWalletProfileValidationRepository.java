package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowWalletProfileValidationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ShadowWalletProfileValidationRepository extends JpaRepository<ShadowWalletProfileValidationEntity, Long> {

    Optional<ShadowWalletProfileValidationEntity> findFirstByWalletProfileIdOrderByStartedAtDesc(Long walletProfileId);
}
