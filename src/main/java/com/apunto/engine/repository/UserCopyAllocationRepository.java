package com.apunto.engine.repository;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface UserCopyAllocationRepository extends JpaRepository<UserCopyAllocationEntity, Long> {

    Optional<UserCopyAllocationEntity> findByMaxWalletAndWalletId(Integer maxWallet, String walletId);

    List<UserCopyAllocationEntity> findAllByMaxWalletAndEndsAtIsNull(Integer maxWallet);
}
