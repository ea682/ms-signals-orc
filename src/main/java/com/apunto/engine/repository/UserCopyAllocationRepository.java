package com.apunto.engine.repository;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UserCopyAllocationRepository extends JpaRepository<UserCopyAllocationEntity, Long> {

    List<UserCopyAllocationEntity> findAllByMaxWalletAndWalletIdIn(Integer maxWallet, List<String> walletIds);
    List<UserCopyAllocationEntity> findAllByMaxWalletAndEndsAtIsNull(Integer maxWallet);
}
