package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyWalletProfileEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CopyWalletProfileRepository extends JpaRepository<CopyWalletProfileEntity, Long> {

    Optional<CopyWalletProfileEntity> findByProfileKey(String profileKey);
}
