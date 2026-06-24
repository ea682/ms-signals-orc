package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowCopyOperationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ShadowCopyOperationRepository extends JpaRepository<ShadowCopyOperationEntity, UUID> {

    Optional<ShadowCopyOperationEntity> findFirstByShadowAllocationIdAndIdOrderOriginAndTypeOperationAndActiveTrue(
            Long shadowAllocationId,
            String idOrderOrigin,
            String typeOperation
    );

    Optional<ShadowCopyOperationEntity> findFirstByWalletProfileIdAndIdOrderOriginAndTypeOperationAndActiveTrue(
            Long walletProfileId,
            String idOrderOrigin,
            String typeOperation
    );

    Optional<ShadowCopyOperationEntity> findFirstByShadowAllocationIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc(
            Long shadowAllocationId,
            String parsymbol,
            String typeOperation
    );

    Optional<ShadowCopyOperationEntity> findFirstByWalletProfileIdAndParsymbolAndTypeOperationAndActiveTrueOrderByDateCreationDesc(
            Long walletProfileId,
            String parsymbol,
            String typeOperation
    );
}
