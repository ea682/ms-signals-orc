package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyFlipSagaEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface CopyFlipSagaRepository extends JpaRepository<CopyFlipSagaEntity, UUID> {

    Optional<CopyFlipSagaEntity> findByUserCopyAllocationIdAndNewSourcePositionCycleId(
            Long userCopyAllocationId, UUID newSourcePositionCycleId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select saga from CopyFlipSagaEntity saga where saga.id = :id")
    Optional<CopyFlipSagaEntity> findByIdForUpdate(@Param("id") UUID id);
}
