package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyPositionOwnershipEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface CopyPositionOwnershipRepository extends JpaRepository<CopyPositionOwnershipEntity, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select ownership
            from CopyPositionOwnershipEntity ownership
            where ownership.exchangeAccountId = :exchangeAccountId
              and upper(ownership.symbol) = upper(:symbol)
              and ownership.ownershipStatus <> 'CLOSED'
            """)
    Optional<CopyPositionOwnershipEntity> findActiveForUpdate(
            @Param("exchangeAccountId") UUID exchangeAccountId,
            @Param("symbol") String symbol);
}
