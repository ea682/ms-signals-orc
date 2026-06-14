package com.apunto.engine.repository;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface UserCopyAllocationRepository extends JpaRepository<UserCopyAllocationEntity, Long> {

    List<UserCopyAllocationEntity> findAllByStatus(UserCopyAllocationEntity.Status status);
    List<UserCopyAllocationEntity> findAllByIdUser(UUID idUser);
    List<UserCopyAllocationEntity> findAllByIdUserAndWalletIdIn(UUID idUser, List<String> walletIds);


    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and lower(uca.walletId) = lower(:walletId)
              and uca.copyStrategyCode = :strategyCode
              and uca.endsAt is null
              and uca.isActive = true
              and uca.status = :status
            """)
    Optional<UserCopyAllocationEntity> findActiveAllocationForUserWalletStrategy(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("strategyCode") String strategyCode,
            @Param("status") UserCopyAllocationEntity.Status status
    );

    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and uca.endsAt is null
            """)
    List<UserCopyAllocationEntity> findActiveByIdUser(@Param("idUser") UUID idUser);
    List<UserCopyAllocationEntity> findAllByIdUserAndEndsAtIsNull(UUID idUser);

    @Query(value = """
            select *
            from futuros_operaciones.user_copy_allocation
            where lower(wallet_id) = lower(:walletId)
              and ends_at is null
              and is_active = true
              and lower(status) = 'active'
            """, nativeQuery = true)
    List<UserCopyAllocationEntity> findActiveByWalletId(@Param("walletId") String walletId);

    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and lower(uca.walletId) = lower(:walletId)
              and uca.endsAt is null
              and uca.isActive = true
              and uca.status = :status
            """)
    List<UserCopyAllocationEntity> findActiveAllocationsForUserWallet(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("status") UserCopyAllocationEntity.Status status
    );

    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and lower(uca.walletId) = lower(:walletId)
              and uca.copyStrategyCode = :strategyCode
              and uca.endsAt is null
            """)
    Optional<UserCopyAllocationEntity> findOpenAllocationForUserWalletStrategy(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("strategyCode") String strategyCode
    );

    default Optional<UserCopyAllocationEntity> findActiveAllocation(
            UUID idUser,
            String walletId,
            UserCopyAllocationEntity.Status status
    ) {
        return findActiveAllocationsForUserWallet(idUser, walletId, status).stream().findFirst();
    }

}
