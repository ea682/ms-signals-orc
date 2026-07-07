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

    @Query(value = """
            select *
            from futuros_operaciones.user_copy_allocation uca
            where uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) = 'active'
              and coalesce(uca.execution_mode, 'LIVE') = 'MICRO_LIVE'
              and coalesce(uca.allocation_pct, 0) > 0
            order by uca.promoted_from_shadow_at asc nulls last, uca.updated_at asc nulls last, uca.id asc
            limit :limit
            """, nativeQuery = true)
    List<UserCopyAllocationEntity> findMicroLivePromotionCandidates(@Param("limit") int limit);

    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and lower(uca.walletId) = lower(:walletId)
              and uca.copyStrategyCode = :strategyCode
              and uca.scopeType = :scopeType
              and uca.scopeValue = :scopeValue
              and uca.endsAt is null
              and uca.isActive = true
              and uca.executionMode = 'LIVE'
            """)
    Optional<UserCopyAllocationEntity> findOpenLiveAllocationForUserWalletStrategyScope(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("strategyCode") String strategyCode,
            @Param("scopeType") String scopeType,
            @Param("scopeValue") String scopeValue
    );

    @Query(value = """
            select count(*)
            from futuros_operaciones.user_copy_allocation uca
            where uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) = lower(:status)
              and coalesce(uca.execution_mode, 'LIVE') = :executionMode
              and coalesce(uca.allocation_pct, 0) > 0
            """, nativeQuery = true)
    long countActiveExecutableAllocationsByMode(
            @Param("executionMode") String executionMode,
            @Param("status") String status
    );

    @Query(value = """
            select count(*)
            from futuros_operaciones.user_copy_allocation uca
            where uca.id_user = :idUser
              and uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) = 'active'
              and coalesce(uca.execution_mode, 'LIVE') in ('LIVE', 'MICRO_LIVE')
            """, nativeQuery = true)
    long countActiveExecutionAllocationsByUser(@Param("idUser") UUID idUser);

    @Query(value = """
            select count(distinct uca.id_user)
            from futuros_operaciones.user_copy_allocation uca
            join futuros_operaciones.users u on u.id = uca.id_user and u.activo = true
            join futuros_operaciones.detail_user du on du.id_users = u.id
            join futuros_operaciones.user_api_keys ak on ak.user_id = u.id
            where uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) = 'active'
              and coalesce(uca.allocation_pct, 0) > 0
              and coalesce(uca.execution_mode, 'LIVE') = :executionMode
              and du.is_user_active = true
              and du.is_api_key_binance_active = true
              and coalesce(du.capital, 0) > 0
              and coalesce(du.max_wallet, 0) > 0
              and nullif(trim(ak.api_key), '') is not null
              and nullif(trim(ak.api_secret), '') is not null
            """, nativeQuery = true)
    long countEligibleExecutionUsersByMode(@Param("executionMode") String executionMode);

    @Query(value = """
            select count(distinct uca.id_user)
            from futuros_operaciones.user_copy_allocation uca
            join futuros_operaciones.users u on u.id = uca.id_user and u.activo = true
            join futuros_operaciones.detail_user du on du.id_users = u.id
            join futuros_operaciones.user_api_keys ak on ak.user_id = u.id
            where uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) = 'active'
              and coalesce(uca.allocation_pct, 0) > 0
              and coalesce(uca.execution_mode, 'LIVE') = 'LIVE'
              and uca.id_user in (:userIds)
              and du.is_user_active = true
              and du.is_api_key_binance_active = true
              and coalesce(du.capital, 0) > 0
              and coalesce(du.max_wallet, 0) > 0
              and nullif(trim(ak.api_key), '') is not null
              and nullif(trim(ak.api_secret), '') is not null
            """, nativeQuery = true)
    long countEligibleLiveExecutionUsersIn(@Param("userIds") List<UUID> userIds);

    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and lower(uca.walletId) in :walletIds
              and uca.executionMode in ('LIVE', 'MICRO_LIVE')
            """)
    List<UserCopyAllocationEntity> findAllByIdUserAndWalletIdIn(@Param("idUser") UUID idUser, @Param("walletIds") List<String> walletIds);


    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and lower(uca.walletId) = lower(:walletId)
              and uca.copyStrategyCode = :strategyCode
              and uca.endsAt is null
              and uca.isActive = true
              and uca.status = :status
              and uca.executionMode in ('LIVE', 'MICRO_LIVE')
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
              and uca.executionMode in ('LIVE', 'MICRO_LIVE')
            """)
    List<UserCopyAllocationEntity> findActiveByIdUser(@Param("idUser") UUID idUser);

    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and uca.endsAt is null
              and uca.executionMode in ('LIVE', 'MICRO_LIVE')
            """)
    List<UserCopyAllocationEntity> findAllByIdUserAndEndsAtIsNull(@Param("idUser") UUID idUser);

    @Query(value = """
            select *
            from futuros_operaciones.user_copy_allocation
            where lower(wallet_id) = lower(:walletId)
              and ends_at is null
              and is_active = true
              and lower(status) = 'active'
              and COALESCE(execution_mode, 'LIVE') in ('LIVE', 'MICRO_LIVE')
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
              and uca.executionMode in ('LIVE', 'MICRO_LIVE')
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
              and uca.executionMode in ('LIVE', 'MICRO_LIVE')
            """)
    Optional<UserCopyAllocationEntity> findOpenAllocationForUserWalletStrategy(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("strategyCode") String strategyCode
    );

    @Query("""
            select uca
            from UserCopyAllocationEntity uca
            where uca.idUser = :idUser
              and lower(uca.walletId) = lower(:walletId)
              and uca.copyStrategyCode = :strategyCode
              and uca.scopeType = :scopeType
              and uca.scopeValue = :scopeValue
              and uca.endsAt is null
              and uca.executionMode in ('LIVE', 'MICRO_LIVE')
            """)
    Optional<UserCopyAllocationEntity> findOpenAllocationForUserWalletStrategyScope(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("strategyCode") String strategyCode,
            @Param("scopeType") String scopeType,
            @Param("scopeValue") String scopeValue
    );

    default Optional<UserCopyAllocationEntity> findActiveAllocation(
            UUID idUser,
            String walletId,
            UserCopyAllocationEntity.Status status
    ) {
        return findActiveAllocationsForUserWallet(idUser, walletId, status).stream().findFirst();
    }

}
