package com.apunto.engine.repository;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface UserCopyAllocationRepository extends JpaRepository<UserCopyAllocationEntity, Long> {

    List<UserCopyAllocationEntity> findAllByStatus(UserCopyAllocationEntity.Status status);
    List<UserCopyAllocationEntity> findAllByIdUser(UUID idUser);
    List<UserCopyAllocationEntity> findAllByLiveCertificationIdAndExecutionMode(
            UUID liveCertificationId, String executionMode);

    @Query(value = """
            select *
            from futuros_operaciones.user_copy_allocation
            where execution_mode = 'LIVE'
              and status = 'paused'
              and is_active = true
              and ends_at is null
              and live_certification_id is not null
              and status_reason in (
                  'LIVE_ADOPTION_VALIDATION_REQUIRED',
                  'LIVE_RECERTIFICATION_USER_REVALIDATION_REQUIRED'
              )
            order by updated_at, id
            limit :limit
            """, nativeQuery = true)
    List<UserCopyAllocationEntity> findPendingLiveAdoptionReconciliation(@Param("limit") int limit);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update UserCopyAllocationEntity u
            set u.status = com.apunto.engine.entity.UserCopyAllocationEntity.Status.EXIT_ONLY,
                u.statusReason = :reason,
                u.statusUpdatedAt = :now,
                u.updatedAt = :now
            where u.liveCertificationId = :certificationId
              and u.executionMode = 'LIVE'
              and u.endsAt is null
              and u.isActive = true
            """)
    int markCertificationAllocationsExitOnly(@Param("certificationId") UUID certificationId,
                                             @Param("reason") String reason,
                                             @Param("now") java.time.OffsetDateTime now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update UserCopyAllocationEntity u
            set u.status = com.apunto.engine.entity.UserCopyAllocationEntity.Status.PAUSED,
                u.statusReason = :reason,
                u.statusUpdatedAt = :now,
                u.updatedAt = :now
            where u.liveCertificationId = :certificationId
              and u.executionMode = 'LIVE'
              and u.endsAt is null
              and u.isActive = true
            """)
    int markCertificationAllocationsPendingRevalidation(@Param("certificationId") UUID certificationId,
                                                        @Param("reason") String reason,
                                                        @Param("now") java.time.OffsetDateTime now);

    @Query(value = """
            select *
            from futuros_operaciones.user_copy_allocation uca
            where uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) = 'active'
              and coalesce(uca.execution_mode, 'LIVE') = 'MICRO_LIVE'
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
              and (
                  :executionMode = 'MICRO_LIVE'
                  or (:executionMode = 'LIVE' and coalesce(uca.allocation_pct, 0) > 0)
              )
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
            select count(*)
            from futuros_operaciones.user_copy_allocation uca
            where uca.exchange_account_id = :exchangeAccountId
              and uca.execution_mode = 'MICRO_LIVE'
              and uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) <> 'closed'
            """, nativeQuery = true)
    long countOccupiedMicroLiveSlots(@Param("exchangeAccountId") UUID exchangeAccountId);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            update futuros_operaciones.user_copy_allocation candidate
            set status = 'closed',
                is_active = false,
                ends_at = :now,
                status_reason = 'MICRO_LIVE_PREEMPTED_IDLE_FOR_RECERTIFICATION',
                status_updated_at = :now,
                updated_at = :now
            where candidate.id = (
                select allocation.id
                from futuros_operaciones.user_copy_allocation allocation
                where allocation.exchange_account_id = :exchangeAccountId
                  and allocation.execution_mode = 'MICRO_LIVE'
                  and allocation.ends_at is null
                  and allocation.is_active
                  and lower(allocation.status) = 'active'
                  and coalesce(allocation.status_reason, '') not like '%RECERTIFICATION%'
                  and not exists (
                      select 1 from futuros_operaciones.copy_operation operation
                      where operation.user_copy_allocation_id = allocation.id and operation.is_active
                  )
                  and not exists (
                      select 1 from futuros_operaciones.copy_dispatch_intent intent
                      where intent.user_copy_allocation_id = allocation.id
                        and intent.status in ('CREATED','CLAIMED','DISPATCHING','ACKNOWLEDGED','NEW',
                                              'PARTIALLY_FILLED','FILLED','RECONCILING','PERSISTENCE_PENDING')
                  )
                  and not exists (
                      select 1 from futuros_operaciones.copy_position_ownership ownership
                      where ownership.user_copy_allocation_id = allocation.id
                        and ownership.ownership_status <> 'CLOSED'
                  )
                  and not exists (
                      select 1 from futuros_operaciones.copy_flip_saga saga
                      where saga.user_copy_allocation_id = allocation.id
                        and saga.saga_status not in ('COMPLETED','COMPLETED_FLAT_NEW_SKIPPED','FAILED')
                  )
                order by allocation.updated_at, allocation.id
                limit 1
                for update skip locked
            )
            """, nativeQuery = true)
    int releaseOneIdleMicroAllocationForRecertification(
            @Param("exchangeAccountId") UUID exchangeAccountId,
            @Param("now") java.time.OffsetDateTime now);

    @Query(value = """
            select count(distinct uca.id_user)
            from futuros_operaciones.user_copy_allocation uca
            join futuros_operaciones.users u on u.id = uca.id_user and u.activo = true
            join futuros_operaciones.detail_user du on du.id_users = u.id
            join futuros_operaciones.user_api_keys ak on ak.user_id = u.id
            where uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) = 'active'
              and coalesce(uca.execution_mode, 'LIVE') = :executionMode
              and (
                  :executionMode = 'MICRO_LIVE'
                  or (:executionMode = 'LIVE' and coalesce(uca.allocation_pct, 0) > 0)
              )
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
              and lower(status) in (
                  'active', 'exit_only', 'paused_by_negative_pnl',
                  'paused_by_stale_metric', 'paused_by_risk'
              )
              and COALESCE(execution_mode, 'LIVE') in ('LIVE', 'MICRO_LIVE')
            """, nativeQuery = true)
    List<UserCopyAllocationEntity> findActiveByWalletId(@Param("walletId") String walletId);

    @Query(value = """
            select *
            from futuros_operaciones.user_copy_allocation uca
            where uca.ends_at is null
              and uca.is_active = true
              and lower(uca.status) in (
                  'active', 'exit_only', 'paused_by_negative_pnl',
                  'paused_by_stale_metric', 'paused_by_risk'
              )
              and coalesce(uca.execution_mode, 'LIVE') in ('LIVE', 'MICRO_LIVE')
              and (
                  coalesce(uca.execution_mode, 'LIVE') = 'MICRO_LIVE'
                  or coalesce(uca.allocation_pct, 0) > 0
              )
            """, nativeQuery = true)
    List<UserCopyAllocationEntity> findAllActiveRuntimeAllocations();

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
              and uca.executionMode = 'LIVE'
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
              and uca.executionMode = 'LIVE'
            """)
    Optional<UserCopyAllocationEntity> findOpenAllocationForUserWalletStrategyScope(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("strategyCode") String strategyCode,
            @Param("scopeType") String scopeType,
            @Param("scopeValue") String scopeValue
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
              and uca.executionMode = :executionMode
            """)
    Optional<UserCopyAllocationEntity> findOpenAllocationForUserWalletStrategyScopeAndMode(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("strategyCode") String strategyCode,
            @Param("scopeType") String scopeType,
            @Param("scopeValue") String scopeValue,
            @Param("executionMode") String executionMode
    );

    default Optional<UserCopyAllocationEntity> findActiveAllocation(
            UUID idUser,
            String walletId,
            UserCopyAllocationEntity.Status status
    ) {
        return findActiveAllocationsForUserWallet(idUser, walletId, status).stream().findFirst();
    }

}
