package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowCopyAllocationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ShadowCopyAllocationRepository extends JpaRepository<ShadowCopyAllocationEntity, Long> {

    @Query("""
            select s
            from ShadowCopyAllocationEntity s
            where s.idUser = :idUser
              and s.active = true
              and s.endsAt is null
            """)
    List<ShadowCopyAllocationEntity> findActiveByUser(@Param("idUser") UUID idUser);

    @Query("""
            select s
            from ShadowCopyAllocationEntity s
            where lower(s.walletId) = lower(:walletId)
              and s.active = true
              and s.endsAt is null
              and s.status in (
                    'SHADOW_TESTING',
                    'SHADOW_ACTIVE',
                    'SHADOW_WARNING',
                    'SHADOW_VALIDATED',
                    'SHADOW_ONLY',
                    'VALIDATED',
                    'LIVE_ELIGIBLE',
                    'PROMOTED_TO_LIVE',
                    'LIVE_ACTIVE'
              )
            """)
    List<ShadowCopyAllocationEntity> findRuntimeActiveByWallet(@Param("walletId") String walletId);

    @Query(value = """
            with ranked as (
                select s.id,
                       row_number() over (
                           partition by coalesce(
                               s.wallet_profile_id::text,
                               lower(s.wallet_id) || '|' || s.copy_strategy_code || '|' || s.scope_type || '|' || s.scope_value
                           )
                           order by
                               case when s.linked_live_allocation_id is null then 0 else 1 end,
                               s.id
                       ) as rn
                from futuros_operaciones.shadow_copy_allocation s
                where lower(s.wallet_id) = lower(:walletId)
                  and s.is_active = true
                  and s.ends_at is null
                  and s.status in (
                        'SHADOW_TESTING',
                        'SHADOW_ACTIVE',
                        'SHADOW_WARNING',
                        'SHADOW_VALIDATED',
                        'SHADOW_ONLY',
                        'VALIDATED',
                        'LIVE_ELIGIBLE',
                        'PROMOTED_TO_LIVE',
                        'LIVE_ACTIVE'
                  )
            )
            select s.*
            from futuros_operaciones.shadow_copy_allocation s
            join ranked r on r.id = s.id
            where r.rn = 1
            """, nativeQuery = true)
    List<ShadowCopyAllocationEntity> findRuntimeProfileRepresentativesByWallet(@Param("walletId") String walletId);

    @Query("""
            select s
            from ShadowCopyAllocationEntity s
            where s.idUser = :idUser
              and lower(s.walletId) = lower(:walletId)
              and s.copyStrategyCode = :strategyCode
              and s.scopeType = :scopeType
              and s.scopeValue = :scopeValue
              and s.shadowVersion = :shadowVersion
              and s.active = true
              and s.endsAt is null
            """)
    Optional<ShadowCopyAllocationEntity> findActiveStrategy(
            @Param("idUser") UUID idUser,
            @Param("walletId") String walletId,
            @Param("strategyCode") String strategyCode,
            @Param("scopeType") String scopeType,
            @Param("scopeValue") String scopeValue,
            @Param("shadowVersion") int shadowVersion
    );
}
