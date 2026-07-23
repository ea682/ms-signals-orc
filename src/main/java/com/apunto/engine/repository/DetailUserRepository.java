package com.apunto.engine.repository;

import com.apunto.engine.entity.DetailUserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

public interface DetailUserRepository extends JpaRepository<DetailUserEntity, UUID> {

    DetailUserEntity findByUser_Id_AndUserActive(UUID user_id, boolean userActive);

    DetailUserEntity findByUser_Id(UUID userId);

    @Query("""
            select d
            from DetailUserEntity d
            join fetch d.user u
            where u.activo = true
              and d.userActive = true
              and d.apiKeyBinar = true
              and d.autoFollowCertifiedLive = true
              and coalesce(d.capital, 0) > 0
              and coalesce(d.maxWallet, 0) > 0
            """)
    List<DetailUserEntity> findEligibleAutoFollowCertifiedLiveUsers();

    @Query("""
            select d
            from DetailUserEntity d
            join fetch d.user u
            where u.activo = true
              and d.userActive = true
              and d.apiKeyBinar = true
              and d.participateInMicroLive = true
              and coalesce(d.capital, 0) >= 100
              and coalesce(d.maxWallet, 0) > 0
            """)
    List<DetailUserEntity> findEligibleMicroLiveUsers();

    @Query("""
            select count(distinct d.user.id)
            from DetailUserEntity d
            where d.userActive = true
            """)
    long countActiveDetailUsers();

    @Query("""
            select count(distinct d.user.id)
            from DetailUserEntity d
            where d.userActive = true
              and d.apiKeyBinar = true
            """)
    long countActiveBinanceEnabledUsers();

    @Query("""
            select count(distinct d.user.id)
            from DetailUserEntity d
            where d.userActive = true
              and d.apiKeyBinar = true
              and coalesce(d.capital, 0) > 0
            """)
    long countActiveBinanceUsersWithCapital();

    @Query("""
            select count(distinct d.user.id)
            from DetailUserEntity d
            where d.userActive = true
              and d.apiKeyBinar = true
              and coalesce(d.capital, 0) > 0
              and coalesce(d.maxWallet, 0) > 0
            """)
    long countActiveBinanceUsersWithCapitalAndMaxWallet();

    @Transactional
    @Modifying(clearAutomatically = false, flushAutomatically = true)
    @Query("update DetailUserEntity d set d.capital = :capital where d.id = :detailId")
    int updateCapitalById(@Param("detailId") UUID detailId, @Param("capital") Integer capital);
}
