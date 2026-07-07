package com.apunto.engine.repository;

import com.apunto.engine.entity.DetailUserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

public interface DetailUserRepository extends JpaRepository<DetailUserEntity, UUID> {

    DetailUserEntity findByUser_Id_AndUserActive(UUID user_id, boolean userActive);

    DetailUserEntity findByUser_Id(UUID userId);

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
