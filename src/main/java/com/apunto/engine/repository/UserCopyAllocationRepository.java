package com.apunto.engine.repository;

import com.apunto.engine.entity.UserCopyAllocationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface UserCopyAllocationRepository extends JpaRepository<UserCopyAllocationEntity, Long> {

    List<UserCopyAllocationEntity> findAllByStatus(UserCopyAllocationEntity.Status status);
    List<UserCopyAllocationEntity> findAllByIdUser(UUID idUser);
    List<UserCopyAllocationEntity> findAllByIdUserAndWalletIdIn(UUID idUser, List<String> walletIds);
    List<UserCopyAllocationEntity> findAllByIdUserAndEndsAtIsNull(UUID idUser);

    @Query(value = """
            select *
            from user_copy_allocation
            where lower(wallet_id) = lower(:walletId)
              and ends_at is null
              and is_active = true
              and lower(status) = 'active'
            """, nativeQuery = true)
    List<UserCopyAllocationEntity> findActiveByWalletId(@Param("walletId") String walletId);

}
