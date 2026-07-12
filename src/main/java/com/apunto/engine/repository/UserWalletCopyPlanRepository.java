package com.apunto.engine.repository;

import com.apunto.engine.entity.UserWalletCopyPlanEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import jakarta.persistence.LockModeType;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface UserWalletCopyPlanRepository extends JpaRepository<UserWalletCopyPlanEntity, Long> {

    Optional<UserWalletCopyPlanEntity> findByIdUserAndWalletLc(UUID idUser, String walletLc);

    @Modifying(flushAutomatically = true)
    @Query(value = """
            insert into futuros_operaciones.user_wallet_copy_plan(
                id_user, wallet_lc, allocation_pct, allocated_capital_usd,
                sizing_mode, allocation_pct_source, created_at, updated_at
            ) values (
                :idUser, :walletLc, null, 100,
                'FIXED_CAPITAL', 'FIXED_MICRO_BUDGET', :now, :now
            )
            on conflict (id_user, wallet_lc) do nothing
            """, nativeQuery = true)
    int ensureFixedBudgetPlan(
            @Param("idUser") UUID idUser,
            @Param("walletLc") String walletLc,
            @Param("now") java.time.OffsetDateTime now
    );

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select plan
            from UserWalletCopyPlanEntity plan
            where plan.idUser = :idUser and plan.walletLc = :walletLc
            """)
    Optional<UserWalletCopyPlanEntity> findForUpdate(
            @Param("idUser") UUID idUser,
            @Param("walletLc") String walletLc
    );
}
