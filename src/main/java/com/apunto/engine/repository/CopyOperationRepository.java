package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyOperationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CopyOperationRepository extends JpaRepository<CopyOperationEntity, UUID> {

    List<CopyOperationEntity> findAllByIdOrderOrigin(String idOrderOrigin);

    Optional<CopyOperationEntity> findFirstByIdOrderOriginAndIdUserAndActiveTrueOrderByDateCreationDesc(String idOrderOrigin, String idUser);

    Optional<CopyOperationEntity> findFirstByIdOrderOriginAndIdUserAndTypeOperationAndActiveTrueOrderByDateCreationDesc(String idOrderOrigin, String idUser, String typeOperation);

    Optional<CopyOperationEntity> findFirstByUserCopyAllocationIdAndIdOrderOriginAndTypeOperationAndActiveTrueOrderByDateCreationDesc(Long userCopyAllocationId, String idOrderOrigin, String typeOperation);

    Optional<CopyOperationEntity> findFirstByIdOrderOriginAndIdUserAndCopyStrategyCodeAndTypeOperationAndActiveTrueOrderByDateCreationDesc(String idOrderOrigin, String idUser, String copyStrategyCode, String typeOperation);

    Optional<CopyOperationEntity> findFirstByUserCopyAllocationIdAndIdOrderOriginAndTypeOperationOrderByDateCreationDesc(Long userCopyAllocationId, String idOrderOrigin, String typeOperation);

    Optional<CopyOperationEntity> findFirstByIdOrderOriginAndIdUserAndCopyStrategyCodeAndTypeOperationOrderByDateCreationDesc(String idOrderOrigin, String idUser, String copyStrategyCode, String typeOperation);

    boolean existsByIdOrderOriginAndIdUser(String idOrderOrigin, String idUser);

    Optional<CopyOperationEntity> findByIdOrden(String idOrden);

    List<CopyOperationEntity> findAllByIdOrden(String idOrden);

    List<CopyOperationEntity> findAllByIdUserAndIdWalletOriginAndActiveTrue(String idUser, String walletId);

    List<CopyOperationEntity> findAllByIdOrderOriginAndIdUserAndActiveTrue(String idOrderOrigin, String idUser);

    List<CopyOperationEntity> findAllByActiveTrue();

    @Query(value = """
            SELECT DISTINCT UPPER(c.parsymbol)
            FROM futuros_operaciones.copy_operation c
            WHERE c.id_user = :idUser
              AND c.parsymbol IS NOT NULL
              AND trim(c.parsymbol) <> ''
              AND COALESCE(c.execution_mode, 'LIVE') in ('LIVE', 'MICRO_LIVE')
              AND COALESCE(c.is_shadow, false) = false
              AND c.date_creation >= now() - (:lookbackDays * interval '1 day')
            ORDER BY UPPER(c.parsymbol)
            LIMIT :limit
            """, nativeQuery = true)
    List<String> findRecentLiveSymbolsForUser(
            @Param("idUser") String idUser,
            @Param("lookbackDays") int lookbackDays,
            @Param("limit") int limit
    );

    @Query(value = """
            SELECT DISTINCT UPPER(c.parsymbol)
            FROM futuros_operaciones.copy_operation c
            WHERE c.user_copy_allocation_id = :allocationId
              AND c.parsymbol IS NOT NULL
              AND trim(c.parsymbol) <> ''
              AND COALESCE(c.execution_mode, 'LIVE') in ('LIVE', 'MICRO_LIVE')
              AND COALESCE(c.is_shadow, false) = false
              AND c.date_creation >= now() - (:lookbackDays * interval '1 day')
            ORDER BY UPPER(c.parsymbol)
            LIMIT :limit
            """, nativeQuery = true)
    List<String> findRecentLiveSymbolsForAllocation(
            @Param("allocationId") Long allocationId,
            @Param("lookbackDays") int lookbackDays,
            @Param("limit") int limit
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE CopyOperationEntity c
            SET c.priceClose = :priceClose,
                c.dateClose = :dateClose,
                c.siseUsd = :sizeUsd,
                c.sizePar = :sizePar,
                c.active = false
            WHERE c.idOperation = :idOperation
              AND c.active = true
            """)
    int closeActiveById(
            @Param("idOperation") UUID idOperation,
            @Param("priceClose") BigDecimal priceClose,
            @Param("dateClose") OffsetDateTime dateClose,
            @Param("sizeUsd") BigDecimal sizeUsd,
            @Param("sizePar") BigDecimal sizePar
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE CopyOperationEntity c
            SET c.priceClose = :priceClose,
                c.dateClose = :dateClose,
                c.siseUsd = :sizeUsd,
                c.sizePar = :sizePar,
                c.active = false
            WHERE c.idOrderOrigin = :idOrderOrigin
              AND c.idUser = :idUser
              AND c.active = true
            """)
    int closeActiveByOriginAndUser(
            @Param("idOrderOrigin") String idOrderOrigin,
            @Param("idUser") String idUser,
            @Param("priceClose") BigDecimal priceClose,
            @Param("dateClose") OffsetDateTime dateClose,
            @Param("sizeUsd") BigDecimal sizeUsd,
            @Param("sizePar") BigDecimal sizePar
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE CopyOperationEntity c
            SET c.priceClose = :priceClose,
                c.dateClose = :dateClose,
                c.siseUsd = :sizeUsd,
                c.sizePar = :sizePar,
                c.active = false
            WHERE c.idOrderOrigin = :idOrderOrigin
              AND c.idUser = :idUser
              AND c.userCopyAllocationId = :allocationId
              AND c.active = true
            """)
    int closeActiveByOriginUserAndAllocation(
            @Param("idOrderOrigin") String idOrderOrigin,
            @Param("idUser") String idUser,
            @Param("allocationId") Long allocationId,
            @Param("priceClose") BigDecimal priceClose,
            @Param("dateClose") OffsetDateTime dateClose,
            @Param("sizeUsd") BigDecimal sizeUsd,
            @Param("sizePar") BigDecimal sizePar
    );

    @Query(value = """
            SELECT COALESCE(SUM((size_usd / NULLIF(leverage, 0)) * (1 + :safety)), 0)
            FROM futuros_operaciones.copy_operation
            WHERE id_user = :idUser
              AND id_wallet_origin = :walletId
              AND is_active = true
            """, nativeQuery = true)
    java.math.BigDecimal sumBufferedMarginActive(@Param("idUser") String idUser,
                                                 @Param("walletId") String walletId,
                                                 @Param("safety") java.math.BigDecimal safety);

    @Query(value = """
            SELECT COALESCE(SUM((size_usd / NULLIF(leverage, 0)) * (1 + :safety)), 0)
            FROM futuros_operaciones.copy_operation
            WHERE id_user = :idUser
              AND is_active = true
              AND COALESCE(is_shadow, false) = false
              AND COALESCE(execution_mode, 'LIVE') IN ('LIVE', 'MICRO_LIVE')
            """, nativeQuery = true)
    java.math.BigDecimal sumBufferedMarginActiveForUser(@Param("idUser") String idUser,
                                                        @Param("safety") java.math.BigDecimal safety);
}
