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

    Optional<CopyOperationEntity> findByIdOrderOriginAndIdUser(String idOrderOrigin, String idUser);

    Optional<CopyOperationEntity> findByIdOrderOriginAndIdUserAndTypeOperation(String idOrderOrigin, String idUser, String typeOperation);

    Optional<CopyOperationEntity> findByIdOrderOriginAndIdUserAndCopyStrategyCode(String idOrderOrigin, String idUser, String copyStrategyCode);

    Optional<CopyOperationEntity> findByIdOrderOriginAndIdUserAndTypeOperationAndCopyStrategyCode(String idOrderOrigin, String idUser, String typeOperation, String copyStrategyCode);

    boolean existsByIdOrderOriginAndIdUser(String idOrderOrigin, String idUser);

    Optional<CopyOperationEntity> findByIdOrden(String idOrden);

    List<CopyOperationEntity> findAllByIdOrden(String idOrden);

    List<CopyOperationEntity> findAllByIdUserAndIdWalletOriginAndActiveTrue(String idUser, String walletId);

    List<CopyOperationEntity> findAllByActiveTrue();

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
              AND c.copyStrategyCode = :copyStrategyCode
              AND c.active = true
            """)
    int closeActiveByOriginAndUserAndCopyStrategyCode(
            @Param("idOrderOrigin") String idOrderOrigin,
            @Param("idUser") String idUser,
            @Param("copyStrategyCode") String copyStrategyCode,
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
}

