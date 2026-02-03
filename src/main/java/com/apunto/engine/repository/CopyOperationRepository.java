package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyOperationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CopyOperationRepository extends JpaRepository<CopyOperationEntity, UUID> {

    List<CopyOperationEntity> findAllByIdOrderOrigin(String idOrderOrigin);

    Optional<CopyOperationEntity> findByIdOrderOriginAndIdUser(String idOrderOrigin, String idUser);

    boolean existsByIdOrderOriginAndIdUser(String idOrderOrigin, String idUser);

    Optional<CopyOperationEntity> findByIdOrden(String idOrden);

    List<CopyOperationEntity> findAllByIdOrden(String idOrden);

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

