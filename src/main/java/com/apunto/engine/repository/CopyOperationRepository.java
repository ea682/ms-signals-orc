package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyOperationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
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
}

