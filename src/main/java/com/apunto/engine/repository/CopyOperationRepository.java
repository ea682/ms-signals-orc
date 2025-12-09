package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyOperationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface CopyOperationRepository extends JpaRepository<CopyOperationEntity, UUID> {

    CopyOperationEntity findByIdOrderOrigin(String idOrdenOrigin);

    List<CopyOperationEntity> findByIdOrden(String idOrden);
}
