package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyOperationEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface CopyOperationEventRepository extends JpaRepository<CopyOperationEventEntity, UUID> {
    Optional<CopyOperationEventEntity> findByClientOrderId(String clientOrderId);
}
