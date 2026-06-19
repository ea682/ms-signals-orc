package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowCopyOperationEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ShadowCopyOperationEventRepository extends JpaRepository<ShadowCopyOperationEventEntity, UUID> {
}
