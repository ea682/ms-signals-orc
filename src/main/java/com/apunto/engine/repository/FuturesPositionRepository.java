package com.apunto.engine.repository;

import com.apunto.engine.entity.FuturesPositionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface FuturesPositionRepository extends JpaRepository<FuturesPositionEntity, UUID> {
    Optional<FuturesPositionEntity> findByIdFuturesPosition(UUID idFuturesPosition);
}
