package com.apunto.engine.repository;

import com.apunto.engine.entity.CopyEconomicCycleEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface CopyEconomicCycleRepository extends JpaRepository<CopyEconomicCycleEntity, UUID> {

    Optional<CopyEconomicCycleEntity> findByCopyOperationId(UUID copyOperationId);

    @Query(value = "select nextval('futuros_operaciones.copy_economic_cycle_sequence')", nativeQuery = true)
    Long nextCycleSequence();
}
