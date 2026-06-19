package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowPositionStateEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.List;
import java.util.UUID;

@Repository
public interface ShadowPositionStateRepository extends JpaRepository<ShadowPositionStateEntity, UUID> {

    Optional<ShadowPositionStateEntity> findFirstByShadowAllocationIdAndParsymbolAndPositionSideAndStatus(
            Long shadowAllocationId,
            String parsymbol,
            String positionSide,
            String status
    );

    List<ShadowPositionStateEntity> findAllByShadowAllocationIdAndParsymbolAndStatus(
            Long shadowAllocationId,
            String parsymbol,
            String status
    );
}
