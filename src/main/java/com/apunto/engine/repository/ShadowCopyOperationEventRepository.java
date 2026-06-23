package com.apunto.engine.repository;

import com.apunto.engine.entity.ShadowCopyOperationEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.UUID;

@Repository
public interface ShadowCopyOperationEventRepository extends JpaRepository<ShadowCopyOperationEventEntity, UUID> {

    boolean existsByShadowAllocationIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
            Long shadowAllocationId,
            String idOrderOrigin,
            String eventType,
            String positionSide,
            OffsetDateTime eventTime
    );

    boolean existsByWalletProfileIdAndIdOrderOriginAndEventTypeAndPositionSideAndEventTime(
            Long walletProfileId,
            String idOrderOrigin,
            String eventType,
            String positionSide,
            OffsetDateTime eventTime
    );
}
