package com.apunto.engine.service;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.events.OperacionEvent;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public interface ShadowCopyTradingService {

    void syncShadowAllocations(UUID idUser, List<MetricaWalletDto> candidates, int userMaxWallet, OffsetDateTime now);

    void linkLiveAllocations(UUID idUser, List<UserCopyAllocationEntity> liveAllocations);

    int recordShadowEvent(OperacionEvent event);

    default int recordShadowEvent(OperacionEvent event, long eventReceivedNs) {
        return recordShadowEvent(event);
    }

    boolean isSeparateShadowEnabled();

    boolean isLivePromotable(UUID idUser, MetricaWalletDto candidate);

    default boolean isMicroLivePromotable(UUID idUser, MetricaWalletDto candidate) {
        return false;
    }
}
