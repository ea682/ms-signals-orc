package com.apunto.engine.service;

import com.apunto.engine.dto.client.MetricaWalletDto;
import com.apunto.engine.entity.UserCopyAllocationEntity;

import java.util.List;

public interface UserCopyAllocationService {

    /**
     * Sincroniza la distribución "vigente" para un perfil maxWallet.
     *
     * - Upsert de wallets presentes en candidates (status=ACTIVE, ends_at=NULL)
     * - Marca como CLOSED las wallets que estaban vigentes y ya no vienen en candidates
     */
    void syncDistribution(int maxWallet, List<MetricaWalletDto> candidates);

    List<UserCopyAllocationEntity> getActiveDistribution(int maxWallet);
}
