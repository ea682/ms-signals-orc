package com.apunto.engine.service;

import com.apunto.engine.dto.FuturesPositionDto;
import com.apunto.engine.dto.OriginBasketPositionDto;

import java.util.List;
import java.util.Optional;

public interface FuturesPositionService {
    Optional<FuturesPositionDto> getIdFuturesPosition(String idFuturesPosition);
    List<OriginBasketPositionDto> getOpenBasketByWallet(String walletId);
}
