package com.apunto.engine.service;

import com.apunto.engine.dto.FuturesPositionDto;

import java.util.Optional;

public interface FuturesPositionService {
    Optional<FuturesPositionDto> getIdFuturesPosition(String idFuturesPosition);
}
