package com.apunto.engine.service.impl;

import com.apunto.engine.dto.FuturesPositionDto;
import com.apunto.engine.dto.OriginBasketPositionDto;
import com.apunto.engine.entity.FuturesPositionEntity;
import com.apunto.engine.repository.FuturesPositionRepository;
import com.apunto.engine.service.FuturesPositionService;
import com.apunto.engine.shared.enums.PositionStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class FuturesPositionImpl implements FuturesPositionService {

    private static final String LOG_EVENT_FIND_BY_ID_SKIPPED =
            "event=futures_position.find_by_id_skipped reason={}";
    private static final String LOG_EVENT_FIND_BY_ID_SKIPPED_WITH_ID =
            "event=futures_position.find_by_id_skipped reason={} idFuturesPosition={}";
    private static final String LOG_EVENT_FIND_BY_ID_NOT_FOUND =
            "event=futures_position.find_by_id_not_found idFuturesPosition={}";
    private static final String LOG_EVENT_FIND_BY_ID_OK =
            "event=futures_position.find_by_id_ok idFuturesPosition={}";

    private static final String REASON_EMPTY_ID = "empty_id";
    private static final String REASON_INVALID_UUID = "invalid_uuid";

    private final FuturesPositionRepository futuresPositionRepository;

    @Override
    public Optional<FuturesPositionDto> getIdFuturesPosition(String idFuturesPosition) {
        if (idFuturesPosition == null || idFuturesPosition.isBlank()) {
            log.warn(LOG_EVENT_FIND_BY_ID_SKIPPED, REASON_EMPTY_ID);
            return Optional.empty();
        }

        final UUID id;
        try {
            id = UUID.fromString(idFuturesPosition);
        } catch (IllegalArgumentException e) {
            log.warn(LOG_EVENT_FIND_BY_ID_SKIPPED_WITH_ID, REASON_INVALID_UUID, idFuturesPosition);
            return Optional.empty();
        }

        final Optional<FuturesPositionDto> result = futuresPositionRepository.findByIdFuturesPosition(id)
                .map(this::toDto);

        if (result.isEmpty()) {
            log.debug(LOG_EVENT_FIND_BY_ID_NOT_FOUND, id);
            return Optional.empty();
        }

        log.debug(LOG_EVENT_FIND_BY_ID_OK, id);
        return result;
    }


    @Override
    public List<OriginBasketPositionDto> getOpenBasketByWallet(String walletId) {
        if (walletId == null || walletId.isBlank()) {
            return List.of();
        }

        return futuresPositionRepository
                .findAllOpenByAccountIdOrderByCreatedAtAsc(walletId, PositionStatus.OPEN.name())
                .stream()
                .map(this::toOriginBasketDto)
                .toList();
    }

    private FuturesPositionDto toDto(FuturesPositionEntity entity) {
        FuturesPositionDto dto = new FuturesPositionDto();
        dto.setIdFuturesPosition(entity.getIdFuturesPosition());
        dto.setIsActive(entity.getStatus());
        return dto;
    }

    private OriginBasketPositionDto toOriginBasketDto(FuturesPositionEntity entity) {
        return OriginBasketPositionDto.builder()
                .originId(entity.getIdFuturesPosition() == null ? null : entity.getIdFuturesPosition().toString())
                .walletId(entity.getAccountId())
                .symbol(entity.getSymbol())
                .side(entity.getSide())
                .entryPrice(entity.getEntryPrice())
                .markPrice(entity.getMarkPrice())
                .marginUsedUsd(entity.getMarginUsedUsd())
                .notionalUsd(entity.getNotionalUsd())
                .sizeQty(entity.getSizeQty())
                .sizeLegacy(entity.getSizeLegacy())
                .build();
    }
}
