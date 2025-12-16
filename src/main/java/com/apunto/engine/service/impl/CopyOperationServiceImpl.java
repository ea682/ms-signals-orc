package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;
import com.apunto.engine.mapper.CopyOperationMapper;
import com.apunto.engine.repository.CopyOperationRepository;
import com.apunto.engine.service.CopyOperationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

@Service
@Slf4j
@RequiredArgsConstructor
public class CopyOperationServiceImpl implements CopyOperationService {

    private final CopyOperationRepository copyOperationRepository;
    private final CopyOperationMapper copyOperationMapper;

    @Transactional
    @Override
    public void newOperation(CopyOperationDto operation) {
        Objects.requireNonNull(operation, "operation no puede ser null");
        Objects.requireNonNull(operation.getIdOrderOrigin(), "idOrderOrigin no puede ser null");
        Objects.requireNonNull(operation.getIdUser(), "idUser no puede ser null");

        CopyOperationEntity entity = buildCopyOperationEntity(operation);

        try {
            copyOperationRepository.save(entity);
            log.info("event=copy_operation.insert_ok originId={} userId={} orderId={}",
                    operation.getIdOrderOrigin(), operation.getIdUser(), operation.getIdOrden());
        } catch (DataIntegrityViolationException ex) {
            if (isUniqueViolation(ex)) {
                log.info("event=copy_operation.insert_duplicate originId={} userId={} orderId={}",
                        operation.getIdOrderOrigin(), operation.getIdUser(), operation.getIdOrden());
                return;
            }
            log.error("event=copy_operation.insert_failed originId={} userId={} err={}",
                    operation.getIdOrderOrigin(), operation.getIdUser(), ex.toString());
            throw ex;
        }
    }

    @Transactional
    @Override
    public void closeOperation(CopyOperationDto operation) {
        Objects.requireNonNull(operation, "operation no puede ser null");
        Objects.requireNonNull(operation.getIdOrderOrigin(), "idOrderOrigin no puede ser null");
        Objects.requireNonNull(operation.getIdUser(), "idUser no puede ser null");

        CopyOperationEntity entity = copyOperationRepository
                .findByIdOrderOriginAndIdUser(operation.getIdOrderOrigin(), operation.getIdUser())
                .orElse(null);

        if (entity == null) {
            log.warn("event=copy_operation.close_missing originId={} userId={}",
                    operation.getIdOrderOrigin(), operation.getIdUser());
            return;
        }

        if (!entity.isActive()) {
            log.info("event=copy_operation.close_already_closed originId={} userId={}",
                    operation.getIdOrderOrigin(), operation.getIdUser());
            return;
        }

        entity.setPriceClose(operation.getPriceClose());
        entity.setDateClose(operation.getDateClose());
        entity.setActive(operation.isActive());

        copyOperationRepository.save(entity);

        log.info("event=copy_operation.close_ok originId={} userId={} active=false",
                operation.getIdOrderOrigin(), operation.getIdUser());
    }

    @Transactional(readOnly = true)
    @Override
    public CopyOperationDto findOperation(String idOrden) {
        if (idOrden == null || idOrden.isBlank()) {
            return null;
        }

        CopyOperationEntity entity = copyOperationRepository.findByIdOrden(idOrden).orElse(null);
        return entity == null ? null : copyOperationMapper.toDto(entity);
    }

    @Transactional(readOnly = true)
    @Override
    public List<CopyOperationDto> findOperationsByOrigin(String idOrderOrigin) {
        if (idOrderOrigin == null || idOrderOrigin.isBlank()) {
            return List.of();
        }

        return copyOperationRepository.findAllByIdOrderOrigin(idOrderOrigin)
                .stream()
                .map(copyOperationMapper::toDto)
                .toList();
    }

    @Transactional(readOnly = true)
    @Override
    public CopyOperationDto findOperationForUser(String idOrderOrigin, String idUser) {
        if (idOrderOrigin == null || idOrderOrigin.isBlank() || idUser == null || idUser.isBlank()) {
            return null;
        }

        return copyOperationRepository.findByIdOrderOriginAndIdUser(idOrderOrigin, idUser)
                .map(copyOperationMapper::toDto)
                .orElse(null);
    }

    @Transactional(readOnly = true)
    @Override
    public boolean existsByOriginAndUser(String idOrderOrigin, String idUser) {
        if (idOrderOrigin == null || idOrderOrigin.isBlank() || idUser == null || idUser.isBlank()) {
            return false;
        }
        return copyOperationRepository.existsByIdOrderOriginAndIdUser(idOrderOrigin, idUser);
    }

    private CopyOperationEntity buildCopyOperationEntity(CopyOperationDto operation) {
        CopyOperationEntity entity = new CopyOperationEntity();

        entity.setIdOrden(operation.getIdOrden());
        entity.setIdOrderOrigin(operation.getIdOrderOrigin());
        entity.setIdUser(operation.getIdUser());
        entity.setIdWalletOrigin(operation.getIdWalletOrigin());
        entity.setParsymbol(operation.getParsymbol());
        entity.setTypeOperation(operation.getTypeOperation());
        entity.setLeverage(operation.getLeverage());
        entity.setSiseUsd(operation.getSiseUsd());
        entity.setSizePar(operation.getSizePar());
        entity.setPriceEntry(operation.getPriceEntry());
        entity.setPriceClose(operation.getPriceClose());
        entity.setDateCreation(operation.getDateCreation());
        entity.setDateClose(operation.getDateClose());
        entity.setActive(operation.isActive());

        return entity;
    }

    private boolean isUniqueViolation(DataIntegrityViolationException ex) {
        Throwable t = ex;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("23505")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
