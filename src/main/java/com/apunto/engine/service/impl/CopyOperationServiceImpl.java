package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;
import com.apunto.engine.mapper.CopyOperationMapper;
import com.apunto.engine.repository.CopyOperationRepository;
import com.apunto.engine.service.CopyOperationService;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@AllArgsConstructor
public class CopyOperationServiceImpl implements CopyOperationService {

    private final CopyOperationRepository copyOperationRepository;
    private final CopyOperationMapper copyOperationMapper;

    @Transactional
    @Override
    public void newOperation(CopyOperationDto operation) {
        buildCopyOperationEntity(operation);
    }

    @Override
    @Transactional
    public void closeOperation(CopyOperationDto operation) {
        CopyOperationEntity entity = copyOperationRepository
                .findByIdOrderOrigin(operation.getIdOrderOrigin());

        if (entity == null) {
            throw new RuntimeException("No se encontró operación con idOrderOrigin: " + operation.getIdOrderOrigin());
        }

        entity.setPriceClose(operation.getPriceClose());
        entity.setDateClose(operation.getDateClose());
        entity.setActive(operation.isActive());

        copyOperationRepository.save(entity);
    }

    @Override
    public CopyOperationDto findOperation(String idOrden) {
        CopyOperationEntity entity =
                copyOperationRepository.findByIdOrderOrigin(idOrden);

        if (entity == null) {
            return null;
        }

        return copyOperationMapper.toDto(entity);
    }

    private void buildCopyOperationEntity(CopyOperationDto operation) {
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

        copyOperationRepository.save(entity);
    }
}
