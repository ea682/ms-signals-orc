package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationDto;
import com.apunto.engine.entity.CopyOperationEntity;
import com.apunto.engine.mapper.CopyOperationMapper;
import com.apunto.engine.repository.CopyOperationRepository;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.CopyOperationService;
import com.apunto.engine.shared.exception.CopyPersistenceConflictException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.Map;
import java.util.LinkedHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class CopyOperationServiceImpl implements CopyOperationService {

    private final CopyOperationRepository copyOperationRepository;
    private final CopyOperationMapper copyOperationMapper;
    private final ActiveCopyOperationCache activeCopyOperationCache;

    @Transactional
    @Override
    public void newOperation(CopyOperationDto operation) {
        Objects.requireNonNull(operation, "operation no puede ser null");
        Objects.requireNonNull(operation.getIdOrderOrigin(), "idOrderOrigin no puede ser null");
        Objects.requireNonNull(operation.getIdUser(), "idUser no puede ser null");

        CopyOperationEntity entity = buildCopyOperationEntity(operation);

        try {
            copyOperationRepository.saveAndFlush(entity);
            activeCopyOperationCache.markOpen(operation);
            log.info("event=copy_operation.insert_ok category=persistence reasonAlias=copy_created friendlyReason=copia_registrada explanation=la_copia_quedo_guardada_en_bd copyImpact=copy_tracked originId={} userId={} typeOperation={} wallet={} symbol={} orderId={}",
                    operation.getIdOrderOrigin(), operation.getIdUser(), operation.getTypeOperation(),
                    operation.getIdWalletOrigin(), operation.getParsymbol(), operation.getIdOrden());
        } catch (DataIntegrityViolationException ex) {
            if (isUniqueViolation(ex)) {
                final CopyOperationDto existing = findDuplicateOperation(operation).orElse(null);
                refreshActiveCacheAfterDuplicate(operation, existing, "copy_duplicate_detected");
                if (existing != null && Objects.equals(existing.getIdOrden(), operation.getIdOrden())) {
                    log.info("event=copy_operation.insert_idempotent category=persistence reasonAlias=copy_already_registered_same_order friendlyReason=copia_ya_registrada explanation=el_insert_se_repitio_para_la_misma_orden_y_se_toma_como_idempotente copyImpact=no_new_db_row originId={} userId={} typeOperation={} wallet={} symbol={} orderId={} existingCopyId={} existingActive={}",
                            operation.getIdOrderOrigin(), operation.getIdUser(), operation.getTypeOperation(),
                            operation.getIdWalletOrigin(), operation.getParsymbol(), operation.getIdOrden(),
                            existing.getIdOperation(), existing.isActive());
                    return;
                }
                log.error("event=copy_operation.insert_conflict category=persistence reasonAlias=copy_duplicate_conflict friendlyReason=conflicto_de_copia_duplicada explanation=ya_existe_una_copia_para_el_mismo_origen_usuario_y_lado_con_otra_orden copyImpact=copy_state_uncertain originId={} userId={} typeOperation={} wallet={} symbol={} incomingOrderId={} existingCopyId={} existingOrderId={} existingActive={}",
                        operation.getIdOrderOrigin(), operation.getIdUser(), operation.getTypeOperation(),
                        operation.getIdWalletOrigin(), operation.getParsymbol(), operation.getIdOrden(),
                        existing == null ? null : existing.getIdOperation(),
                        existing == null ? null : existing.getIdOrden(),
                        existing == null ? null : existing.isActive());
                throw new CopyPersistenceConflictException(
                        "Ya existe una copy_operation para el mismo origen, usuario y lado con otra orden",
                        ex,
                        conflictDetails(operation, existing)
                );
            }
            log.error("event=copy_operation.insert_failed category=persistence reasonAlias=copy_insert_failed friendlyReason=no_se_pudo_guardar_la_copia explanation=fallo_bd_al_insertar_copy_operation copyImpact=copy_state_uncertain originId={} userId={} typeOperation={} wallet={} symbol={} errClass={} errMsg=\"{}\"",
                    operation.getIdOrderOrigin(), operation.getIdUser(), operation.getTypeOperation(),
                    operation.getIdWalletOrigin(), operation.getParsymbol(), ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            throw ex;
        }
    }

    @Transactional
    @Override
    public void closeOperation(CopyOperationDto operation) {
        Objects.requireNonNull(operation, "operation no puede ser null");
        Objects.requireNonNull(operation.getIdOrderOrigin(), "idOrderOrigin no puede ser null");
        Objects.requireNonNull(operation.getIdUser(), "idUser no puede ser null");

        final BigDecimal sizeUsd = operation.getSiseUsd() == null ? BigDecimal.ZERO : operation.getSiseUsd();
        final BigDecimal sizePar = operation.getSizePar() == null ? BigDecimal.ZERO : operation.getSizePar();

        int updated = 0;
        if (operation.getIdOperation() != null) {
            updated = copyOperationRepository.closeActiveById(
                    operation.getIdOperation(),
                    operation.getPriceClose(),
                    operation.getDateClose(),
                    sizeUsd,
                    sizePar
            );
        }

        if (updated == 0) {
            updated = copyOperationRepository.closeActiveByOriginAndUser(
                    operation.getIdOrderOrigin(),
                    operation.getIdUser(),
                    operation.getPriceClose(),
                    operation.getDateClose(),
                    sizeUsd,
                    sizePar
            );
        }

        if (updated == 0) {
            log.info("event=copy_operation.close_noop originId={} userId={} reason=missing_or_already_closed",
                    operation.getIdOrderOrigin(), operation.getIdUser());
            return;
        }

        activeCopyOperationCache.markClosed(operation.getIdOrderOrigin(), operation.getIdUser());
        log.info("event=copy_operation.close_ok originId={} userId={} active=false updated={}",
                operation.getIdOrderOrigin(), operation.getIdUser(), updated);
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
    public Optional<CopyOperationEntity> findOperationByOrigin(String idOrderOrigin) {
        if (idOrderOrigin == null || idOrderOrigin.isBlank()) {
            return Optional.empty();
        }

        return copyOperationRepository.findAllByIdOrderOrigin(idOrderOrigin)
                .stream()
                .findFirst();
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
    public CopyOperationDto findOperationForUserAndType(String idOrderOrigin, String idUser, String typeOperation) {
        if (idOrderOrigin == null || idOrderOrigin.isBlank()
                || idUser == null || idUser.isBlank()
                || typeOperation == null || typeOperation.isBlank()) {
            return null;
        }

        return copyOperationRepository.findByIdOrderOriginAndIdUserAndTypeOperation(idOrderOrigin, idUser, typeOperation)
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

    @Transactional(readOnly = true)
    @Override
    public List<CopyOperationDto> findActiveOperationsByUserAndWallet(String idUser, String walletId) {
        if (idUser == null || idUser.isBlank() || walletId == null || walletId.isBlank()) {
            return List.of();
        }

        return copyOperationRepository.findAllByIdUserAndIdWalletOriginAndActiveTrue(idUser, walletId)
                .stream()
                .map(copyOperationMapper::toDto)
                .toList();
    }

    @Transactional
    @Override
    public void upsertActiveOperation(CopyOperationDto operation) {
        Objects.requireNonNull(operation, "operation no puede ser null");
        Objects.requireNonNull(operation.getIdOrderOrigin(), "idOrderOrigin no puede ser null");
        Objects.requireNonNull(operation.getIdUser(), "idUser no puede ser null");

        CopyOperationEntity entity = findOperationEntityForUpsert(operation).orElse(null);
        final boolean created = entity == null;

        if (created) {
            entity = buildCopyOperationEntity(operation);
        } else {
            applyOperation(entity, operation);
        }

        try {
            copyOperationRepository.saveAndFlush(entity);
        } catch (DataIntegrityViolationException ex) {
            if (isUniqueViolation(ex)) {
                final CopyOperationEntity recovered = findOperationEntityForUpsert(operation).orElse(null);
                if (recovered == null) {
                    markOperationUncertain(operation, "copy_upsert_conflict_unrecoverable");
                    log.error("event=copy_operation.upsert_conflict_unrecoverable category=persistence reasonAlias=copy_upsert_conflict friendlyReason=no_se_pudo_recuperar_la_copia explanation=hubo_conflicto_unico_pero_no_se_encontro_la_fila_existente copyImpact=copy_state_uncertain originId={} userId={} typeOperation={} wallet={} symbol={} incomingOrderId={}",
                            operation.getIdOrderOrigin(), operation.getIdUser(), operation.getTypeOperation(),
                            operation.getIdWalletOrigin(), operation.getParsymbol(), operation.getIdOrden());
                    throw new CopyPersistenceConflictException(
                            "Conflicto de upsert copy_operation sin fila recuperable",
                            ex,
                            conflictDetails(operation, null)
                    );
                }
                applyOperation(recovered, operation);
                copyOperationRepository.saveAndFlush(recovered);
                log.info("event=copy_operation.upsert_recovered category=persistence reasonAlias=copy_conflict_recovered friendlyReason=copia_actualizada_tras_conflicto explanation=se_recargo_la_fila_existente_y_se_actualizo_sin_crear_duplicado copyImpact=copy_tracked originId={} userId={} typeOperation={} wallet={} symbol={} orderId={} copyId={} active={}",
                        operation.getIdOrderOrigin(), operation.getIdUser(), operation.getTypeOperation(),
                        operation.getIdWalletOrigin(), operation.getParsymbol(), operation.getIdOrden(),
                        recovered.getIdOperation(), operation.isActive());
            } else {
                log.error("event=copy_operation.upsert_failed category=persistence reasonAlias=copy_upsert_failed friendlyReason=no_se_pudo_actualizar_la_copia explanation=fallo_bd_al_guardar_estado_de_copy_operation copyImpact=copy_state_uncertain originId={} userId={} typeOperation={} wallet={} symbol={} errClass={} errMsg=\"{}\"",
                        operation.getIdOrderOrigin(), operation.getIdUser(), operation.getTypeOperation(),
                        operation.getIdWalletOrigin(), operation.getParsymbol(), ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
                throw ex;
            }
        }

        if (operation.isActive()) {
            activeCopyOperationCache.markOpen(operation);
        } else {
            activeCopyOperationCache.markClosed(operation.getIdOrderOrigin(), operation.getIdUser());
        }

        log.info("event=copy_operation.upsert_ok category=persistence reasonAlias={} friendlyReason={} explanation={} copyImpact=copy_tracked originId={} userId={} typeOperation={} wallet={} symbol={} orderId={} active={} created={}",
                created ? "copy_created" : "copy_updated",
                created ? "copia_registrada" : "copia_actualizada",
                created ? "se_creo_el_estado_de_copia_en_bd" : "se_actualizo_el_estado_de_copia_existente_en_bd",
                operation.getIdOrderOrigin(), operation.getIdUser(), operation.getTypeOperation(),
                operation.getIdWalletOrigin(), operation.getParsymbol(), operation.getIdOrden(), operation.isActive(), created);
    }

    @Transactional(readOnly = true)
    @Override
    public BigDecimal sumBufferedMarginActive(String idUser, String walletId, BigDecimal safety) {
        if (idUser == null || idUser.isBlank() || walletId == null || walletId.isBlank()) {
            return BigDecimal.ZERO;
        }
        final BigDecimal s = safety == null ? BigDecimal.ZERO : safety;
        final BigDecimal v = copyOperationRepository.sumBufferedMarginActive(idUser, walletId, s);
        return v == null ? BigDecimal.ZERO : v;
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

    private Optional<CopyOperationEntity> findOperationEntityForUpsert(CopyOperationDto operation) {
        final String typeOperation = operation.getTypeOperation();
        if (typeOperation != null && !typeOperation.isBlank()) {
            final Optional<CopyOperationEntity> byType = copyOperationRepository
                    .findByIdOrderOriginAndIdUserAndTypeOperation(operation.getIdOrderOrigin(), operation.getIdUser(), typeOperation);
            if (byType.isPresent()) {
                return byType;
            }
        }
        return copyOperationRepository.findByIdOrderOriginAndIdUser(operation.getIdOrderOrigin(), operation.getIdUser());
    }

    private Optional<CopyOperationDto> findDuplicateOperation(CopyOperationDto operation) {
        return findOperationEntityForUpsert(operation).map(copyOperationMapper::toDto);
    }

    private Map<String, Object> conflictDetails(CopyOperationDto incoming, CopyOperationDto existing) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("originId", incoming.getIdOrderOrigin());
        details.put("userId", incoming.getIdUser());
        details.put("typeOperation", incoming.getTypeOperation());
        details.put("incomingOrderId", incoming.getIdOrden());
        details.put("existingCopyId", existing == null ? null : existing.getIdOperation());
        details.put("existingOrderId", existing == null ? null : existing.getIdOrden());
        details.put("existingActive", existing != null && existing.isActive());
        return details;
    }

    private void refreshActiveCacheAfterDuplicate(CopyOperationDto incoming, CopyOperationDto existing, String reasonCode) {
        if (existing == null) {
            markOperationUncertain(incoming, reasonCode);
            return;
        }
        if (existing.isActive()) {
            activeCopyOperationCache.markOpen(existing);
        } else {
            activeCopyOperationCache.markClosed(existing.getIdOrderOrigin(), existing.getIdUser());
        }
    }

    private void markOperationUncertain(CopyOperationDto operation, String reasonCode) {
        final String traceId = activeCopyOperationCache.traceId(
                operation.getIdOrderOrigin(),
                operation.getIdUser(),
                operation.getIdWalletOrigin(),
                operation.getParsymbol()
        );
        activeCopyOperationCache.markUncertain(operation, traceId, reasonCode);
    }

    private void applyOperation(CopyOperationEntity entity, CopyOperationDto operation) {
        entity.setIdOrden(operation.getIdOrden());
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
    }

    private boolean isUniqueViolation(DataIntegrityViolationException ex) {
        Throwable t = ex;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null) {
                String normalized = msg.toLowerCase();
                if (normalized.contains("23505")
                        || normalized.contains("duplicate key value violates unique constraint")
                        || normalized.contains("ux_copy_operation_origin_user_type")) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }

    private String safeLog(String value) {
        if (value == null) {
            return "null";
        }
        return value.replace('\n', ' ').replace('\r', ' ');
    }
}
