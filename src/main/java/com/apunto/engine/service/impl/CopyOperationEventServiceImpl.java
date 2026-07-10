package com.apunto.engine.service.impl;

import com.apunto.engine.dto.CopyOperationEventRecordCommand;
import com.apunto.engine.entity.CopyOperationEventEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.CopyOperationEventRepository;
import com.apunto.engine.outbox.service.MetricCopyOperationOutboxService;
import com.apunto.engine.service.CopyOperationEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class CopyOperationEventServiceImpl implements CopyOperationEventService {

    private final CopyOperationEventRepository repository;
    private final MetricCopyOperationOutboxService metricCopyOperationOutboxService;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(CopyOperationEventRecordCommand command) {
        persist(command, false);
    }

    @Override
    @Transactional
    public void recordRequired(CopyOperationEventRecordCommand command) {
        persist(command, true);
    }

    private void persist(CopyOperationEventRecordCommand command, boolean required) {
        if (!isRecordable(command)) {
            log.warn("event=copy_operation_event.skip category=audit reasonAlias=ledger_payload_incomplete friendlyReason=evento_de_historial_incompleto explanation=no_se_guardo_el_evento_porque_faltan_campos_minimos copyImpact=ledger_missing traceId={} originId={} userId={} symbol={} eventType={}",
                    safe(command == null ? null : command.getTraceId()),
                    safe(command == null ? null : command.getIdOrderOrigin()),
                    safe(command == null ? null : command.getIdUser()),
                    safe(command == null ? null : command.getParsymbol()),
                    safe(command == null ? null : command.getEventType()));
            if (required) {
                throw new IllegalArgumentException(
                        "Required copy_operation_event is missing minimum ledger fields");
            }
            return;
        }

        // Durable intents may generate multiple cumulative partial-fill progress
        // events. Legacy events without an intent retain clientOrderId idempotency.
        if (command.getDispatchIntentId() == null && StringUtils.hasText(command.getClientOrderId())) {
            var existing = repository.findByClientOrderId(command.getClientOrderId());
            if (existing.isPresent()) {
                log.info("event=copy_operation_event.idempotent category=audit reasonAlias=client_order_already_recorded friendlyReason=evento_ya_registrado explanation=el_clientOrderId_ya_existia_en_el_historial_y_no_se_duplica copyImpact=ledger_idempotent traceId={} originId={} userId={} symbol={} eventType={} clientOrderId={} existingEventId={}",
                        safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getParsymbol()),
                        safe(command.getEventType()), safe(command.getClientOrderId()), existing.get().getIdEvent());
                return;
            }
        }

        if (command.getDispatchIntentId() != null
                && repository.existsDispatchProgress(command.getDispatchIntentId(), command.getEventType(),
                command.getQtyExecuted(), command.getResultingQty())) {
            log.info("event=copy_operation_event.idempotent category=audit reasonAlias=dispatch_progress_already_recorded copyImpact=ledger_idempotent dispatchIntentId={} traceId={} originId={} userId={} symbol={} eventType={} qtyExecuted={} resultingQty={}",
                    command.getDispatchIntentId(), safe(command.getTraceId()), safe(command.getIdOrderOrigin()),
                    safe(command.getIdUser()), safe(command.getParsymbol()), safe(command.getEventType()),
                    command.getQtyExecuted(), command.getResultingQty());
            return;
        }

        CopyOperationEventEntity entity = toEntity(command);
        try {
            repository.saveAndFlush(entity);
            metricCopyOperationOutboxService.enqueue(entity);
            log.info("event=copy_operation_event.insert_ok category=audit reasonAlias=ledger_event_recorded friendlyReason=historial_actualizado explanation=se_guardo_el_movimiento_de_la_copia_para_reconstruir_pnl copyImpact=ledger_tracked traceId={} originId={} userId={} wallet={} symbol={} eventType={} copyIntent={} orderId={} clientOrderId={} qtyExecuted={} price={} resultingQty={} realizedPnlUsd={}",
                    safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getIdWalletOrigin()), safe(command.getParsymbol()),
                    safe(command.getEventType()), safe(command.getCopyIntent()), safe(command.getBinanceOrderId()), safe(command.getClientOrderId()),
                    command.getQtyExecuted(), command.getPrice(), command.getResultingQty(), command.getRealizedPnlUsd());
        } catch (DataIntegrityViolationException ex) {
            if (isUniqueViolation(ex) && StringUtils.hasText(command.getClientOrderId())) {
                log.info("event=copy_operation_event.insert_duplicate category=audit reasonAlias=ledger_duplicate_ignored friendlyReason=historial_ya_tenia_el_movimiento explanation=se_ignora_duplicado_por_clientOrderId copyImpact=ledger_idempotent traceId={} originId={} userId={} symbol={} eventType={} clientOrderId={}",
                        safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getParsymbol()),
                        safe(command.getEventType()), safe(command.getClientOrderId()));
                return;
            }
            log.error("event=copy_operation_event.insert_failed category=audit reasonAlias=ledger_insert_failed friendlyReason=no_se_pudo_guardar_historial explanation=fallo_bd_al_guardar_evento_de_copy_operation copyImpact=ledger_missing traceId={} originId={} userId={} symbol={} eventType={} errClass={} errMsg=\"{}\"",
                    safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getParsymbol()),
                    safe(command.getEventType()), ex.getClass().getSimpleName(), safe(ex.getMessage()));
            if (required) throw ex;
        } catch (DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            log.error("event=copy_operation_event.insert_failed category=audit reasonAlias=ledger_insert_failed friendlyReason=no_se_pudo_guardar_historial explanation=fallo_bd_al_guardar_evento_de_copy_operation copyImpact=ledger_missing traceId={} originId={} userId={} symbol={} eventType={} errClass={} errMsg=\"{}\"",
                    safe(command.getTraceId()), safe(command.getIdOrderOrigin()), safe(command.getIdUser()), safe(command.getParsymbol()),
                    safe(command.getEventType()), ex.getClass().getSimpleName(), safe(ex.getMessage()));
            if (required) throw ex;
        }
    }

    private boolean isRecordable(CopyOperationEventRecordCommand command) {
        return command != null
                && StringUtils.hasText(command.getIdOrderOrigin())
                && StringUtils.hasText(command.getIdUser())
                && StringUtils.hasText(command.getIdWalletOrigin())
                && StringUtils.hasText(command.getParsymbol())
                && StringUtils.hasText(command.getTypeOperation())
                && StringUtils.hasText(command.getEventType());
    }

    private CopyOperationEventEntity toEntity(CopyOperationEventRecordCommand command) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        return CopyOperationEventEntity.builder()
                .idOperation(command.getIdOperation())
                .dispatchIntentId(command.getDispatchIntentId())
                .userCopyAllocationId(command.getUserCopyAllocationId())
                .copyStrategyCode(normalizeStrategy(command.getCopyStrategyCode()))
                .executionMode(normalizeExecutionMode(command.getExecutionMode()))
                .shadow(Boolean.TRUE.equals(command.getShadow()))
                .decision(safeDecision(command.getDecision()))
                .decisionReason(safeReason(command.getDecisionReason()))
                .sourceMovementKey(safeReason(command.getSourceMovementKey()))
                .idOrderOrigin(command.getIdOrderOrigin())
                .idUser(command.getIdUser())
                .idWalletOrigin(command.getIdWalletOrigin())
                .parsymbol(command.getParsymbol())
                .typeOperation(command.getTypeOperation())
                .eventType(command.getEventType())
                .copyIntent(command.getCopyIntent())
                .binanceOrderId(command.getBinanceOrderId())
                .clientOrderId(command.getClientOrderId())
                .side(command.getSide())
                .positionSide(command.getPositionSide())
                .qtyRequested(command.getQtyRequested())
                .qtyExecuted(command.getQtyExecuted())
                .price(command.getPrice())
                .priceStatus(command.getPriceStatus())
                .notionalUsd(command.getNotionalUsd())
                .previousQty(command.getPreviousQty())
                .resultingQty(command.getResultingQty())
                .realizedPnlUsd(command.getRealizedPnlUsd())
                .feeUsd(command.getFeeUsd())
                .traceId(command.getTraceId())
                .source(command.getSource())
                .reasonCode(command.getReasonCode())
                .eventTime(Objects.requireNonNullElse(command.getEventTime(), now))
                .dateCreation(now)
                .build();
    }

    private boolean isUniqueViolation(DataIntegrityViolationException ex) {
        Throwable t = ex;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null) {
                String normalized = msg.toLowerCase();
                if (normalized.contains("23505")
                        || normalized.contains("duplicate key value violates unique constraint")
                        || normalized.contains("ux_copy_operation_event_client_order_id")
                        || normalized.contains("ux_copy_operation_event_dispatch_progress")) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }

    private String normalizeExecutionMode(String mode) {
        return UserCopyAllocationEntity.normalizeExecutionMode(mode);
    }

    private String normalizeStrategy(String strategy) {
        if (strategy == null || strategy.isBlank()) return null;
        return strategy.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
    }

    private String safeDecision(String value) {
        if (!StringUtils.hasText(value)) return null;
        String normalized = value.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        return normalized.length() > 40 ? normalized.substring(0, 40) : normalized;
    }

    private String safeReason(String value) {
        if (!StringUtils.hasText(value)) return null;
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('\"', '\'').trim();
        return clean.length() > 160 ? clean.substring(0, 160) : clean;
    }

    private String safe(String value) {
        if (!StringUtils.hasText(value)) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }
}
