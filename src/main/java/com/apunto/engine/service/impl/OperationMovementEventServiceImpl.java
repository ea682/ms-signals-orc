package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.OperationMovementEventRecordCommand;
import com.apunto.engine.entity.OperationMovementEventEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.repository.OperationMovementEventRepository;
import com.apunto.engine.service.OperationMovementEventService;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.util.CopyTraceIdUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class OperationMovementEventServiceImpl implements OperationMovementEventService {

    private static final String SOURCE_DIRECT_INGEST = "hyperliquid_direct_ingest";
    private static final String SOURCE_OPERATION_EVENT_INGEST = "operation_event_ingest";
    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final int CALC_SCALE = 18;

    private final OperationMovementEventRepository repository;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;
    private final boolean enabled;
    private final BlockingQueue<QueuedMovement> queue;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicLong submitted = new AtomicLong(0);
    private final AtomicLong persisted = new AtomicLong(0);
    private final AtomicLong skipped = new AtomicLong(0);
    private final AtomicLong rejected = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);

    public OperationMovementEventServiceImpl(
            OperationMovementEventRepository repository,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager,
            MeterRegistry meterRegistry,
            @Value("${operation.movement-ledger.enabled:true}") boolean enabled,
            @Value("${operation.movement-ledger.worker-threads:2}") int workerThreads,
            @Value("${operation.movement-ledger.queue-capacity:20000}") int queueCapacity
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.meterRegistry = meterRegistry;
        this.enabled = enabled;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
        this.workers = Executors.newFixedThreadPool(Math.max(1, workerThreads), new NamedThreadFactory("operation-movement-ledger-"));
        registerMetrics();
        log.info("event=operation_movement_event.config category=audit enabled={} workerThreads={} queueCapacity={}",
                enabled, Math.max(1, workerThreads), Math.max(1, queueCapacity));
    }

    @PostConstruct
    public void start() {
        if (!enabled) {
            log.warn("event=operation_movement_event.disabled category=audit friendlyReason=historial_de_movimientos_desactivado explanation=no_se_guardaran_movimientos_de_operaciones");
            return;
        }
        running.set(true);
        int workerCount = Math.max(1, ((java.util.concurrent.ThreadPoolExecutor) workers).getCorePoolSize());
        for (int i = 0; i < workerCount; i++) {
            workers.execute(this::workerLoop);
        }
        log.info("event=operation_movement_event.started category=audit queueCapacity={}", queue.remainingCapacity() + queue.size());
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        workers.shutdownNow();
        log.info("event=operation_movement_event.stopped category=audit queueDepth={} submitted={} persisted={} skipped={} rejected={} failed={}",
                queue.size(), submitted.get(), persisted.get(), skipped.get(), rejected.get(), failed.get());
    }

    @Override
    public void recordAsync(HyperliquidMappedDelta mappedDelta, HyperliquidDirectCopyDispatchResult dispatchResult, String reasonCode) {
        if (!enabled) {
            return;
        }
        OperationMovementEventRecordCommand command = fromMappedDelta(mappedDelta, dispatchResult, reasonCode);
        submit(command);
    }

    @Override
    public void recordAsync(OperacionEvent event, String source, String traceId, String reasonCode) {
        if (!enabled) {
            return;
        }
        OperationMovementEventRecordCommand command = fromOperacionEvent(event, source, traceId, reasonCode);
        submit(command);
    }

    private void submit(OperationMovementEventRecordCommand command) {
        if (!isRecordable(command)) {
            skipped.incrementAndGet();
            meterRegistry.counter("signals.operation_movement_event.skipped.total", "reason", "payload_incomplete").increment();
            log.warn("event=operation_movement_event.skip category=audit reasonAlias=ledger_payload_incomplete friendlyReason=evento_de_operacion_incompleto explanation=no_se_guardo_movimiento_porque_faltan_campos_minimos copyImpact=copy_not_affected traceId={} originId={} wallet={} symbol={} deltaType={} movementKey={}",
                    safe(command == null ? null : command.getTraceId()),
                    safe(command == null ? null : asString(command.getIdOrderOrigin())),
                    safe(command == null ? null : command.getIdWalletOrigin()),
                    safe(command == null ? null : command.getParsymbol()),
                    safe(command == null ? null : command.getDeltaType()),
                    safe(command == null ? null : command.getMovementKey()));
            return;
        }
        QueuedMovement queuedMovement = new QueuedMovement(command, System.nanoTime());
        if (!queue.offer(queuedMovement)) {
            rejected.incrementAndGet();
            meterRegistry.counter("signals.operation_movement_event.rejected.total", "reason", "queue_full").increment();
            log.error("event=operation_movement_event.rejected category=audit reasonAlias=ledger_queue_full friendlyReason=cola_de_historial_llena explanation=no_se_bloquea_el_copiado_pero_no_se_pudo_encolar_el_movimiento copyImpact=copy_not_blocked traceId={} originId={} wallet={} symbol={} deltaType={} movementKey={} queueDepth={}",
                    safe(command.getTraceId()), safe(asString(command.getIdOrderOrigin())), safe(command.getIdWalletOrigin()), safe(command.getParsymbol()),
                    safe(command.getDeltaType()), safe(command.getMovementKey()), queue.size());
            return;
        }
        submitted.incrementAndGet();
        meterRegistry.counter("signals.operation_movement_event.submitted.total", "deltaType", safeTag(command.getDeltaType())).increment();
    }

    private void workerLoop() {
        activeWorkers.incrementAndGet();
        try {
            while (running.get() || !queue.isEmpty()) {
                QueuedMovement task = queue.take();
                persistSafely(task);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            activeWorkers.decrementAndGet();
        }
    }

    private void persistSafely(QueuedMovement task) {
        OperationMovementEventRecordCommand command = task.command();
        try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", safeTraceId(command))) {
            long startedNs = System.nanoTime();
            transactionTemplate.executeWithoutResult(status -> persist(command, task.acceptedNs(), startedNs));
        } catch (DataIntegrityViolationException ex) {
            if (isUniqueViolation(ex)) {
                skipped.incrementAndGet();
                log.info("event=operation_movement_event.insert_duplicate category=audit reasonAlias=ledger_duplicate_ignored friendlyReason=historial_ya_tenia_el_movimiento explanation=se_ignora_duplicado_por_movementKey copyImpact=ledger_idempotent traceId={} originId={} wallet={} symbol={} deltaType={} movementKey={}",
                        safe(command.getTraceId()), safe(asString(command.getIdOrderOrigin())), safe(command.getIdWalletOrigin()), safe(command.getParsymbol()),
                        safe(command.getDeltaType()), safe(command.getMovementKey()));
                return;
            }
            failed(command, task, ex);
        } catch (DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            failed(command, task, ex);
        }
    }

    private void persist(OperationMovementEventRecordCommand command, long acceptedNs, long startedNs) {
        if (repository.existsByMovementKeyInGuard(command.getMovementKey())) {
            skipped.incrementAndGet();
            log.info("event=operation_movement_event.idempotent category=audit reasonAlias=movement_already_recorded friendlyReason=movimiento_ya_registrado explanation=movementKey_ya_existia_en_guard_y_no_se_duplica copyImpact=ledger_idempotent traceId={} originId={} wallet={} symbol={} deltaType={} movementKey={}",
                    safe(command.getTraceId()), safe(asString(command.getIdOrderOrigin())), safe(command.getIdWalletOrigin()), safe(command.getParsymbol()),
                    safe(command.getDeltaType()), safe(command.getMovementKey()));
            return;
        }

        OperationMovementEventEntity previous = previousMovement(command);
        OperationMovementEventEntity entity = toEntity(command, previous);
        repository.saveAndFlush(entity);
        persisted.incrementAndGet();
        meterRegistry.counter("signals.operation_movement_event.persisted.total", "eventType", safeTag(entity.getEventType()), "deltaType", safeTag(entity.getDeltaType())).increment();
        meterRegistry.timer("signals.operation_movement_event.persist.duration", "result", "ok", "eventType", safeTag(entity.getEventType()))
                .record(Duration.ofNanos(System.nanoTime() - startedNs));
        log.info("event=operation_movement_event.insert_ok category=audit reasonAlias=origin_movement_recorded friendlyReason=historial_de_operacion_actualizado explanation=se_guardo_el_movimiento_original_para_reconstruir_rentabilidad_y_trazabilidad copyImpact=copy_not_blocked traceId={} originId={} wallet={} symbol={} side={} eventType={} deltaType={} previousSizeQty={} resultingSizeQty={} deltaSizeQty={} realizedPnlUsd={} copyEligibleUsers={} copySubmittedTasks={} queueDelayMs={} elapsedMs={} queueDepth={}",
                safe(entity.getTraceId()), safe(asString(entity.getIdOrderOrigin())), safe(entity.getIdWalletOrigin()), safe(entity.getParsymbol()), safe(entity.getTypeOperation()),
                safe(entity.getEventType()), safe(entity.getDeltaType()), entity.getPreviousSizeQty(), entity.getResultingSizeQty(), entity.getDeltaSizeQty(), entity.getRealizedPnlUsd(),
                entity.getCopyEligibleUsers(), entity.getCopySubmittedTasks(), elapsedMs(acceptedNs), elapsedMs(startedNs), queue.size());
    }

    private OperationMovementEventEntity previousMovement(OperationMovementEventRecordCommand command) {
        if (!StringUtils.hasText(command.getPositionKey())) {
            return null;
        }
        if (command.getEventTime() != null) {
            return repository.findTopByPositionKeyAndEventTimeLessThanEqualOrderByEventTimeDescDateCreationDesc(command.getPositionKey(), command.getEventTime())
                    .orElse(null);
        }
        return repository.findTopByPositionKeyOrderByEventTimeDescDateCreationDesc(command.getPositionKey()).orElse(null);
    }

    private void failed(OperationMovementEventRecordCommand command, QueuedMovement task, RuntimeException ex) {
        failed.incrementAndGet();
        meterRegistry.counter("signals.operation_movement_event.failed.total", "reason", safeTag(ex.getClass().getSimpleName())).increment();
        log.error("event=operation_movement_event.insert_failed category=audit reasonAlias=ledger_insert_failed friendlyReason=no_se_pudo_guardar_historial_de_operacion explanation=fallo_bd_al_guardar_movimiento_original copyImpact=copy_not_blocked traceId={} originId={} wallet={} symbol={} deltaType={} movementKey={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={}",
                safe(command == null ? null : command.getTraceId()),
                safe(command == null ? null : asString(command.getIdOrderOrigin())),
                safe(command == null ? null : command.getIdWalletOrigin()),
                safe(command == null ? null : command.getParsymbol()),
                safe(command == null ? null : command.getDeltaType()),
                safe(command == null ? null : command.getMovementKey()),
                ex.getClass().getSimpleName(), safe(ex.getMessage()), elapsedMs(task.acceptedNs()), 0L, queue.size(), ex);
    }

    private OperationMovementEventRecordCommand fromMappedDelta(HyperliquidMappedDelta mappedDelta, HyperliquidDirectCopyDispatchResult dispatchResult, String reasonCode) {
        if (mappedDelta == null || mappedDelta.event() == null || mappedDelta.event().getOperacion() == null) {
            return null;
        }
        OperacionEvent event = mappedDelta.event();
        OperacionDto op = event.getOperacion();
        HyperliquidDeltaRequest req = mappedDelta.request();
        OffsetDateTime sourceTs = req == null ? null : fromEpochMillis(req.sourceTs());
        OffsetDateTime detectedAt = req == null ? null : fromInstant(req.detectedAt());
        OffsetDateTime publishedAt = req == null ? null : fromInstant(req.publishedAt());
        OffsetDateTime eventTime = firstNonNull(sourceTs, detectedAt, publishedAt, fromInstant(op.getFechaCierre()), fromInstant(op.getFechaCreacion()), utcNow());
        String deltaType = firstNonBlank(mappedDelta.deltaType(), event.getDeltaType(), req == null ? null : req.deltaType(), "UNKNOWN");
        String traceId = currentOrOriginTraceId(op.getIdOperacion(), firstNonBlank(mappedDelta.wallet(), op.getIdCuenta()), firstNonBlank(mappedDelta.symbol(), op.getParSymbol()));

        return OperationMovementEventRecordCommand.builder()
                .idOrderOrigin(op.getIdOperacion())
                .movementKey(compactKey(buildMovementKey(mappedDelta, eventTime)))
                .idempotencyKey(firstNonBlank(mappedDelta.idempotencyKey(), req == null ? null : req.idempotencyKey()))
                .positionKey(firstNonBlank(mappedDelta.positionKey(), buildPositionKey(op)))
                .idWalletOrigin(firstNonBlank(mappedDelta.wallet(), op.getIdCuenta()))
                .parsymbol(firstNonBlank(mappedDelta.symbol(), op.getParSymbol()))
                .typeOperation(firstNonBlank(mappedDelta.side(), req == null ? null : req.side(), op.getTipoOperacion() == null ? null : op.getTipoOperacion().name()))
                .eventType("UNKNOWN")
                .deltaType(deltaType)
                .sourceEventType(event.getTipo() == null ? null : event.getTipo().name())
                .status(req == null ? null : req.status())
                .sizeQty(firstNonNull(req == null ? null : req.sizeQty(), op.getSizeQty(), op.getSize()))
                .signedSizeQty(req == null ? null : req.signedSizeQty())
                .notionalUsd(firstNonNull(req == null ? null : req.notionalUsd(), op.getNotionalUsd()))
                .marginUsedUsd(firstNonNull(req == null ? null : req.marginUsedUsd(), op.getMarginUsedUsd()))
                .entryPrice(firstNonNull(req == null ? null : req.entryPrice(), op.getPrecioEntrada()))
                .markPrice(firstNonNull(req == null ? null : req.markPrice(), op.getPrecioMercado()))
                .exitPrice(op.getPrecioCierre())
                .leverage(req == null ? null : req.leverage())
                .walletVersion(req == null ? null : req.walletVersion())
                .snapshotVersion(req == null ? null : req.snapshotVersion())
                .sourceTs(sourceTs)
                .detectedAt(detectedAt)
                .publishedAt(publishedAt)
                .eventTime(eventTime)
                .traceId(traceId)
                .source(SOURCE_DIRECT_INGEST)
                .reasonCode(reasonCode)
                .copyEligibleUsers(dispatchResult == null ? null : dispatchResult.eligibleUsers())
                .copySubmittedTasks(dispatchResult == null ? null : dispatchResult.submittedTasks())
                .copyBusinessSkipped(dispatchResult == null ? null : dispatchResult.businessSkipped())
                .copyFallbackJobs(dispatchResult == null ? null : dispatchResult.fallbackJobs())
                .copyFallbackUsed(dispatchResult == null ? null : dispatchResult.fallbackUsed())
                .raw(rawFromMapped(mappedDelta, dispatchResult, reasonCode))
                .build();
    }

    private OperationMovementEventRecordCommand fromOperacionEvent(OperacionEvent event, String source, String traceId, String reasonCode) {
        if (event == null || event.getOperacion() == null || event.getOperacion().getIdOperacion() == null) {
            return null;
        }
        OperacionDto op = event.getOperacion();
        OffsetDateTime eventTime = firstNonNull(fromInstant(op.getFechaCierre()), fromInstant(op.getFechaCreacion()), utcNow());
        String effectiveTraceId = firstNonBlank(traceId, currentOrOriginTraceId(op.getIdOperacion(), op.getIdCuenta(), op.getParSymbol()));
        String movementKey = compactKey(buildMovementKey(event, eventTime, source));
        return OperationMovementEventRecordCommand.builder()
                .idOrderOrigin(op.getIdOperacion())
                .movementKey(movementKey)
                .idempotencyKey(null)
                .positionKey(buildPositionKey(op))
                .idWalletOrigin(op.getIdCuenta())
                .parsymbol(op.getParSymbol())
                .typeOperation(op.getTipoOperacion() == null ? null : op.getTipoOperacion().name())
                .eventType("UNKNOWN")
                .deltaType(firstNonBlank(event.getDeltaType(), event.getTipo() == OperacionEvent.Tipo.CERRADA ? "CLOSE" : "OPEN"))
                .sourceEventType(event.getTipo() == null ? null : event.getTipo().name())
                .status(event.getTipo() == OperacionEvent.Tipo.CERRADA ? "CLOSED" : "OPEN")
                .sizeQty(firstNonNull(op.getSizeQty(), op.getSize()))
                .signedSizeQty(null)
                .notionalUsd(op.getNotionalUsd())
                .marginUsedUsd(op.getMarginUsedUsd())
                .entryPrice(op.getPrecioEntrada())
                .markPrice(op.getPrecioMercado())
                .exitPrice(op.getPrecioCierre())
                .sourceTs(eventTime)
                .eventTime(eventTime)
                .traceId(effectiveTraceId)
                .source(firstNonBlank(source, SOURCE_OPERATION_EVENT_INGEST))
                .reasonCode(reasonCode)
                .raw(rawFromOperacionEvent(event, source, reasonCode))
                .build();
    }

    private OperationMovementEventEntity toEntity(OperationMovementEventRecordCommand command, OperationMovementEventEntity previous) {
        OffsetDateTime now = utcNow();
        BigDecimal previousSize = positive(firstNonNull(command.getPreviousSizeQty(), previous == null ? null : previous.getResultingSizeQty()));
        BigDecimal resultingSize = resultingSize(command);
        BigDecimal deltaSize = command.getDeltaSizeQty();
        if (deltaSize == null && resultingSize != null && previousSize != null) {
            deltaSize = resultingSize.subtract(previousSize);
        }
        String eventType = classifyEvent(command, previousSize, resultingSize, deltaSize);
        BigDecimal realizedPnl = firstNonNull(command.getRealizedPnlUsd(), estimateRealizedPnl(command, previous, eventType, deltaSize));
        return OperationMovementEventEntity.builder()
                .idOrderOrigin(command.getIdOrderOrigin())
                .movementKey(command.getMovementKey())
                .idempotencyKey(command.getIdempotencyKey())
                .positionKey(command.getPositionKey())
                .idWalletOrigin(command.getIdWalletOrigin())
                .parsymbol(normalizeSymbol(command.getParsymbol()))
                .typeOperation(normalizeUpper(command.getTypeOperation(), "UNKNOWN"))
                .eventType(eventType)
                .deltaType(normalizeUpper(command.getDeltaType(), "UNKNOWN"))
                .sourceEventType(normalizeUpper(command.getSourceEventType(), null))
                .status(normalizeUpper(command.getStatus(), null))
                .sizeQty(command.getSizeQty())
                .signedSizeQty(command.getSignedSizeQty())
                .previousSizeQty(previousSize)
                .resultingSizeQty(resultingSize)
                .deltaSizeQty(deltaSize)
                .notionalUsd(command.getNotionalUsd())
                .marginUsedUsd(command.getMarginUsedUsd())
                .entryPrice(firstNonNull(command.getEntryPrice(), previous == null ? null : previous.getEntryPrice()))
                .markPrice(command.getMarkPrice())
                .exitPrice(firstNonNull(command.getExitPrice(), command.getMarkPrice()))
                .realizedPnlUsd(realizedPnl)
                .leverage(command.getLeverage())
                .walletVersion(command.getWalletVersion())
                .snapshotVersion(command.getSnapshotVersion())
                .sourceTs(command.getSourceTs())
                .detectedAt(command.getDetectedAt())
                .publishedAt(command.getPublishedAt())
                .eventTime(Objects.requireNonNullElse(command.getEventTime(), now))
                .traceId(safeTraceId(command))
                .source(firstNonBlank(command.getSource(), SOURCE_OPERATION_EVENT_INGEST))
                .reasonCode(command.getReasonCode())
                .copyEligibleUsers(command.getCopyEligibleUsers())
                .copySubmittedTasks(command.getCopySubmittedTasks())
                .copyBusinessSkipped(command.getCopyBusinessSkipped())
                .copyFallbackJobs(command.getCopyFallbackJobs())
                .copyFallbackUsed(command.getCopyFallbackUsed())
                .raw(command.getRaw())
                .dateCreation(now)
                .build();
    }

    private String classifyEvent(OperationMovementEventRecordCommand command, BigDecimal previousSize, BigDecimal resultingSize, BigDecimal deltaSize) {
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(command.getDeltaType());
        if (deltaType == HyperliquidDeltaType.FLIP) {
            return "FLIP";
        }
        if (isClosed(command)) {
            return "CLOSE";
        }
        if (deltaType == HyperliquidDeltaType.OPEN) {
            return "OPEN";
        }
        if (deltaSize != null) {
            int sign = deltaSize.compareTo(ZERO);
            if (sign > 0) {
                return isZero(previousSize) ? "OPEN" : "INCREASE";
            }
            if (sign < 0) {
                return isZero(resultingSize) ? "CLOSE" : "REDUCE";
            }
            return deltaType == HyperliquidDeltaType.NO_CHANGE ? "NO_CHANGE" : "UPDATE";
        }
        if (isZero(previousSize) && positive(resultingSize) != null && positive(resultingSize).compareTo(ZERO) > 0) {
            return "OPEN";
        }
        return normalizeUpper(command.getEventType(), "UNKNOWN");
    }

    private BigDecimal resultingSize(OperationMovementEventRecordCommand command) {
        if (command.getResultingSizeQty() != null) {
            return positive(command.getResultingSizeQty());
        }
        if (isClosed(command)) {
            return ZERO;
        }
        return positive(firstNonNull(command.getSizeQty(), command.getSignedSizeQty()));
    }

    private boolean isClosed(OperationMovementEventRecordCommand command) {
        String sourceEvent = normalizeUpper(command.getSourceEventType(), null);
        String status = normalizeUpper(command.getStatus(), null);
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(command.getDeltaType());
        return deltaType == HyperliquidDeltaType.CLOSE
                || "CERRADA".equals(sourceEvent)
                || "CLOSED".equals(status)
                || "CLOSE".equals(normalizeUpper(command.getEventType(), null));
    }

    private BigDecimal estimateRealizedPnl(OperationMovementEventRecordCommand command, OperationMovementEventEntity previous, String eventType, BigDecimal deltaSize) {
        if (!("REDUCE".equals(eventType) || "CLOSE".equals(eventType) || "FLIP".equals(eventType))) {
            return null;
        }
        BigDecimal previousSize = positive(previous == null ? command.getPreviousSizeQty() : previous.getResultingSizeQty());
        BigDecimal resultingSize = resultingSize(command);
        BigDecimal closedQty = null;
        if (deltaSize != null && deltaSize.compareTo(ZERO) < 0) {
            closedQty = deltaSize.abs();
        } else if (previousSize != null && resultingSize != null && previousSize.compareTo(resultingSize) > 0) {
            closedQty = previousSize.subtract(resultingSize).abs();
        }
        if (closedQty == null || closedQty.compareTo(ZERO) <= 0) {
            return null;
        }
        BigDecimal entry = firstNonNull(previous == null ? null : previous.getEntryPrice(), command.getEntryPrice());
        BigDecimal exit = firstNonNull(command.getExitPrice(), command.getMarkPrice(), command.getEntryPrice());
        if (entry == null || exit == null) {
            return null;
        }
        String side = normalizeUpper(command.getTypeOperation(), previous == null ? null : previous.getTypeOperation());
        BigDecimal unitPnl;
        if (PositionSide.SHORT.name().equals(side)) {
            unitPnl = entry.subtract(exit);
        } else {
            unitPnl = exit.subtract(entry);
        }
        return unitPnl.multiply(closedQty).setScale(CALC_SCALE, RoundingMode.HALF_UP);
    }

    private boolean isRecordable(OperationMovementEventRecordCommand command) {
        return command != null
                && command.getIdOrderOrigin() != null
                && StringUtils.hasText(command.getMovementKey())
                && StringUtils.hasText(command.getPositionKey())
                && StringUtils.hasText(command.getIdWalletOrigin())
                && StringUtils.hasText(command.getParsymbol())
                && StringUtils.hasText(command.getTypeOperation())
                && StringUtils.hasText(command.getDeltaType());
    }

    private JsonNode rawFromMapped(HyperliquidMappedDelta mappedDelta, HyperliquidDirectCopyDispatchResult dispatchResult, String reasonCode) {
        LinkedHashMap<String, Object> raw = new LinkedHashMap<>();
        raw.put("kind", "hyperliquid_direct_delta");
        raw.put("movementKeySchema", "canonical-v2-sha256");
        raw.put("idempotencyKeyAuditOnly", true);
        raw.put("idempotencyKey", mappedDelta.idempotencyKey());
        raw.put("positionKey", mappedDelta.positionKey());
        raw.put("wallet", mappedDelta.wallet());
        raw.put("symbol", mappedDelta.symbol());
        raw.put("side", mappedDelta.side());
        raw.put("deltaType", mappedDelta.deltaType());
        raw.put("request", mappedDelta.request());
        raw.put("dispatchResult", dispatchResult);
        raw.put("reasonCode", reasonCode);
        return objectMapper.valueToTree(raw);
    }

    private JsonNode rawFromOperacionEvent(OperacionEvent event, String source, String reasonCode) {
        LinkedHashMap<String, Object> raw = new LinkedHashMap<>();
        raw.put("kind", "operation_event");
        raw.put("movementKeySchema", "canonical-v2-sha256");
        raw.put("idempotencyKeyAuditOnly", true);
        raw.put("source", source);
        raw.put("event", event);
        raw.put("reasonCode", reasonCode);
        return objectMapper.valueToTree(raw);
    }

    private String buildMovementKey(HyperliquidMappedDelta mappedDelta, OffsetDateTime eventTime) {
        return canonicalMovementKey(canonicalMovementPayload(mappedDelta, eventTime));
    }

    private String buildMovementKey(OperacionEvent event, OffsetDateTime eventTime, String source) {
        return canonicalMovementKey(canonicalMovementPayload(event, eventTime, source));
    }

    private String canonicalMovementKey(String canonicalPayload) {
        return "movement|sha256:" + hashHex(canonicalPayload);
    }

    private String canonicalMovementPayload(HyperliquidMappedDelta mappedDelta, OffsetDateTime eventTime) {
        HyperliquidDeltaRequest req = mappedDelta == null ? null : mappedDelta.request();
        OperacionDto op = mappedDelta == null || mappedDelta.event() == null ? null : mappedDelta.event().getOperacion();
        String positionKey = firstNonBlank(mappedDelta == null ? null : mappedDelta.positionKey(), op == null ? null : buildPositionKey(op));
        String wallet = firstNonBlank(mappedDelta == null ? null : mappedDelta.wallet(), op == null ? null : op.getIdCuenta());
        String symbol = firstNonBlank(mappedDelta == null ? null : mappedDelta.symbol(), op == null ? null : op.getParSymbol());
        String side = firstNonBlank(mappedDelta == null ? null : mappedDelta.side(), req == null ? null : req.side(), op == null || op.getTipoOperacion() == null ? null : op.getTipoOperacion().name());
        String deltaType = firstNonBlank(mappedDelta == null ? null : mappedDelta.deltaType(), mappedDelta == null || mappedDelta.event() == null ? null : mappedDelta.event().getDeltaType(), req == null ? null : req.deltaType());
        String sourceEventType = firstNonBlank(req == null ? null : req.eventType(), mappedDelta == null || mappedDelta.event() == null || mappedDelta.event().getTipo() == null ? null : mappedDelta.event().getTipo().name());
        BigDecimal sizeQty = firstNonNull(req == null ? null : req.sizeQty(), req == null ? null : req.signedSizeQty(), op == null ? null : op.getSizeQty(), op == null ? null : op.getSize());
        BigDecimal notionalUsd = firstNonNull(req == null ? null : req.notionalUsd(), op == null ? null : op.getNotionalUsd());
        BigDecimal marginUsedUsd = firstNonNull(req == null ? null : req.marginUsedUsd(), op == null ? null : op.getMarginUsedUsd());
        BigDecimal tradePrice = firstNonNull(op == null ? null : op.getPrecioCierre(), req == null ? null : req.entryPrice(), op == null ? null : op.getPrecioEntrada());
        return String.join("|",
                "movement_v2",
                "origin=" + safePart(asString(op == null ? null : op.getIdOperacion())),
                "position=" + safePart(positionKey),
                "wallet=" + safePart(normalizeLower(wallet)),
                "symbol=" + safePart(normalizeSymbol(symbol)),
                "side=" + safePart(normalizeUpper(side, "UNKNOWN")),
                "delta=" + safePart(normalizeUpper(deltaType, "UNKNOWN")),
                "sourceEvent=" + safePart(normalizeUpper(sourceEventType, "UNKNOWN")),
                "status=" + safePart(normalizeUpper(req == null ? null : req.status(), "UNKNOWN")),
                "eventTime=" + timeKey(eventTime),
                "sizeQty=" + decimalKey(sizeQty),
                "notionalUsd=" + decimalKey(notionalUsd),
                "marginUsedUsd=" + decimalKey(marginUsedUsd),
                "tradePrice=" + decimalKey(tradePrice)
        );
    }

    private String canonicalMovementPayload(OperacionEvent event, OffsetDateTime eventTime, String source) {
        OperacionDto op = event == null ? null : event.getOperacion();
        String deltaType = firstNonBlank(event == null ? null : event.getDeltaType(), event == null || event.getTipo() == null ? null : event.getTipo() == OperacionEvent.Tipo.CERRADA ? "CLOSE" : "OPEN");
        return String.join("|",
                "movement_v2",
                "origin=" + safePart(asString(op == null ? null : op.getIdOperacion())),
                "position=" + safePart(buildPositionKey(op)),
                "wallet=" + safePart(normalizeLower(op == null ? null : op.getIdCuenta())),
                "symbol=" + safePart(normalizeSymbol(op == null ? null : op.getParSymbol())),
                "side=" + safePart(normalizeUpper(op == null || op.getTipoOperacion() == null ? null : op.getTipoOperacion().name(), "UNKNOWN")),
                "delta=" + safePart(normalizeUpper(deltaType, "UNKNOWN")),
                "sourceEvent=" + safePart(event == null || event.getTipo() == null ? "UNKNOWN" : event.getTipo().name()),
                "status=" + safePart(event != null && event.getTipo() == OperacionEvent.Tipo.CERRADA ? "CLOSED" : "OPEN"),
                "eventTime=" + timeKey(eventTime),
                "sizeQty=" + decimalKey(firstNonNull(op == null ? null : op.getSizeQty(), op == null ? null : op.getSize())),
                "notionalUsd=" + decimalKey(op == null ? null : op.getNotionalUsd()),
                "marginUsedUsd=" + decimalKey(op == null ? null : op.getMarginUsedUsd()),
                "tradePrice=" + decimalKey(firstNonNull(op == null ? null : op.getPrecioCierre(), op == null ? null : op.getPrecioEntrada()))
        );
    }

    private String buildPositionKey(OperacionDto op) {
        if (op == null) {
            return "operation-position:NA";
        }
        return String.join(":",
                "operation-position",
                normalizeLower(op.getIdCuenta()),
                normalizeLower(op.getParSymbol()),
                normalizeLower(op.getTipoOperacion() == null ? null : op.getTipoOperacion().name()));
    }

    private String currentOrOriginTraceId(UUID originId, String wallet, String symbol) {
        String current = MDC.get("traceId");
        if (StringUtils.hasText(current) && !"NA".equalsIgnoreCase(current)) {
            return current;
        }
        return CopyTraceIdUtil.copyTraceId(asString(originId), "origin", wallet, symbol);
    }

    private String safeTraceId(OperationMovementEventRecordCommand command) {
        return firstNonBlank(command == null ? null : command.getTraceId(), CopyTraceIdUtil.copyTraceId(asString(command == null ? null : command.getIdOrderOrigin()), "origin", command == null ? null : command.getIdWalletOrigin(), command == null ? null : command.getParsymbol()));
    }

    private boolean isUniqueViolation(DataIntegrityViolationException ex) {
        Throwable t = ex;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null) {
                String normalized = msg.toLowerCase(Locale.ROOT);
                if (normalized.contains("23505")
                        || normalized.contains("duplicate key value violates unique constraint")
                        || normalized.contains("ux_operation_movement_event_movement_key")) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }

    private void registerMetrics() {
        Gauge.builder("signals.operation_movement_event.queue.depth", queue, q -> q.size())
                .description("Operation movement ledger queue depth")
                .register(meterRegistry);
        Gauge.builder("signals.operation_movement_event.workers.active", activeWorkers, a -> a.get())
                .description("Operation movement ledger active workers")
                .register(meterRegistry);
        Gauge.builder("signals.operation_movement_event.persisted.total.gauge", persisted, a -> a.get())
                .description("Operation movement ledger persisted counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.operation_movement_event.failed.total.gauge", failed, a -> a.get())
                .description("Operation movement ledger failed counter gauge")
                .register(meterRegistry);
    }

    private OffsetDateTime fromEpochMillis(Long millis) {
        if (millis == null || millis <= 0) {
            return null;
        }
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    private OffsetDateTime fromInstant(Instant instant) {
        if (instant == null) {
            return null;
        }
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private OffsetDateTime utcNow() {
        return OffsetDateTime.now(ZoneOffset.UTC);
    }

    @SafeVarargs
    private static <T> T firstNonNull(T... values) {
        if (values == null) {
            return null;
        }
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private BigDecimal positive(BigDecimal value) {
        return value == null ? null : value.abs();
    }

    private boolean isZero(BigDecimal value) {
        return value == null || value.compareTo(ZERO) == 0;
    }

    private String normalizeUpper(String value, String fallback) {
        if (!StringUtils.hasText(value)) {
            return fallback;
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private String normalizeLower(String value) {
        if (!StringUtils.hasText(value)) {
            return "na";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private String normalizeSymbol(String symbol) {
        return normalizeUpper(symbol, "UNKNOWN");
    }


    private String timeKey(OffsetDateTime value) {
        return value == null ? "NA" : value.withOffsetSameInstant(ZoneOffset.UTC).toInstant().toString();
    }

    private String compactKey(String value) {
        String key = StringUtils.hasText(value) ? value.trim() : "operation-movement|missing";
        if (key.length() <= 600) {
            return key;
        }
        return key.substring(0, 520) + "|sha256:" + hashHex(key);
    }

    private String hashHex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException ex) {
            return String.format("%016x", input.hashCode());
        }
    }

    private String decimalKey(BigDecimal value) {
        return value == null ? "NA" : value.stripTrailingZeros().toPlainString();
    }

    private String safePart(String value) {
        if (!StringUtils.hasText(value)) {
            return "NA";
        }
        return value.trim().replace('|', '/').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
    }

    private String safe(String value) {
        if (!StringUtils.hasText(value)) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }

    private String safeTag(String value) {
        if (!StringUtils.hasText(value)) {
            return "unknown";
        }
        String clean = value.trim();
        return clean.length() > 64 ? clean.substring(0, 64) : clean;
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private record QueuedMovement(OperationMovementEventRecordCommand command, long acceptedNs) {
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(1);

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, prefix + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
