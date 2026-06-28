package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.entity.FuturesPositionEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.exception.HyperliquidOriginLifecycleException;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.repository.FuturesPositionRepository;
import com.apunto.engine.shared.enums.PositionStatus;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.util.CopyTraceIdUtil;
import com.apunto.engine.shared.util.CopyLogAdvice;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class HyperliquidOriginPositionStoreService {

    private static final String PLATFORM = "hyperliquid";
    private static final String VENUE = "HYPERLIQUID";
    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal ONE = BigDecimal.ONE;
    private static final int CALC_SCALE = 18;

    private final FuturesPositionRepository repository;
    private final ObjectMapper objectMapper;
    private final BinanceFuturesPriceNormalizerService priceNormalizerService;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;
    private final boolean enabled;
    private final boolean hydrateOpenPositions;
    private final boolean failCopyOnBindError;
    private final boolean skipLateAdjustments;
    private final int workerCount;
    private final AtomicBoolean[] workerSlots;
    private final BlockingQueue<PersistTask> queue;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicLong submitted = new AtomicLong(0);
    private final AtomicLong persisted = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);
    private final AtomicLong skipped = new AtomicLong(0);
    private final AtomicLong rejected = new AtomicLong(0);
    private final AtomicLong workerUncaughtFailures = new AtomicLong(0);
    private final AtomicLong workerRestarts = new AtomicLong(0);
    private final AtomicLong workerStops = new AtomicLong(0);
    private final AtomicLong queueHighWaterMark = new AtomicLong(0);
    private final AtomicLong maxQueueDelayMs = new AtomicLong(0);
    private final AtomicInteger recentQueueDelayIndex = new AtomicInteger(0);
    private final long[] recentQueueDelaysMs = new long[1024];
    private final ConcurrentMap<String, UUID> activeOriginIds = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Object> positionLocks = new ConcurrentHashMap<>();

    @PersistenceContext
    private EntityManager entityManager;

    public HyperliquidOriginPositionStoreService(
            FuturesPositionRepository repository,
            ObjectMapper objectMapper,
            BinanceFuturesPriceNormalizerService priceNormalizerService,
            PlatformTransactionManager transactionManager,
            MeterRegistry meterRegistry,
            @Value("${hyperliquid.direct-ingest.origin-store.enabled:true}") boolean enabled,
            @Value("${hyperliquid.direct-ingest.origin-store.worker-threads:2}") int workerThreads,
            @Value("${hyperliquid.direct-ingest.origin-store.queue-capacity:20000}") int queueCapacity,
            @Value("${hyperliquid.direct-ingest.origin-store.hydrate-open-positions:true}") boolean hydrateOpenPositions,
            @Value("${hyperliquid.direct-ingest.origin-store.fail-copy-on-bind-error:false}") boolean failCopyOnBindError,
            @Value("${hyperliquid.direct-ingest.origin-store.skip-late-adjustments:true}") boolean skipLateAdjustments
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.priceNormalizerService = priceNormalizerService;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.meterRegistry = meterRegistry;
        this.enabled = enabled;
        this.hydrateOpenPositions = hydrateOpenPositions;
        this.failCopyOnBindError = failCopyOnBindError;
        this.skipLateAdjustments = skipLateAdjustments;
        this.workerCount = Math.max(1, workerThreads);
        this.workerSlots = new AtomicBoolean[this.workerCount];
        for (int i = 0; i < this.workerSlots.length; i++) {
            this.workerSlots[i] = new AtomicBoolean(false);
        }
        this.queue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
        this.workers = Executors.newFixedThreadPool(this.workerCount, new NamedThreadFactory("hl-origin-store-"));
        registerMetrics();
        log.info("event=hyperliquid.origin_store.config enabled={} workerThreads={} queueCapacity={} hydrateOpenPositions={} failCopyOnBindError={} skipLateAdjustments={}",
                enabled, this.workerCount, Math.max(1, queueCapacity), hydrateOpenPositions, failCopyOnBindError, skipLateAdjustments);
    }

    @PostConstruct
    public void start() {
        if (!enabled) {
            log.warn("event=hyperliquid.origin_store.disabled");
            return;
        }
        hydrateActivePositionCache();
        running.set(true);
        for (int i = 0; i < workerCount; i++) {
            startWorker(i, "startup");
        }
        log.info("event=hyperliquid.origin_store.started queueCapacity={} expectedWorkers={} activeCacheSize={}", queue.remainingCapacity() + queue.size(), workerCount, activeOriginIds.size());
    }

    @Scheduled(fixedDelayString = "${hyperliquid.direct-ingest.origin-store.log-interval-ms:${hyperliquid.direct-ingest.log-interval-ms:10000}}")
    public void logMetrics() {
        if (!enabled) {
            return;
        }
        ensureWorkersHealthy("metrics");
        long p95QueueDelayMs = p95RecentQueueDelayMs();
        long maxDelay = maxQueueDelayMs.get();
        int queueDepth = queue.size();
        int queueCapacity = queue.remainingCapacity() + queueDepth;
        long highWaterMark = queueHighWaterMark.get();
        log.info("event=hyperliquid.origin_store.metrics queueDepth={} queueCapacity={} activeWorkers={} expectedWorkers={} workerRestarts={} workerStops={} workerUncaughtFailures={} submitted={} persisted={} skipped={} failed={} rejected={} activeCacheSize={} p95QueueDelayMs={} maxQueueDelayMs={} queueHighWaterMark={}",
                queueDepth, queueCapacity, activeWorkers.get(), workerCount, workerRestarts.get(), workerStops.get(), workerUncaughtFailures.get(), submitted.get(), persisted.get(), skipped.get(), failed.get(), rejected.get(), activeOriginIds.size(), p95QueueDelayMs, maxDelay, highWaterMark);
        if (queueCapacity > 0 && queueDepth >= Math.max(1, (int) (queueCapacity * 0.80d))) {
            log.warn("event=hyperliquid.origin_store.queue_pressure reasonCode=origin_store_queue_pressure queueDepth={} queueCapacity={} activeWorkers={} expectedWorkers={} p95QueueDelayMs={} maxQueueDelayMs={} rejected={} action=scale_workers_or_reduce_blocking_io",
                    queueDepth, queueCapacity, activeWorkers.get(), workerCount, p95QueueDelayMs, maxDelay, rejected.get());
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        workers.shutdownNow();
        log.info("event=hyperliquid.origin_store.stopped queueDepth={} submitted={} persisted={} skipped={} failed={} rejected={} activeCacheSize={}",
                queue.size(), submitted.get(), persisted.get(), skipped.get(), failed.get(), rejected.get(), activeOriginIds.size());
    }

    public HyperliquidMappedDelta bindOriginIdForCopy(HyperliquidMappedDelta mapped) {
        if (mapped == null || mapped.event() == null || mapped.event().getOperacion() == null) {
            if (failCopyOnBindError) {
                throw new HyperliquidOriginLifecycleException(
                        "Mapped hyperliquid delta is required",
                        Map.of("reason", "mapped_delta_missing")
                );
            }
            return mapped;
        }
        if (!enabled) {
            return mapped;
        }
        UUID originId = resolveOriginId(mapped);
        return mapped.withOriginId(originId);
    }

    public void submitAfterCopy(HyperliquidMappedDelta mapped, HyperliquidDirectCopyDispatchResult dispatchResult) {
        if (!enabled || mapped == null) {
            return;
        }
        if (shouldSkipLateAdjustment(mapped)) {
            skipped.incrementAndGet();
            meterRegistry.counter("signals.hyperliquid.origin_store.skipped.total", "reason", "late_adjustment_without_active_origin").increment();
            log.info("event=hyperliquid.origin_store.skipped category=origin_position reasonCode=late_adjustment_without_active_origin reasonAlias=adjustment_without_active_origin friendlyReason=ajuste_sin_posicion_original_activa explanation=ajuste_no_guardado_porque_no_existe_posicion_original_activa copyImpact=no_copy_order originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} skipLateAdjustments={} activeCacheSize={} queueDepth={} {}",
                    originId(mapped),
                    safeLog(mapped.idempotencyKey()),
                    safeLog(mapped.positionKey()),
                    safeLog(mapped.wallet()),
                    safeLog(mapped.symbol()),
                    safeLog(mapped.side()),
                    safeLog(mapped.deltaType()),
                    skipLateAdjustments,
                    activeOriginIds.size(),
                    queue.size(),
                    CopyLogAdvice.fields("late_adjustment_without_active_origin", CopyLogAdvice.context(dispatchResult == null ? null : dispatchResult.eligibleUsers(), dispatchResult == null ? null : dispatchResult.eligibleUsers(), dispatchResult == null ? null : dispatchResult.submittedTasks(), dispatchResult == null ? null : dispatchResult.businessSkipped(), queue.size(), null, activeOriginIds.size(), "origin_store_pre_queue")));
            return;
        }
        PersistTask task = new PersistTask(mapped, dispatchResult, System.nanoTime());
        if (!queue.offer(task)) {
            rejected.incrementAndGet();
            meterRegistry.counter("signals.hyperliquid.origin_store.rejected.total", "reason", "queue_full").increment();
            log.error("event=hyperliquid.origin_store.rejected reasonCode=queue_full reason=queue_full originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} queueDepth={} {}",
                    originId(mapped), safeLog(mapped.idempotencyKey()), safeLog(mapped.positionKey()), safeLog(mapped.wallet()), safeLog(mapped.symbol()), safeLog(mapped.side()), safeLog(mapped.deltaType()), queue.size(),
                    CopyLogAdvice.fields("queue_full", CopyLogAdvice.context(dispatchResult == null ? null : dispatchResult.eligibleUsers(), dispatchResult == null ? null : dispatchResult.eligibleUsers(), dispatchResult == null ? null : dispatchResult.submittedTasks(), dispatchResult == null ? null : dispatchResult.businessSkipped(), queue.size(), null, activeOriginIds.size(), "origin_store")));
            return;
        }
        submitted.incrementAndGet();
        recordQueueHighWater(queue.size());
        ensureWorkersHealthy("submit_after_copy");
        meterRegistry.counter("signals.hyperliquid.origin_store.submitted.total", "deltaType", safeTag(mapped.deltaType())).increment();
    }

    private UUID resolveOriginId(HyperliquidMappedDelta mapped) {
        String key = requireText(mapped.positionKey(), "positionKey is required");
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(mapped.deltaType());
        boolean closing = isClosing(mapped);
        if (closing) {
            UUID existing = activeOriginIds.get(key);
            if (existing != null) {
                return existing;
            }
            UUID databaseOriginId = findOpenOriginIdForActivePosition(mapped);
            if (databaseOriginId != null) {
                cacheRecoveredLifecycle(key, databaseOriginId, mapped, "db_open_position");
                return databaseOriginId;
            }
            UUID fallback = fallbackLifecycleId(mapped);
            log.warn("event=hyperliquid.origin_store.lifecycle_missing category=origin_position action=close_fallback reasonCode=close_without_active_origin reasonAlias=close_without_active_origin friendlyReason=cierre_sin_posicion_original_activa explanation=cierre_recibido_sin_posicion_original_activa_registrada copyImpact=close_fallback originId={} positionKey={} wallet={} symbol={} side={} deltaType={} {}",
                    fallback, safeLog(mapped.positionKey()), safeLog(mapped.wallet()), safeLog(mapped.symbol()), safeLog(mapped.side()), safeLog(mapped.deltaType()),
                    CopyLogAdvice.fields("close_without_active_origin", CopyLogAdvice.context(null, null, null, null, queue.size(), null, activeOriginIds.size(), "origin_store_bind")));
            return fallback;
        }

        if (deltaType.canStartCopyLifecycle() || deltaType == HyperliquidDeltaType.FLIP) {
            UUID lifecycleId = activeOriginIds.computeIfAbsent(key, ignored -> UUID.randomUUID());
            if (deltaType == HyperliquidDeltaType.FLIP) {
                log.info("event=hyperliquid.origin_store.flip_lifecycle_start category=origin_position reasonCode=flip_starts_new_side reasonAlias=flip_starts_new_side friendlyReason=flip_inicia_nuevo_lado explanation=el_FLIP_se_persiste_como_nueva_posicion_y_cierra_el_lado_anterior_si_existe copyImpact=origin_state_consistent originId={} positionKey={} wallet={} symbol={} side={} deltaType={} {}",
                        lifecycleId, safeLog(mapped.positionKey()), safeLog(mapped.wallet()), safeLog(mapped.symbol()), safeLog(mapped.side()), safeLog(mapped.deltaType()),
                        CopyLogAdvice.fields("flip_starts_new_side", CopyLogAdvice.context(null, null, null, null, queue.size(), null, activeOriginIds.size(), "origin_store_bind")));
            }
            return lifecycleId;
        }

        if (deltaType.canAdjustExistingCopy()) {
            UUID existing = activeOriginIds.get(key);
            if (existing != null) {
                return existing;
            }
            UUID databaseOriginId = findOpenOriginIdForActivePosition(mapped);
            if (databaseOriginId != null) {
                cacheRecoveredLifecycle(key, databaseOriginId, mapped, "db_open_position");
                return databaseOriginId;
            }
            UUID fallback = originId(mapped) == null ? fallbackLifecycleId(mapped) : originId(mapped);
            log.info("event=hyperliquid.origin_store.lifecycle_missing category=origin_position action=adjustment_skip reasonCode=late_adjustment_without_active_origin reasonAlias=adjustment_without_active_origin friendlyReason=ajuste_sin_posicion_original_activa explanation=ajuste_recibido_sin_posicion_original_activa_no_es_demora_de_websocket copyImpact=no_copy_order originId={} positionKey={} wallet={} symbol={} side={} deltaType={} skipLateAdjustments={} {}",
                    fallback, safeLog(mapped.positionKey()), safeLog(mapped.wallet()), safeLog(mapped.symbol()), safeLog(mapped.side()), safeLog(mapped.deltaType()), skipLateAdjustments,
                    CopyLogAdvice.fields("late_adjustment_without_active_origin", CopyLogAdvice.context(null, null, null, null, queue.size(), null, activeOriginIds.size(), "origin_store_bind")));
            return fallback;
        }

        UUID fallback = originId(mapped) == null ? fallbackLifecycleId(mapped) : originId(mapped);
        log.info("event=hyperliquid.origin_store.lifecycle_missing category=origin_position action=non_copyable_skip reasonCode=delta_not_lifecycle_start reasonAlias=delta_not_lifecycle_start friendlyReason=evento_no_inicia_posicion_original explanation=delta_no_puede_crear_lifecycle_por_regla_de_negocio copyImpact=no_copy_order originId={} positionKey={} wallet={} symbol={} side={} deltaType={} {}",
                fallback, safeLog(mapped.positionKey()), safeLog(mapped.wallet()), safeLog(mapped.symbol()), safeLog(mapped.side()), safeLog(mapped.deltaType()),
                CopyLogAdvice.fields("delta_not_lifecycle_start", CopyLogAdvice.context(null, null, null, null, queue.size(), null, activeOriginIds.size(), "origin_store_bind")));
        return fallback;
    }

    private void cacheRecoveredLifecycle(String key, UUID originId, HyperliquidMappedDelta mapped, String source) {
        activeOriginIds.put(key, originId);
        log.info("event=hyperliquid.origin_store.lifecycle_recovered originId={} positionKey={} wallet={} symbol={} side={} deltaType={} source={}",
                originId, safeLog(mapped.positionKey()), safeLog(mapped.wallet()), safeLog(mapped.symbol()), safeLog(mapped.side()), safeLog(mapped.deltaType()), source);
    }

    private UUID findOpenOriginIdForActivePosition(HyperliquidMappedDelta mapped) {
        String accountId = lower(firstNonBlank(mapped.wallet(), null));
        String symbol = firstNonBlank(mapped.symbol(), null);
        String side = safeUpper(mapped.side());
        if (accountId == null || symbol == null || side == null || "UNKNOWN".equals(side)) {
            return null;
        }
        try {
            return repository.findLatestActiveByPlatformAccountSymbolSide(
                            PLATFORM,
                            accountId,
                            symbol,
                            side,
                            PositionStatus.OPEN.name()
                    )
                    .map(FuturesPositionEntity::getIdFuturesPosition)
                    .orElse(null);
        } catch (DataAccessException | IllegalArgumentException | IllegalStateException ex) {
            log.warn("event=hyperliquid.origin_store.lifecycle_lookup_failed positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\"",
                    safeLog(mapped.positionKey()), safeLog(mapped.wallet()), safeLog(mapped.symbol()), safeLog(mapped.side()), safeLog(mapped.deltaType()),
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()));
            return null;
        }
    }

    private boolean shouldSkipLateAdjustment(HyperliquidMappedDelta mapped) {
        if (!skipLateAdjustments || mapped == null || isClosing(mapped)) {
            return false;
        }
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(mapped.deltaType());
        if (deltaType == HyperliquidDeltaType.FLIP) {
            return false;
        }
        if (!deltaType.canAdjustExistingCopy()) {
            return false;
        }
        String key = mapped.positionKey();
        if (key != null && activeOriginIds.containsKey(key)) {
            return false;
        }
        UUID databaseOriginId = findOpenOriginIdForActivePosition(mapped);
        if (databaseOriginId != null) {
            if (key != null) {
                cacheRecoveredLifecycle(key, databaseOriginId, mapped, "db_open_position_before_persist");
            }
            return false;
        }
        return true;
    }

    private void startWorker(int workerIndex, String reason) {
        if (!enabled || !running.get()) {
            return;
        }
        if (workerIndex < 0 || workerIndex >= workerSlots.length) {
            return;
        }
        AtomicBoolean slot = workerSlots[workerIndex];
        if (!slot.compareAndSet(false, true)) {
            return;
        }
        if (!"startup".equals(reason)) {
            workerRestarts.incrementAndGet();
        }
        try {
            workers.execute(() -> workerLoop(workerIndex, reason));
        } catch (RuntimeException ex) {
            slot.set(false);
            log.error("event=hyperliquid.origin_store.worker_start_failed reasonCode=origin_store_worker_start_failed workerIndex={} reason={} errClass={} errMsg=\"{}\" queueDepth={} activeWorkers={}",
                    workerIndex, safeLog(reason), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), queue.size(), activeWorkers.get(), ex);
        }
    }

    private void ensureWorkersHealthy(String reason) {
        if (!enabled || !running.get()) {
            return;
        }
        for (int i = 0; i < workerSlots.length; i++) {
            if (!workerSlots[i].get()) {
                log.warn("event=hyperliquid.origin_store.worker_missing reasonCode=origin_store_worker_missing workerIndex={} reason={} queueDepth={} activeWorkers={} expectedWorkers={} action=restart_worker",
                        i, safeLog(reason), queue.size(), activeWorkers.get(), workerCount);
                startWorker(i, reason);
            }
        }
    }

    private void workerLoop(int workerIndex, String startReason) {
        activeWorkers.incrementAndGet();
        log.info("event=hyperliquid.origin_store.worker_started workerIndex={} startReason={} queueDepth={} activeWorkers={} expectedWorkers={}",
                workerIndex, safeLog(startReason), queue.size(), activeWorkers.get(), workerCount);
        try {
            while (running.get() || !queue.isEmpty()) {
                PersistTask task = queue.poll(250, TimeUnit.MILLISECONDS);
                if (task != null) {
                    processSafely(task, workerIndex);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.info("event=hyperliquid.origin_store.worker_interrupted workerIndex={} queueDepth={} activeWorkers={}",
                    workerIndex, queue.size(), activeWorkers.get());
        } finally {
            workerStops.incrementAndGet();
            int remainingWorkers = activeWorkers.decrementAndGet();
            workerSlots[workerIndex].set(false);
            log.warn("event=hyperliquid.origin_store.worker_stopped workerIndex={} queueDepth={} activeWorkers={} expectedWorkers={} running={}",
                    workerIndex, queue.size(), remainingWorkers, workerCount, running.get());
        }
    }

    private void processSafely(PersistTask task, int workerIndex) {
        try {
            process(task);
        } catch (Throwable ex) {
            if (isDuplicateKey(ex)) {
                HyperliquidMappedDelta mapped = task == null ? null : task.mappedDelta();
                long startedNs = System.nanoTime();
                long queueDelayMs = task == null ? 0L : elapsedMs(task.acceptedNs());
                handleDuplicateOriginPosition(mapped, task == null ? null : task.dispatchResult(), startedNs, queueDelayMs, null, ex, "worker_guard");
                return;
            }
            failed.incrementAndGet();
            workerUncaughtFailures.incrementAndGet();
            HyperliquidMappedDelta mapped = task == null ? null : task.mappedDelta();
            String deltaType = mapped == null ? null : mapped.deltaType();
            meterRegistry.counter("signals.hyperliquid.origin_store.worker_uncaught.total", "deltaType", safeTag(deltaType)).increment();
            log.error("event=hyperliquid.origin_store.worker_uncaught reasonCode=origin_store_worker_uncaught workerIndex={} originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" queueDepth={} activeWorkers={} expectedWorkers={} action=keep_worker_alive",
                    workerIndex,
                    originId(mapped),
                    mapped == null ? "NA" : safeLog(mapped.idempotencyKey()),
                    mapped == null ? "NA" : safeLog(mapped.positionKey()),
                    mapped == null ? "NA" : safeLog(mapped.wallet()),
                    mapped == null ? "NA" : safeLog(mapped.symbol()),
                    mapped == null ? "NA" : safeLog(mapped.side()),
                    mapped == null ? "NA" : safeLog(mapped.deltaType()),
                    ex.getClass().getSimpleName(),
                    safeLog(ex.getMessage()),
                    queue.size(),
                    activeWorkers.get(),
                    workerCount,
                    ex);
            if (ex instanceof VirtualMachineError) {
                throw (VirtualMachineError) ex;
            }
            if (ex instanceof ThreadDeath) {
                throw (ThreadDeath) ex;
            }
        }
    }

    private void process(PersistTask task) {
        long startedNs = System.nanoTime();
        HyperliquidMappedDelta mapped = task.mappedDelta();
        long queueDelayMs = elapsedMs(task.acceptedNs());
        recordQueueDelay(queueDelayMs);
        BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference = null;
        try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", originTraceId(mapped))) {
            if (shouldSkipLateAdjustment(mapped)) {
                skipped.incrementAndGet();
                meterRegistry.counter("signals.hyperliquid.origin_store.skipped.total", "reason", "late_adjustment_without_active_origin_after_queue").increment();
                log.info("event=hyperliquid.origin_store.skipped category=origin_position reasonCode=late_adjustment_without_active_origin_after_queue reasonAlias=adjustment_without_active_origin_after_queue friendlyReason=ajuste_sin_posicion_original_activa_luego_de_cola explanation=evento_entro_a_cola_pero_al_persistir_seguia_sin_posicion_original_activa copyImpact=no_copy_order originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} skipLateAdjustments={} activeCacheSize={} queueDelayMs={} elapsedMs={} queueDepth={} {}",
                        originId(mapped),
                        safeLog(mapped.idempotencyKey()),
                        safeLog(mapped.positionKey()),
                        safeLog(mapped.wallet()),
                        safeLog(mapped.symbol()),
                        safeLog(mapped.side()),
                        safeLog(mapped.deltaType()),
                        skipLateAdjustments,
                        activeOriginIds.size(),
                        queueDelayMs,
                        elapsedMs(startedNs),
                        queue.size(),
                        CopyLogAdvice.fields("late_adjustment_without_active_origin_after_queue", CopyLogAdvice.context(task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().submittedTasks(), task.dispatchResult() == null ? null : task.dispatchResult().businessSkipped(), queue.size(), null, activeOriginIds.size(), "origin_store_after_queue")));
                return;
            }
            priceReference = resolvePriceReference(mapped);
            String lockKey = lockKey(mapped);
            Object lock = positionLocks.computeIfAbsent(lockKey, ignoredLock -> new Object());
            PersistOutcome outcome;
            final BinanceFuturesPriceNormalizerService.BinancePriceReference priceReferenceForPersist = priceReference;
            synchronized (lock) {
                outcome = transactionTemplate.execute(status -> persistLifecycle(mapped, task.dispatchResult(), startedNs, priceReferenceForPersist));
            }
            if (outcome != null && outcome.status() != PositionStatus.OPEN) {
                positionLocks.remove(lockKey, lock);
            }
            persisted.incrementAndGet();
            meterRegistry.timer("signals.hyperliquid.origin_store.persist.duration", Tags.of("result", "ok", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            String copySkipReasonCode = task.dispatchResult() == null ? null : task.dispatchResult().reasonCode();
            String copySkipDiagnostic = copySkipReasonCode == null ? "" : CopyLogAdvice.fields(
                    copySkipReasonCode,
                    CopyLogAdvice.context(task.dispatchResult().eligibleUsers(), task.dispatchResult().eligibleUsers(), task.dispatchResult().submittedTasks(), task.dispatchResult().businessSkipped(), queue.size(), null, activeOriginIds.size(), "origin_store_upsert")
            );
            log.info("event=futures_position.origin_upsert_ok action={} originId={} created={} platform={} wallet={} symbol={} side={} status={} deltaType={} priceSource={} binanceSymbol={} binancePrice={} copySkipReasonCode={} queueDelayMs={} elapsedMs={} queueDepth={} {}",
                    outcome == null ? "unknown" : outcome.action(),
                    originId(mapped),
                    outcome != null && outcome.created(),
                    PLATFORM,
                    safeLog(mapped.wallet()),
                    safeLog(mapped.symbol()),
                    safeLog(mapped.side()),
                    outcome == null ? "NA" : outcome.status(),
                    safeLog(mapped.deltaType()),
                    outcome == null ? "NA" : safeLog(outcome.priceSource()),
                    outcome == null ? "NA" : safeLog(outcome.binanceSymbol()),
                    outcome == null ? "NA" : outcome.binancePrice(),
                    safeLog(copySkipReasonCode),
                    queueDelayMs,
                    elapsedMs(startedNs),
                    queue.size(),
                    copySkipDiagnostic);
        } catch (DataIntegrityViolationException ex) {
            if (isDuplicateKey(ex)) {
                handleDuplicateOriginPosition(mapped, task.dispatchResult(), startedNs, queueDelayMs, priceReference, ex, "data_integrity");
                return;
            }
            failed.incrementAndGet();
            MDC.put("traceId", originTraceId(mapped));
            meterRegistry.timer("signals.hyperliquid.origin_store.persist.duration", Tags.of("result", "error", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            log.error("event=futures_position.origin_upsert_failed reasonCode=origin_upsert_failed originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={} {}",
                    originId(mapped),
                    safeLog(mapped.idempotencyKey()),
                    safeLog(mapped.positionKey()),
                    safeLog(mapped.wallet()),
                    safeLog(mapped.symbol()),
                    safeLog(mapped.side()),
                    safeLog(mapped.deltaType()),
                    ex.getClass().getSimpleName(),
                    safeLog(ex.getMessage()),
                    queueDelayMs,
                    elapsedMs(startedNs),
                    queue.size(),
                    CopyLogAdvice.fields("origin_upsert_failed", CopyLogAdvice.context(task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().submittedTasks(), task.dispatchResult() == null ? null : task.dispatchResult().businessSkipped(), queue.size(), null, activeOriginIds.size(), "origin_store_upsert")));
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
            if (isDuplicateKey(ex)) {
                handleDuplicateOriginPosition(mapped, task.dispatchResult(), startedNs, queueDelayMs, priceReference, ex, "runtime_guard");
                return;
            }
            failed.incrementAndGet();
            MDC.put("traceId", originTraceId(mapped));
            meterRegistry.timer("signals.hyperliquid.origin_store.persist.duration", Tags.of("result", "error", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            log.error("event=futures_position.origin_upsert_failed reasonCode=origin_upsert_failed originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={} {}",
                    originId(mapped),
                    safeLog(mapped.idempotencyKey()),
                    safeLog(mapped.positionKey()),
                    safeLog(mapped.wallet()),
                    safeLog(mapped.symbol()),
                    safeLog(mapped.side()),
                    safeLog(mapped.deltaType()),
                    ex.getClass().getSimpleName(),
                    safeLog(ex.getMessage()),
                    queueDelayMs,
                    elapsedMs(startedNs),
                    queue.size(),
                    CopyLogAdvice.fields("origin_upsert_failed", CopyLogAdvice.context(task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().submittedTasks(), task.dispatchResult() == null ? null : task.dispatchResult().businessSkipped(), queue.size(), null, activeOriginIds.size(), "origin_store_upsert")));
        } catch (RuntimeException ex) {
            if (isDuplicateKey(ex)) {
                handleDuplicateOriginPosition(mapped, task.dispatchResult(), startedNs, queueDelayMs, priceReference, ex, "runtime_guard");
                return;
            }
            failed.incrementAndGet();
            MDC.put("traceId", originTraceId(mapped));
            meterRegistry.timer("signals.hyperliquid.origin_store.persist.duration", Tags.of("result", "error", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            log.error("event=futures_position.origin_upsert_failed reasonCode=origin_upsert_failed originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={} {}",
                    originId(mapped),
                    safeLog(mapped.idempotencyKey()),
                    safeLog(mapped.positionKey()),
                    safeLog(mapped.wallet()),
                    safeLog(mapped.symbol()),
                    safeLog(mapped.side()),
                    safeLog(mapped.deltaType()),
                    ex.getClass().getSimpleName(),
                    safeLog(ex.getMessage()),
                    queueDelayMs,
                    elapsedMs(startedNs),
                    queue.size(),
                    CopyLogAdvice.fields("origin_upsert_failed", CopyLogAdvice.context(task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().submittedTasks(), task.dispatchResult() == null ? null : task.dispatchResult().businessSkipped(), queue.size(), null, activeOriginIds.size(), "origin_store_upsert")));
        } finally {
            MDC.remove("traceId");
        }
    }

    private void handleDuplicateOriginPosition(
            HyperliquidMappedDelta mapped,
            HyperliquidDirectCopyDispatchResult dispatchResult,
            long startedNs,
            long queueDelayMs,
            BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference,
            Throwable ex,
            String source
    ) {
        boolean recovered = recoverDuplicateByUpdate(mapped, dispatchResult, startedNs, priceReference);
        recoverActiveOriginAfterDuplicate(mapped);
        if (recovered) {
            persisted.incrementAndGet();
        } else {
            skipped.incrementAndGet();
        }
        meterRegistry.counter("signals.hyperliquid.origin_store.skipped.total", "reason", "duplicate_origin_position").increment();
        meterRegistry.counter("signals.hyperliquid.origin_store.idempotent.total", "result", recovered ? "updated" : "ignored").increment();
        log.info("event=hyperliquid.origin_store.position_upsert_idempotent reasonCode=POSITION_ALREADY_EXISTS action=update_or_ignore impact=origin_store_consistent source={} recovered={} positionId={} originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} priceSource={} binanceSymbol={} priceUsed={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={}",
                safeLog(source),
                recovered,
                originId(mapped),
                originId(mapped),
                mapped == null ? "NA" : safeLog(mapped.idempotencyKey()),
                mapped == null ? "NA" : safeLog(mapped.positionKey()),
                mapped == null ? "NA" : safeLog(mapped.wallet()),
                mapped == null ? "NA" : safeLog(mapped.symbol()),
                mapped == null ? "NA" : safeLog(mapped.side()),
                mapped == null ? "NA" : safeLog(mapped.deltaType()),
                priceReference == null ? "NA" : safeLog(priceReference.source()),
                priceReference == null ? "NA" : safeLog(priceReference.symbol()),
                priceReference == null ? null : priceReference.price(),
                ex == null ? "NA" : ex.getClass().getSimpleName(),
                ex == null ? "NA" : safeLog(ex.getMessage()),
                queueDelayMs,
                elapsedMs(startedNs),
                queue.size());
    }

    private boolean recoverDuplicateByUpdate(
            HyperliquidMappedDelta mapped,
            HyperliquidDirectCopyDispatchResult dispatchResult,
            long startedNs,
            BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference
    ) {
        try {
            Boolean recovered = transactionTemplate.execute(status -> {
                if (mapped == null || mapped.event() == null || mapped.event().getOperacion() == null) {
                    return false;
                }
                OperacionDto operation = mapped.event().getOperacion();
                UUID id = operation.getIdOperacion();
                if (id == null) {
                    return false;
                }
                FuturesPositionEntity entity = repository.findByIdFuturesPosition(id).orElse(null);
                if (entity == null) {
                    return false;
                }
                if (shouldIgnoreNonClosingAfterClose(entity, mapped)) {
                    activeOriginIds.remove(mapped.positionKey(), entity.getIdFuturesPosition());
                    return true;
                }
                closeOppositeSideWhenFlip(mapped, operation, priceReference);
                boolean closing = isClosing(mapped);
                applyCommonFields(entity, mapped, operation, priceReference, dispatchResult, closing);
                applyStatusFields(entity, mapped.event(), operation, priceReference);
                FuturesPositionEntity saved = repository.saveAndFlush(entity);
                if (saved.getStatus() != PositionStatus.OPEN) {
                    activeOriginIds.remove(mapped.positionKey(), saved.getIdFuturesPosition());
                } else {
                    activeOriginIds.put(mapped.positionKey(), saved.getIdFuturesPosition());
                }
                return true;
            });
            return Boolean.TRUE.equals(recovered);
        } catch (RuntimeException recoverEx) {
            log.warn("event=hyperliquid.origin_store.position_upsert_idempotent_recover_failed reasonCode=POSITION_ALREADY_EXISTS originId={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" elapsedMs={}",
                    originId(mapped),
                    mapped == null ? "NA" : safeLog(mapped.positionKey()),
                    mapped == null ? "NA" : safeLog(mapped.wallet()),
                    mapped == null ? "NA" : safeLog(mapped.symbol()),
                    mapped == null ? "NA" : safeLog(mapped.side()),
                    mapped == null ? "NA" : safeLog(mapped.deltaType()),
                    recoverEx.getClass().getSimpleName(),
                    safeLog(recoverEx.getMessage()),
                    elapsedMs(startedNs));
            return false;
        }
    }


    private boolean isDuplicateKey(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String lower = message.toLowerCase(Locale.ROOT);
                if (lower.contains("duplicate key")
                        || lower.contains("unique constraint")
                        || lower.contains("futures_position_pkey")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private void recoverActiveOriginAfterDuplicate(HyperliquidMappedDelta mapped) {
        UUID id = originId(mapped);
        if (id == null) {
            return;
        }
        try {
            repository.findByIdFuturesPosition(id).ifPresent(entity -> {
                String key = mapped == null || mapped.positionKey() == null || mapped.positionKey().isBlank()
                        ? "origin-id:" + id
                        : mapped.positionKey();
                if (entity.getStatus() == PositionStatus.OPEN) {
                    activeOriginIds.put(key, id);
                } else {
                    activeOriginIds.remove(key, id);
                }
            });
        } catch (RuntimeException recoverEx) {
            log.warn("event=hyperliquid.origin_store.duplicate_recover_failed originId={} positionKey={} errClass={} errMsg=\"{}\"",
                    id, mapped == null ? "NA" : safeLog(mapped.positionKey()), recoverEx.getClass().getSimpleName(), safeLog(recoverEx.getMessage()));
        }
    }

    private void registerMetrics() {
        Gauge.builder("signals.hyperliquid.origin_store.queue.depth", queue, BlockingQueue::size)
                .description("Hyperliquid origin store queue depth")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.queue.high_water_mark", queueHighWaterMark, AtomicLong::get)
                .description("Hyperliquid origin store queue high-water mark")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.workers.active", activeWorkers, AtomicInteger::get)
                .description("Hyperliquid origin store active worker loops")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.workers.expected", this, svc -> svc.workerCount)
                .description("Hyperliquid origin store expected worker loops")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.workers.uncaught_failures", workerUncaughtFailures, AtomicLong::get)
                .description("Hyperliquid origin store uncaught worker failures")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.workers.restarts", workerRestarts, AtomicLong::get)
                .description("Hyperliquid origin store self-healed worker restarts")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.submitted.total.gauge", submitted, AtomicLong::get)
                .description("Hyperliquid origin store submitted counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.persisted.total.gauge", persisted, AtomicLong::get)
                .description("Hyperliquid origin store persisted counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.failed.total.gauge", failed, AtomicLong::get)
                .description("Hyperliquid origin store failed counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.origin_store.rejected.total.gauge", rejected, AtomicLong::get)
                .description("Hyperliquid origin store rejected counter gauge")
                .register(meterRegistry);
    }

    private void recordQueueHighWater(int depth) {
        if (depth > 0) {
            queueHighWaterMark.accumulateAndGet(depth, Math::max);
        }
    }

    private void recordQueueDelay(long queueDelayMs) {
        if (queueDelayMs < 0) {
            return;
        }
        int index = Math.floorMod(recentQueueDelayIndex.getAndIncrement(), recentQueueDelaysMs.length);
        recentQueueDelaysMs[index] = queueDelayMs;
        maxQueueDelayMs.accumulateAndGet(queueDelayMs, Math::max);
    }

    private long p95RecentQueueDelayMs() {
        long[] snapshot = recentQueueDelaysMs.clone();
        int count = 0;
        for (long value : snapshot) {
            if (value > 0) {
                count++;
            }
        }
        if (count == 0) {
            return 0L;
        }
        long[] values = new long[count];
        int i = 0;
        for (long value : snapshot) {
            if (value > 0) {
                values[i++] = value;
            }
        }
        Arrays.sort(values);
        int idx = Math.min(values.length - 1, (int) Math.ceil(values.length * 0.95) - 1);
        return values[Math.max(0, idx)];
    }

    private String originTraceId(HyperliquidMappedDelta mapped) {
        UUID id = originId(mapped);
        String wallet = mapped == null ? null : mapped.wallet();
        String symbol = mapped == null ? null : mapped.symbol();
        if (mapped != null && mapped.event() != null && mapped.event().getOperacion() != null) {
            wallet = firstNonBlank(wallet, mapped.event().getOperacion().getIdCuenta(), "NA");
            symbol = firstNonBlank(symbol, mapped.event().getOperacion().getParSymbol(), "NA");
        }
        return CopyTraceIdUtil.copyTraceId(id == null ? "NA" : id.toString(), "origin", wallet, symbol);
    }

    private BinanceFuturesPriceNormalizerService.BinancePriceReference resolvePriceReference(HyperliquidMappedDelta mapped) {
        if (mapped == null || mapped.event() == null || mapped.event().getOperacion() == null) {
            return null;
        }
        OperacionDto operation = mapped.event().getOperacion();
        BigDecimal eventPrice = eventPriceFallback(operation, isClosing(mapped));
        String rawSymbol = firstNonBlank(operation.getParSymbol(), mapped.symbol(), "UNKNOWN");
        if (isPositive(eventPrice)) {
            BigDecimal price = eventPrice.stripTrailingZeros();
            log.debug("event=hyperliquid.origin_store.price_resolved symbol={} source=event_price priceUsed={} copyImpact=safe",
                    safeLog(rawSymbol), price);
            log.debug("event=hyperliquid.origin_store.binance_price.failed reasonCode=binance_price_failed symbol={} fallbackUsed=true fallbackSource=event_price priceUsed={} copyImpact=safe",
                    safeLog(rawSymbol), price);
            return new BinanceFuturesPriceNormalizerService.BinancePriceReference(
                    rawSymbol,
                    price,
                    "event_price",
                    priceReferenceTs(operation),
                    0L,
                    0L,
                    price,
                    ONE,
                    rawSymbol,
                    rawSymbol
            );
        }
        BinanceFuturesPriceNormalizerService.BinancePriceReference resolved = priceNormalizerService.resolve(operation.getParSymbol()).orElse(null);
        if (resolved != null && isPositive(resolved.price())) {
            return resolved;
        }
        log.warn("event=hyperliquid.origin_store.binance_price.failed reasonCode=binance_price_failed symbol={} fallbackUsed=false fallbackSource=none priceUsed=null copyImpact=price_unavailable",
                safeLog(rawSymbol));
        return null;
    }

    private BigDecimal eventPriceFallback(OperacionDto operation, boolean closing) {
        if (operation == null) {
            return null;
        }
        if (closing || operation.getFechaCierre() != null || !operation.isOperacionActiva()) {
            return firstPositive(operation.getPrecioCierre(), operation.getPrecioMercado(), operation.getPrecioEntrada());
        }
        return firstPositive(operation.getPrecioMercado(), operation.getPrecioEntrada(), operation.getPrecioCierre());
    }

    private OffsetDateTime priceReferenceTs(OperacionDto operation) {
        Instant ts = firstNonNull(
                operation == null ? null : operation.getFechaCierre(),
                operation == null ? null : operation.getFechaCreacion(),
                Instant.now()
        );
        return toOffsetDateTime(ts);
    }

    private BigDecimal firstPositive(BigDecimal... values) {
        if (values == null) {
            return null;
        }
        for (BigDecimal value : values) {
            if (isPositive(value)) {
                return value;
            }
        }
        return null;
    }

    private String lockKey(HyperliquidMappedDelta mapped) {
        if (HyperliquidDeltaType.from(mapped == null ? null : mapped.deltaType()) == HyperliquidDeltaType.FLIP) {
            String wallet = lower(mapped == null ? null : mapped.wallet());
            String symbol = mapped == null ? null : mapped.symbol();
            if (symbol != null && symbol.isBlank()) {
                symbol = null;
            }
            if (wallet != null && symbol != null) {
                return "flip:" + wallet + ":" + symbol;
            }
        }
        if (mapped != null && mapped.positionKey() != null && !mapped.positionKey().isBlank()) {
            return mapped.positionKey();
        }
        UUID id = originId(mapped);
        if (id != null) {
            return "origin-id:" + id;
        }
        return "unknown:" + System.identityHashCode(mapped);
    }

    private PersistOutcome persistLifecycle(HyperliquidMappedDelta mapped, HyperliquidDirectCopyDispatchResult dispatchResult, long startedNs, BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference) {
        OperacionEvent event = mapped.event();
        OperacionDto operation = event.getOperacion();
        UUID originId = operation.getIdOperacion();
        if (originId == null) {
            throw new HyperliquidOriginLifecycleException(
                    "Mapped operation id is required",
                    Map.of("reason", "mapped_operation_id_missing")
            );
        }

        FuturesPositionEntity entity = repository.findByIdFuturesPosition(originId).orElse(null);
        boolean created = entity == null;
        if (created) {
            entity = new FuturesPositionEntity();
            entity.setIdFuturesPosition(originId);
            entity.setPlatform(PLATFORM);
            entity.setVenue(VENUE);
            entity.setExternalId("hyperliquid|direct|" + originId);
            entity.setCreatedAt(resolveCreatedAt(operation, mapped.request()));
            entity.setIngestedAt(OffsetDateTime.now(ZoneOffset.UTC));
            entity.setFailedAttempts(0);
            entity.setHasAccountIssue(false);
        }

        boolean closing = isClosing(mapped);
        if (!created && shouldIgnoreNonClosingAfterClose(entity, mapped)) {
            activeOriginIds.remove(mapped.positionKey(), entity.getIdFuturesPosition());
            return new PersistOutcome(
                    "stale_ignore",
                    false,
                    entity.getStatus(),
                    priceReference == null ? "binance_missing" : priceReference.source(),
                    priceReference == null ? null : priceReference.symbol(),
                    priceReference == null ? null : priceReference.price(),
                    elapsedMs(startedNs)
            );
        }

        closeOppositeSideWhenFlip(mapped, operation, priceReference);

        applyCommonFields(entity, mapped, operation, priceReference, dispatchResult, closing);
        applyStatusFields(entity, event, operation, priceReference);

        FuturesPositionEntity saved;
        if (created) {
            int affected = upsertFuturesPosition(entity);
            saved = repository.findByIdFuturesPosition(originId).orElse(entity);
            log.info("event=position_upsert.ok inserted={} updated={} idempotent={} positionKey={} walletId={} symbol={} side={} sourceUpdatedAt={} affectedRows={}",
                    affected > 0,
                    false,
                    affected == 0,
                    mapped.positionKey(),
                    mapped.wallet(),
                    mapped.symbol(),
                    mapped.side(),
                    entity.getSourceTs(),
                    affected);
        } else {
            saved = repository.saveAndFlush(entity);
            log.info("event=position_upsert.ok inserted=false updated=true idempotent=false positionKey={} walletId={} symbol={} side={} sourceUpdatedAt={} affectedRows=1",
                    mapped.positionKey(),
                    mapped.wallet(),
                    mapped.symbol(),
                    mapped.side(),
                    entity.getSourceTs());
        }
        if (saved.getStatus() != PositionStatus.OPEN) {
            activeOriginIds.remove(mapped.positionKey(), saved.getIdFuturesPosition());
        } else {
            activeOriginIds.put(mapped.positionKey(), saved.getIdFuturesPosition());
        }

        String action = created ? "insert" : (saved.getStatus() == PositionStatus.OPEN ? "update" : "close");
        return new PersistOutcome(
                action,
                created,
                saved.getStatus(),
                priceReference == null ? "binance_missing" : priceReference.source(),
                priceReference == null ? null : priceReference.symbol(),
                priceReference == null ? null : priceReference.price(),
                elapsedMs(startedNs)
        );
    }

    private int upsertFuturesPosition(FuturesPositionEntity entity) {
        String sql = """
                insert into futuros_operaciones.futures_position as fp (
                    id, platform, venue, chain_id, external_id, account_id, symbol, status, side,
                    operation_type, leverage, size_qty, notional_usd, margin_used_usd, size_legacy,
                    entry_price, mark_price, exit_price, unrealized_pnl_usd, realized_pnl_usd,
                    liquidation_price, source_ts, created_at, updated_at, closed_at, ingested_at,
                    is_active, has_account_issue, failed_attempts, raw, client_order_id, close_client_order_id
                ) values (
                    :id, :platform, :venue, :chainId, :externalId, :accountId, :symbol,
                    cast(:status as futuros_operaciones.position_status),
                    cast(:side as futuros_operaciones.position_side),
                    :operationType, :leverage, :sizeQty, :notionalUsd, :marginUsedUsd, :sizeLegacy,
                    :entryPrice, :markPrice, :exitPrice, :unrealizedPnlUsd, :realizedPnlUsd,
                    :liquidationPrice, :sourceTs, :createdAt, :updatedAt, :closedAt, :ingestedAt,
                    :isActive, :hasAccountIssue, :failedAttempts, cast(:raw as jsonb), :clientOrderId, :closeClientOrderId
                )
                on conflict (id) do update set
                    platform = excluded.platform,
                    venue = excluded.venue,
                    chain_id = excluded.chain_id,
                    external_id = excluded.external_id,
                    account_id = excluded.account_id,
                    symbol = excluded.symbol,
                    status = excluded.status,
                    side = excluded.side,
                    operation_type = excluded.operation_type,
                    leverage = excluded.leverage,
                    size_qty = excluded.size_qty,
                    notional_usd = excluded.notional_usd,
                    margin_used_usd = excluded.margin_used_usd,
                    size_legacy = excluded.size_legacy,
                    entry_price = excluded.entry_price,
                    mark_price = excluded.mark_price,
                    exit_price = excluded.exit_price,
                    unrealized_pnl_usd = excluded.unrealized_pnl_usd,
                    realized_pnl_usd = excluded.realized_pnl_usd,
                    liquidation_price = excluded.liquidation_price,
                    source_ts = excluded.source_ts,
                    updated_at = excluded.updated_at,
                    closed_at = excluded.closed_at,
                    ingested_at = excluded.ingested_at,
                    is_active = excluded.is_active,
                    has_account_issue = excluded.has_account_issue,
                    failed_attempts = excluded.failed_attempts,
                    raw = excluded.raw,
                    client_order_id = excluded.client_order_id,
                    close_client_order_id = excluded.close_client_order_id
                where fp.source_ts is null
                   or excluded.source_ts is null
                   or excluded.source_ts >= fp.source_ts
                """;
        return entityManager.createNativeQuery(sql)
                .setParameter("id", entity.getIdFuturesPosition())
                .setParameter("platform", entity.getPlatform())
                .setParameter("venue", entity.getVenue())
                .setParameter("chainId", entity.getChainId())
                .setParameter("externalId", entity.getExternalId())
                .setParameter("accountId", entity.getAccountId())
                .setParameter("symbol", entity.getSymbol())
                .setParameter("status", entity.getStatus() == null ? PositionStatus.OPEN.name() : entity.getStatus().name())
                .setParameter("side", entity.getSide() == null ? null : entity.getSide().name())
                .setParameter("operationType", entity.getOperationType())
                .setParameter("leverage", entity.getLeverage())
                .setParameter("sizeQty", entity.getSizeQty())
                .setParameter("notionalUsd", entity.getNotionalUsd())
                .setParameter("marginUsedUsd", entity.getMarginUsedUsd())
                .setParameter("sizeLegacy", entity.getSizeLegacy())
                .setParameter("entryPrice", entity.getEntryPrice())
                .setParameter("markPrice", entity.getMarkPrice())
                .setParameter("exitPrice", entity.getExitPrice())
                .setParameter("unrealizedPnlUsd", entity.getUnrealizedPnlUsd())
                .setParameter("realizedPnlUsd", entity.getRealizedPnlUsd())
                .setParameter("liquidationPrice", entity.getLiquidationPrice())
                .setParameter("sourceTs", entity.getSourceTs())
                .setParameter("createdAt", entity.getCreatedAt())
                .setParameter("updatedAt", entity.getUpdatedAt())
                .setParameter("closedAt", entity.getClosedAt())
                .setParameter("ingestedAt", entity.getIngestedAt())
                .setParameter("isActive", entity.getIsActive())
                .setParameter("hasAccountIssue", entity.getHasAccountIssue())
                .setParameter("failedAttempts", entity.getFailedAttempts())
                .setParameter("raw", rawJson(entity.getRaw()))
                .setParameter("clientOrderId", entity.getClientOrderId())
                .setParameter("closeClientOrderId", entity.getCloseClientOrderId())
                .executeUpdate();
    }

    private String rawJson(JsonNode raw) {
        if (raw == null || raw.isNull()) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(raw);
        } catch (Exception ex) {
            return "{}";
        }
    }


    private boolean shouldIgnoreNonClosingAfterClose(FuturesPositionEntity entity, HyperliquidMappedDelta mapped) {
        if (entity == null || entity.getStatus() != PositionStatus.CLOSED || isClosing(mapped)) {
            return false;
        }
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(mapped == null ? null : mapped.deltaType());
        if (deltaType == HyperliquidDeltaType.RESIZE || deltaType == HyperliquidDeltaType.UPDATE || deltaType == HyperliquidDeltaType.FLIP) {
            log.info("event=hyperliquid.origin_store.lifecycle_stale_ignored originId={} positionKey={} wallet={} symbol={} side={} deltaType={} currentStatus={} closedAt={}",
                    entity.getIdFuturesPosition(),
                    mapped == null ? "NA" : safeLog(mapped.positionKey()),
                    mapped == null ? "NA" : safeLog(mapped.wallet()),
                    mapped == null ? "NA" : safeLog(mapped.symbol()),
                    mapped == null ? "NA" : safeLog(mapped.side()),
                    deltaType,
                    entity.getStatus(),
                    entity.getClosedAt());
            return true;
        }
        return false;
    }

    private void closeOppositeSideWhenFlip(HyperliquidMappedDelta mapped,
                                           OperacionDto operation,
                                           BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference) {
        if (HyperliquidDeltaType.from(mapped == null ? null : mapped.deltaType()) != HyperliquidDeltaType.FLIP || operation == null) {
            return;
        }
        com.apunto.engine.shared.enums.PositionSide newSide = operation.getTipoOperacion();
        com.apunto.engine.shared.enums.PositionSide previousSide = opposite(newSide);
        if (previousSide == null) {
            return;
        }
        String accountId = lower(firstNonBlank(operation.getIdCuenta(), mapped == null ? null : mapped.wallet()));
        String symbol = firstNonBlank(operation.getParSymbol(), mapped == null ? null : mapped.symbol());
        if (accountId == null || symbol == null) {
            return;
        }
        repository.findLatestActiveByPlatformAccountSymbolSide(
                        PLATFORM,
                        accountId,
                        symbol,
                        previousSide.name(),
                        PositionStatus.OPEN.name()
                )
                .ifPresent(previous -> {
                    OffsetDateTime closedAt = toOffsetDateTime(firstNonNull(operation.getFechaCreacion(), Instant.now()));
                    BigDecimal exitPrice = positiveOrNull(priceReference == null ? null : priceReference.price());
                    previous.setStatus(PositionStatus.CLOSED);
                    previous.setIsActive(false);
                    previous.setClosedAt(closedAt);
                    previous.setExitPrice(exitPrice);
                    previous.setMarkPrice(nonNegative(firstNonNull(exitPrice, previous.getMarkPrice(), ZERO)));
                    previous.setRealizedPnlUsd(exitPrice == null ? ZERO : realizedPnl(previous.getSide(), effectiveSizeQtyForPnl(previous), previous.getEntryPrice(), exitPrice));
                    previous.setUnrealizedPnlUsd(ZERO);
                    previous.setUpdatedAt(OffsetDateTime.now(ZoneOffset.UTC));
                    repository.saveAndFlush(previous);
                    String previousKey = positionKey(previous.getAccountId(), previous.getSymbol(), previous.getSide() == null ? null : previous.getSide().name());
                    if (previousKey != null) {
                        activeOriginIds.remove(previousKey, previous.getIdFuturesPosition());
                    }
                    log.info("event=hyperliquid.origin_store.flip_previous_closed category=origin_position reasonCode=flip_closed_previous_side reasonAlias=flip_closed_previous_side friendlyReason=flip_cerro_lado_anterior explanation=se_cerro_la_posicion_original_opuesta_antes_de_abrir_el_nuevo_lado copyImpact=origin_state_consistent previousOriginId={} newOriginId={} wallet={} symbol={} previousSide={} newSide={} exitPrice={} realizedPnlUsd={} {}",
                            previous.getIdFuturesPosition(),
                            operation.getIdOperacion(),
                            safeLog(accountId),
                            safeLog(symbol),
                            previousSide,
                            newSide,
                            exitPrice,
                            previous.getRealizedPnlUsd(),
                            CopyLogAdvice.fields("flip_closed_previous_side", CopyLogAdvice.context(null, null, null, null, queue.size(), true, activeOriginIds.size(), "origin_store_flip")));
                });
    }

    private com.apunto.engine.shared.enums.PositionSide opposite(com.apunto.engine.shared.enums.PositionSide side) {
        if (side == com.apunto.engine.shared.enums.PositionSide.LONG) {
            return com.apunto.engine.shared.enums.PositionSide.SHORT;
        }
        if (side == com.apunto.engine.shared.enums.PositionSide.SHORT) {
            return com.apunto.engine.shared.enums.PositionSide.LONG;
        }
        return null;
    }

    private void applyCommonFields(
            FuturesPositionEntity entity,
            HyperliquidMappedDelta mapped,
            OperacionDto operation,
            BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference,
            HyperliquidDirectCopyDispatchResult dispatchResult,
            boolean closing
    ) {
        HyperliquidDeltaRequest request = mapped.request();
        BigDecimal sizeQty = nonNegative(operation.getSizeQty());
        BigDecimal normalizedPrice = priceReference == null ? null : priceReference.price();
        BigDecimal priceForNotional = positiveOrNull(normalizedPrice);
        BigDecimal notional = priceForNotional == null
                ? nonNegative(firstNonNull(entity.getNotionalUsd(), operation.getNotionalUsd(), operation.getSize(), ZERO))
                : nonNegative(firstNonNull(operation.getNotionalUsd(), operation.getSize(), sizeQty.multiply(priceForNotional)));
        if (sizeQty.compareTo(ZERO) > 0 && priceForNotional != null) {
            notional = sizeQty.multiply(priceForNotional).setScale(CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
        }

        entity.setPlatform(PLATFORM);
        entity.setVenue(VENUE);
        entity.setAccountId(lower(firstNonBlank(operation.getIdCuenta(), mapped.wallet())));
        entity.setSymbol(firstNonBlank(operation.getParSymbol(), mapped.symbol()));
        entity.setSide(operation.getTipoOperacion());
        entity.setOperationType(operation.getTipoOperacion() == null ? safeUpper(mapped.side()) : operation.getTipoOperacion().name());
        entity.setLeverage(positiveOrDefault(request == null ? null : request.leverage(), ONE));
        BigDecimal marginUsed = nonNegative(operation.getMarginUsedUsd());
        BigDecimal sizeLegacy = nonNegative(firstNonNull(notional, operation.getSize(), operation.getNotionalUsd(), sizeQty));
        if (!closing || !isPositive(entity.getSizeQty())) {
            entity.setSizeQty(sizeQty);
        }
        if (!closing || !isPositive(entity.getNotionalUsd())) {
            entity.setNotionalUsd(nonNegative(notional));
        }
        if (!closing || !isPositive(entity.getMarginUsedUsd())) {
            entity.setMarginUsedUsd(marginUsed);
        }
        if (!closing || !isPositive(entity.getSizeLegacy())) {
            entity.setSizeLegacy(sizeLegacy);
        }
        if (entity.getEntryPrice() == null || entity.getEntryPrice().compareTo(ZERO) <= 0) {
            entity.setEntryPrice(nonNegative(firstNonNull(!closing ? normalizedPrice : null, ZERO)));
        }
        entity.setMarkPrice(nonNegative(firstNonNull(normalizedPrice, entity.getMarkPrice(), ZERO)));
        entity.setUnrealizedPnlUsd(ZERO);
        entity.setSourceTs(resolveSourceTs(operation, request));
        entity.setUpdatedAt(OffsetDateTime.now(ZoneOffset.UTC));
        entity.setRaw(raw(mapped, operation, priceReference, dispatchResult));
    }

    private void applyStatusFields(
            FuturesPositionEntity entity,
            OperacionEvent event,
            OperacionDto operation,
            BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference
    ) {
        boolean open = event.getTipo() == OperacionEvent.Tipo.ABIERTA && operation.isOperacionActiva();
        if (open) {
            entity.setStatus(PositionStatus.OPEN);
            entity.setIsActive(true);
            entity.setClosedAt(null);
            entity.setExitPrice(null);
            return;
        }

        OffsetDateTime closedAt = toOffsetDateTime(firstNonNull(operation.getFechaCierre(), operation.getFechaCreacion(), Instant.now()));
        BigDecimal exitPrice = positiveOrNull(priceReference == null ? null : priceReference.price());
        entity.setStatus(PositionStatus.CLOSED);
        entity.setIsActive(false);
        entity.setClosedAt(closedAt);
        entity.setExitPrice(exitPrice);
        entity.setMarkPrice(nonNegative(firstNonNull(exitPrice, entity.getMarkPrice(), ZERO)));
        entity.setRealizedPnlUsd(exitPrice == null ? ZERO : realizedPnl(entity.getSide(), effectiveSizeQtyForPnl(entity), entity.getEntryPrice(), exitPrice));
        entity.setUnrealizedPnlUsd(ZERO);
    }

    private void hydrateActivePositionCache() {
        if (!hydrateOpenPositions || !enabled) {
            return;
        }
        long startedNs = System.nanoTime();
        try {
            List<FuturesPositionEntity> active = repository.findAllActiveByPlatformAndStatus(PLATFORM, PositionStatus.OPEN.name());
            int added = 0;
            for (FuturesPositionEntity entity : active) {
                if (entity == null || entity.getIdFuturesPosition() == null) {
                    continue;
                }
                String key = positionKey(entity.getAccountId(), entity.getSymbol(), entity.getSide() == null ? null : entity.getSide().name());
                if (key == null) {
                    continue;
                }
                activeOriginIds.put(key, entity.getIdFuturesPosition());
                added++;
            }
            log.info("event=hyperliquid.origin_store.hydrate.ok activePositions={} cached={} elapsedMs={}", active.size(), added, elapsedMs(startedNs));
        } catch (DataAccessException | IllegalStateException | IllegalArgumentException ex) {
            log.warn("event=hyperliquid.origin_store.hydrate.failed errClass={} errMsg=\"{}\" elapsedMs={}",
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
        }
    }

    private boolean isClosing(HyperliquidMappedDelta mapped) {
        return mapped.event() != null && mapped.event().getTipo() == OperacionEvent.Tipo.CERRADA;
    }

    private UUID fallbackLifecycleId(HyperliquidMappedDelta mapped) {
        String seed = "hyperliquid|direct|orphan-close|" + firstNonBlank(mapped.idempotencyKey(), mapped.positionKey(), UUID.randomUUID().toString());
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    }

    private String positionKey(String wallet, String symbol, String side) {
        String cleanWallet = lower(firstNonBlank(wallet, null));
        String cleanSymbol = firstNonBlank(symbol, null);
        String cleanSide = firstNonBlank(side, null);
        if (cleanWallet == null || cleanSymbol == null || cleanSide == null) {
            return null;
        }
        return "hyperliquid-position:" + cleanWallet + ':' + cleanSymbol + ':' + cleanSide;
    }

    private JsonNode raw(
            HyperliquidMappedDelta mapped,
            OperacionDto operation,
            BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference,
            HyperliquidDirectCopyDispatchResult dispatchResult
    ) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("__entry_price_feed", operation.getPrecioEntrada());
        meta.put("__mark_price_feed", operation.getPrecioMercado());
        meta.put("__exit_price_feed", operation.getPrecioCierre());
        meta.put("__price_source", priceReference == null ? "binance_missing" : priceReference.source());
        meta.put("__price_correction_pending", priceReference == null);
        meta.put("__main_price_columns", "origin_unit_price_normalized");
        if (priceReference != null) {
            meta.put("__price_corrected", priceReference.price());
            meta.put("__price_corrected_at", OffsetDateTime.now(ZoneOffset.UTC).toString());
            meta.put("__price_binance_symbol", priceReference.symbol());
            meta.put("__price_binance_reference_ts", priceReference.referenceTs().toString());
            meta.put("__price_binance_reference_diff_ms", priceReference.referenceDiffMs());
            meta.put("__price_binance_fetch_elapsed_ms", priceReference.fetchElapsedMs());
            meta.put("__price_binance_contract_price", priceReference.contractPrice());
            meta.put("__price_binance_contract_multiplier", priceReference.contractMultiplier());
            meta.put("__price_binance_raw_symbol", priceReference.rawSymbol());
            meta.put("__price_binance_canonical_symbol", priceReference.canonicalSymbol());
        }
        if (dispatchResult != null) {
            meta.put("__direct_copy_eligible_users", dispatchResult.eligibleUsers());
            meta.put("__direct_copy_submitted_tasks", dispatchResult.submittedTasks());
            meta.put("__direct_copy_business_skipped", dispatchResult.businessSkipped());
            meta.put("__direct_copy_fallback_jobs", dispatchResult.fallbackJobs());
            meta.put("__direct_copy_fallback_used", dispatchResult.fallbackUsed());
        }

        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("source", "ms-signals-orc.hyperliquid.direct_ingest");
        raw.put("idempotencyKey", mapped.idempotencyKey());
        raw.put("positionKey", mapped.positionKey());
        raw.put("deltaType", mapped.deltaType());
        raw.put("wallet", mapped.wallet());
        raw.put("symbol", mapped.symbol());
        raw.put("side", mapped.side());
        raw.put("operationActive", operation.isOperacionActiva());
        raw.put("request", mapped.request());
        raw.put("__meta", meta);
        return objectMapper.valueToTree(raw);
    }

    private boolean isPositive(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0;
    }

    private BigDecimal effectiveSizeQtyForPnl(FuturesPositionEntity entity) {
        if (entity == null) {
            return ZERO;
        }
        if (isPositive(entity.getSizeQty())) {
            return entity.getSizeQty();
        }
        BigDecimal sizeLegacy = entity.getSizeLegacy();
        BigDecimal entryPrice = entity.getEntryPrice();
        if (isPositive(sizeLegacy) && isPositive(entryPrice)) {
            return sizeLegacy.divide(entryPrice, CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
        }
        BigDecimal notional = entity.getNotionalUsd();
        if (isPositive(notional) && isPositive(entryPrice)) {
            return notional.divide(entryPrice, CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
        }
        return ZERO;
    }

    private BigDecimal realizedPnl(com.apunto.engine.shared.enums.PositionSide side, BigDecimal sizeQty, BigDecimal entryPrice, BigDecimal exitPrice) {
        BigDecimal qty = nonNegative(sizeQty);
        BigDecimal entry = nonNegative(entryPrice);
        BigDecimal exit = nonNegative(exitPrice);
        if (qty.compareTo(ZERO) <= 0 || entry.compareTo(ZERO) <= 0 || exit.compareTo(ZERO) <= 0 || side == null) {
            return ZERO;
        }
        BigDecimal diff = side.name().equals("SHORT") ? entry.subtract(exit) : exit.subtract(entry);
        return diff.multiply(qty).setScale(CALC_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
    }

    private OffsetDateTime resolveCreatedAt(OperacionDto operation, HyperliquidDeltaRequest request) {
        if (operation.getFechaCreacion() != null) {
            return toOffsetDateTime(operation.getFechaCreacion());
        }
        if (request != null && request.sourceTs() != null && request.sourceTs() > 0) {
            return OffsetDateTime.ofInstant(Instant.ofEpochMilli(request.sourceTs()), ZoneOffset.UTC);
        }
        return OffsetDateTime.now(ZoneOffset.UTC);
    }

    private OffsetDateTime resolveSourceTs(OperacionDto operation, HyperliquidDeltaRequest request) {
        if (request != null && request.sourceTs() != null && request.sourceTs() > 0) {
            return OffsetDateTime.ofInstant(Instant.ofEpochMilli(request.sourceTs()), ZoneOffset.UTC);
        }
        if (operation.getFechaCreacion() != null) {
            return toOffsetDateTime(operation.getFechaCreacion());
        }
        return OffsetDateTime.now(ZoneOffset.UTC);
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private BigDecimal nonNegative(BigDecimal value) {
        if (value == null) {
            return ZERO;
        }
        return value.signum() < 0 ? value.abs() : value;
    }

    private BigDecimal nonZero(BigDecimal value) {
        return value == null ? ZERO : value;
    }

    private BigDecimal positiveOrNull(BigDecimal value) {
        return value != null && value.compareTo(ZERO) > 0 ? value : null;
    }

    private BigDecimal positiveOrDefault(BigDecimal value, BigDecimal fallback) {
        if (value == null || value.compareTo(ZERO) <= 0) {
            return fallback;
        }
        return value;
    }

    private String lower(String value) {
        return value == null ? null : value.toLowerCase(Locale.ROOT);
    }

    private String safeUpper(String value) {
        return value == null ? "UNKNOWN" : value.toUpperCase(Locale.ROOT);
    }

    private String requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new HyperliquidOriginLifecycleException(
                    message,
                    Map.of("reason", "required_text_missing", "message", message)
            );
        }
        return value.trim();
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first.trim();
        }
        if (second != null && !second.isBlank()) {
            return second.trim();
        }
        return null;
    }

    private String firstNonBlank(String first, String second, String third) {
        String value = firstNonBlank(first, second);
        return value != null ? value : firstNonBlank(third, null);
    }

    @SafeVarargs
    private <T> T firstNonNull(T... values) {
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

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private UUID originId(HyperliquidMappedDelta mapped) {
        return mapped == null || mapped.event() == null || mapped.event().getOperacion() == null
                ? null
                : mapped.event().getOperacion().getIdOperacion();
    }

    private String safeTag(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value.length() > 64 ? value.substring(0, 64) : value;
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }

    private record PersistTask(
            HyperliquidMappedDelta mappedDelta,
            HyperliquidDirectCopyDispatchResult dispatchResult,
            long acceptedNs
    ) {
    }

    private record PersistOutcome(
            String action,
            boolean created,
            PositionStatus status,
            String priceSource,
            String binanceSymbol,
            BigDecimal binancePrice,
            long elapsedMs
    ) {
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger sequence = new AtomicInteger(1);

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, prefix + sequence.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
