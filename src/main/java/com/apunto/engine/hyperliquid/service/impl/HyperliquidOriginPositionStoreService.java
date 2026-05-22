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
import com.apunto.engine.service.binance.BinanceFuturesSymbolCatalog;
import com.apunto.engine.shared.enums.PositionStatus;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.util.CopyTraceIdUtil;
import com.apunto.engine.shared.util.CopyLogAdvice;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final BinanceFuturesSymbolCatalog symbolCatalog;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;
    private final boolean enabled;
    private final boolean hydrateOpenPositions;
    private final boolean failCopyOnBindError;
    private final boolean skipLateAdjustments;
    private final BlockingQueue<PersistTask> queue;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicLong submitted = new AtomicLong(0);
    private final AtomicLong persisted = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);
    private final AtomicLong skipped = new AtomicLong(0);
    private final AtomicLong rejected = new AtomicLong(0);
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
            BinanceFuturesSymbolCatalog symbolCatalog,
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
        this.symbolCatalog = symbolCatalog;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.meterRegistry = meterRegistry;
        this.enabled = enabled;
        this.hydrateOpenPositions = hydrateOpenPositions;
        this.failCopyOnBindError = failCopyOnBindError;
        this.skipLateAdjustments = skipLateAdjustments;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, queueCapacity));
        this.workers = Executors.newFixedThreadPool(Math.max(1, workerThreads), new NamedThreadFactory("hl-origin-store-"));
        log.info("event=hyperliquid.origin_store.config enabled={} workerThreads={} queueCapacity={} hydrateOpenPositions={} failCopyOnBindError={} skipLateAdjustments={}",
                enabled, Math.max(1, workerThreads), Math.max(1, queueCapacity), hydrateOpenPositions, failCopyOnBindError, skipLateAdjustments);
    }

    @PostConstruct
    public void start() {
        if (!enabled) {
            log.warn("event=hyperliquid.origin_store.disabled");
            return;
        }
        hydrateActivePositionCache();
        running.set(true);
        int workerCount = Math.max(1, ((java.util.concurrent.ThreadPoolExecutor) workers).getCorePoolSize());
        for (int i = 0; i < workerCount; i++) {
            workers.execute(this::workerLoop);
        }
        log.info("event=hyperliquid.origin_store.started queueCapacity={} activeCacheSize={}", queue.remainingCapacity() + queue.size(), activeOriginIds.size());
    }

    @Scheduled(fixedDelayString = "${hyperliquid.direct-ingest.origin-store.log-interval-ms:${hyperliquid.direct-ingest.log-interval-ms:10000}}")
    public void logMetrics() {
        if (!enabled) {
            return;
        }
        long p95QueueDelayMs = p95RecentQueueDelayMs();
        long maxDelay = maxQueueDelayMs.get();
        log.info("event=hyperliquid.origin_store.metrics queueDepth={} queueCapacity={} activeWorkers={} submitted={} persisted={} skipped={} failed={} rejected={} activeCacheSize={} p95QueueDelayMs={} maxQueueDelayMs={}",
                queue.size(), queue.remainingCapacity() + queue.size(), activeWorkers.get(), submitted.get(), persisted.get(), skipped.get(), failed.get(), rejected.get(), activeOriginIds.size(), p95QueueDelayMs, maxDelay);
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

        if (deltaType.canStartCopyLifecycle()) {
            return activeOriginIds.computeIfAbsent(key, ignored -> UUID.randomUUID());
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

    private void workerLoop() {
        activeWorkers.incrementAndGet();
        try {
            while (running.get() || !queue.isEmpty()) {
                PersistTask task = queue.take();
                process(task);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            activeWorkers.decrementAndGet();
        }
    }

    private void process(PersistTask task) {
        long startedNs = System.nanoTime();
        HyperliquidMappedDelta mapped = task.mappedDelta();
        long queueDelayMs = elapsedMs(task.acceptedNs());
        recordQueueDelay(queueDelayMs);
        try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", originTraceId(mapped))) {
            if (isUnsupportedBinanceSymbol(mapped)) {
                skipped.incrementAndGet();
                meterRegistry.counter("signals.hyperliquid.origin_store.skipped.total", "reason", "binance_symbol_unsupported").increment();
                log.info("event=hyperliquid.origin_store.skipped category=origin_position reasonCode=binance_symbol_unsupported reasonAlias=binance_symbol_unsupported friendlyReason=simbolo_no_existe_en_binance explanation=no_se_guarda_estado_original_con_precio_binance_si_el_simbolo_no_existe copyImpact=no_copy_order originId={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} cacheSize={} queueDelayMs={} elapsedMs={} queueDepth={} {}",
                        originId(mapped),
                        safeLog(mapped.idempotencyKey()),
                        safeLog(mapped.positionKey()),
                        safeLog(mapped.wallet()),
                        safeLog(mapped.symbol()),
                        safeLog(mapped.side()),
                        safeLog(mapped.deltaType()),
                        symbolCatalog.cachedSymbols(),
                        queueDelayMs,
                        elapsedMs(startedNs),
                        queue.size(),
                        CopyLogAdvice.fields("binance_symbol_unsupported", CopyLogAdvice.context(task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().eligibleUsers(), task.dispatchResult() == null ? null : task.dispatchResult().submittedTasks(), task.dispatchResult() == null ? null : task.dispatchResult().businessSkipped(), queue.size(), null, activeOriginIds.size(), "origin_store")));
                return;
            }
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
            BinanceFuturesPriceNormalizerService.BinancePriceReference priceReference = resolvePriceReference(mapped);
            String lockKey = lockKey(mapped);
            Object lock = positionLocks.computeIfAbsent(lockKey, ignoredLock -> new Object());
            PersistOutcome outcome;
            synchronized (lock) {
                outcome = transactionTemplate.execute(status -> persistLifecycle(mapped, task.dispatchResult(), startedNs, priceReference));
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
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
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

    private boolean isUnsupportedBinanceSymbol(HyperliquidMappedDelta mapped) {
        if (mapped == null || mapped.event() == null || mapped.event().getOperacion() == null) {
            return true;
        }
        String symbol = mapped.event().getOperacion().getParSymbol();
        if (symbol == null || symbol.isBlank()) {
            symbol = mapped.symbol();
        }
        return symbolCatalog.resolve(symbol).isEmpty();
    }

    private BinanceFuturesPriceNormalizerService.BinancePriceReference resolvePriceReference(HyperliquidMappedDelta mapped) {
        if (mapped == null || mapped.event() == null || mapped.event().getOperacion() == null) {
            return null;
        }
        return priceNormalizerService.resolve(mapped.event().getOperacion().getParSymbol()).orElse(null);
    }

    private String lockKey(HyperliquidMappedDelta mapped) {
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

        applyCommonFields(entity, mapped, operation, priceReference, dispatchResult, closing);
        applyStatusFields(entity, event, operation, priceReference);

        FuturesPositionEntity saved;
        if (created) {
            entityManager.persist(entity);
            entityManager.flush();
            saved = entity;
        } else {
            saved = repository.saveAndFlush(entity);
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
        BigDecimal exitPrice = nonNegative(firstNonNull(
                priceReference == null ? null : priceReference.price(),
                entity.getMarkPrice(),
                ZERO
        ));
        entity.setStatus(PositionStatus.CLOSED);
        entity.setIsActive(false);
        entity.setClosedAt(closedAt);
        entity.setExitPrice(exitPrice);
        entity.setMarkPrice(exitPrice);
        entity.setRealizedPnlUsd(realizedPnl(entity.getSide(), effectiveSizeQtyForPnl(entity), entity.getEntryPrice(), exitPrice));
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
        meta.put("__main_price_columns", "binance_only");
        if (priceReference != null) {
            meta.put("__price_corrected", priceReference.price());
            meta.put("__price_corrected_at", OffsetDateTime.now(ZoneOffset.UTC).toString());
            meta.put("__price_binance_symbol", priceReference.symbol());
            meta.put("__price_binance_reference_ts", priceReference.referenceTs().toString());
            meta.put("__price_binance_reference_diff_ms", priceReference.referenceDiffMs());
            meta.put("__price_binance_fetch_elapsed_ms", priceReference.fetchElapsedMs());
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
