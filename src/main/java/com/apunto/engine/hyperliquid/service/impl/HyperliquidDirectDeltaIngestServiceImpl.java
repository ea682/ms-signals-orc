package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaAcceptedResponse;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.exception.HyperliquidDirectIngestRejectedException;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectDeltaIngestService;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectCopyDispatchService;
import com.apunto.engine.service.OperationMovementEventService;
import com.apunto.engine.service.ShadowCopyTradingService;
import com.apunto.engine.service.copy.concurrency.PostgresDeadlockRetryExecutor;
import com.apunto.engine.service.copy.recovery.ShadowEventDeadLetterStore;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.util.CopyTraceIdUtil;
import com.apunto.engine.shared.util.CopyLogAdvice;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class HyperliquidDirectDeltaIngestServiceImpl implements HyperliquidDirectDeltaIngestService {

    private final HyperliquidDirectIngestProperties properties;
    private final HyperliquidDirectCopyDispatchService directCopyDispatchService;
    private final HyperliquidDirectIngestIdempotencyGuard idempotencyGuard;
    private final HyperliquidOriginPositionStoreService originPositionStoreService;
    private final OperationMovementEventService operationMovementEventService;
    private final ShadowCopyTradingService shadowCopyTradingService;
    private final MeterRegistry meterRegistry;
    private final HyperliquidFlipExecutionBasisPolicy flipExecutionBasisPolicy =
            new HyperliquidFlipExecutionBasisPolicy();
    private final int laneCount;
    private final BlockingQueue<QueuedDelta>[] lanes;
    private final AtomicBoolean[] laneWorkerSlots;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicLong accepted = new AtomicLong(0);
    private final AtomicLong processed = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);
    private final AtomicLong duplicates = new AtomicLong(0);
    private final AtomicLong workerUncaughtFailures = new AtomicLong(0);
    private final AtomicLong workerRestarts = new AtomicLong(0);
    private final AtomicLong workerStops = new AtomicLong(0);
    private final AtomicLong queueHighWaterMark = new AtomicLong(0);
    private final Cache<String, Boolean> recentKeys;
    private final boolean shadowAsyncEnabled;
    private final int shadowWorkerCount;
    private final long shadowEnqueueTimeoutMs;
    private final long shadowSlowLogMs;
    private final BlockingQueue<ShadowTask>[] shadowLanes;
    private final ExecutorService shadowWorkers;
    private final AtomicBoolean[] shadowWorkerSlots;
    private final AtomicBoolean shadowRunning = new AtomicBoolean(false);
    private final AtomicInteger activeShadowWorkers = new AtomicInteger(0);
    private final AtomicLong shadowEnqueued = new AtomicLong(0);
    private final AtomicLong shadowRecorded = new AtomicLong(0);
    private final AtomicLong shadowDuplicates = new AtomicLong(0);
    private final AtomicLong shadowDeferred = new AtomicLong(0);
    private final AtomicLong shadowDropped = new AtomicLong(0);
    private final AtomicLong shadowFailed = new AtomicLong(0);
    private final AtomicLong shadowWorkerRestarts = new AtomicLong(0);
    private final AtomicLong shadowWorkerStops = new AtomicLong(0);
    private final AtomicLong shadowQueueHighWaterMark = new AtomicLong(0);

    @Autowired(required = false)
    private PostgresDeadlockRetryExecutor postgresDeadlockRetryExecutor;

    @Autowired(required = false)
    private ShadowEventDeadLetterStore shadowEventDeadLetterStore;

    public HyperliquidDirectDeltaIngestServiceImpl(
            HyperliquidDirectIngestProperties properties,
            HyperliquidDirectCopyDispatchService directCopyDispatchService,
            HyperliquidDirectIngestIdempotencyGuard idempotencyGuard,
            HyperliquidOriginPositionStoreService originPositionStoreService,
            OperationMovementEventService operationMovementEventService,
            ShadowCopyTradingService shadowCopyTradingService,
            MeterRegistry meterRegistry,
            @Value("${copy.shadow.async.enabled:true}") boolean shadowAsyncEnabled,
            @Value("${copy.shadow.queue-capacity:10000}") int shadowQueueCapacity,
            @Value("${copy.shadow.worker-threads:2}") int shadowWorkerThreads,
            @Value("${copy.shadow.enqueue-timeout-ms:2}") long shadowEnqueueTimeoutMs,
            @Value("${copy.shadow.log-slow-ms:100}") long shadowSlowLogMs
    ) {
        this.properties = properties;
        this.directCopyDispatchService = directCopyDispatchService;
        this.idempotencyGuard = idempotencyGuard;
        this.originPositionStoreService = originPositionStoreService;
        this.operationMovementEventService = operationMovementEventService;
        this.shadowCopyTradingService = shadowCopyTradingService;
        this.meterRegistry = meterRegistry;
        this.laneCount = Math.max(1, properties.getWorkerThreads());
        int perLaneCapacity = Math.max(1, (Math.max(1, properties.getQueueCapacity()) + laneCount - 1) / laneCount);
        this.lanes = new BlockingQueue[laneCount];
        this.laneWorkerSlots = new AtomicBoolean[laneCount];
        for (int i = 0; i < laneCount; i++) {
            this.lanes[i] = new ArrayBlockingQueue<>(perLaneCapacity);
            this.laneWorkerSlots[i] = new AtomicBoolean(false);
        }
        this.workers = Executors.newFixedThreadPool(
                laneCount,
                new NamedThreadFactory("hl-delta-ingest-")
        );
        this.recentKeys = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(Math.max(1L, properties.getDedupeTtlSeconds())))
                .maximumSize(Math.max(1000L, properties.getQueueCapacity() * 2L))
                .build();
        this.shadowAsyncEnabled = shadowAsyncEnabled;
        this.shadowWorkerCount = Math.max(1, shadowWorkerThreads);
        this.shadowEnqueueTimeoutMs = Math.max(0L, shadowEnqueueTimeoutMs);
        this.shadowSlowLogMs = Math.max(1L, shadowSlowLogMs);
        int perShadowLaneCapacity = Math.max(1, (Math.max(1, shadowQueueCapacity) + this.shadowWorkerCount - 1) / this.shadowWorkerCount);
        this.shadowLanes = new BlockingQueue[this.shadowWorkerCount];
        for (int i = 0; i < this.shadowLanes.length; i++) {
            this.shadowLanes[i] = new ArrayBlockingQueue<>(perShadowLaneCapacity);
        }
        this.shadowWorkers = Executors.newFixedThreadPool(this.shadowWorkerCount, new NamedThreadFactory("hl-shadow-ingest-"));
        this.shadowWorkerSlots = new AtomicBoolean[this.shadowWorkerCount];
        for (int i = 0; i < this.shadowWorkerSlots.length; i++) {
            this.shadowWorkerSlots[i] = new AtomicBoolean(false);
        }
        registerMetrics();
    }

    @PostConstruct
    public void start() {
        if (!properties.isEnabled()) {
            log.warn("event=hyperliquid.direct_ingest.disabled queueCapacity={} workerThreads={} rejectWhenDisabled={}",
                    properties.getQueueCapacity(), properties.getWorkerThreads(), properties.isRejectWhenDisabled());
            return;
        }
        running.set(true);
        for (int i = 0; i < laneCount; i++) {
            startWorker(i, "startup");
        }
        startShadowWorkers("startup");
        log.info("event=hyperliquid.direct_ingest.started queueCapacity={} workerThreads={} dedupeEnabled={} dedupeTtlSeconds={} distributedDedupeEnabled={} dedupeLeaseTtlMs={} failOpenOnDedupeError={} shadowAsyncEnabled={} shadowQueueCapacity={} shadowWorkerThreads={} shadowEnqueueTimeoutMs={} humanMessage=direct_ingest_listo_con_idempotencia_para_varias_instancias",
                properties.getQueueCapacity(), properties.getWorkerThreads(), properties.isDedupeEnabled(), properties.getDedupeTtlSeconds(),
                properties.isDistributedDedupeEnabled(), properties.getDedupeLeaseTtlMs(), properties.isFailOpenOnDedupeError(),
                shadowAsyncEnabled, shadowQueueCapacity(), shadowWorkerCount, shadowEnqueueTimeoutMs);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        shadowRunning.set(false);
        workers.shutdownNow();
        shadowWorkers.shutdownNow();
        log.info("event=hyperliquid.direct_ingest.stopped queueDepth={} accepted={} processed={} failed={} duplicates={} shadowQueueDepth={} shadowEnqueued={} shadowRecorded={} shadowDuplicates={} shadowDeferred={} shadowDropped={} shadowFailed={}",
                queueDepth(), accepted.get(), processed.get(), failed.get(), duplicates.get(),
                shadowQueueDepth(), shadowEnqueued.get(), shadowRecorded.get(), shadowDuplicates.get(), shadowDeferred.get(), shadowDropped.get(), shadowFailed.get());
    }

    @Scheduled(fixedDelayString = "${hyperliquid.direct-ingest.log-interval-ms:10000}")
    public void logMetrics() {
        if (!properties.isEnabled()) {
            return;
        }
        ensureWorkersHealthy("metrics");
        ensureShadowWorkersHealthy("metrics");
        int queueDepth = queueDepth();
        int shadowQueueDepth = shadowQueueDepth();
        int shadowQueueCapacity = shadowQueueCapacity();
        log.info("event=hyperliquid.direct_ingest.metrics queueDepth={} queueCapacity={} activeWorkers={} expectedWorkers={} workerRestarts={} workerStops={} workerUncaughtFailures={} accepted={} processed={} failed={} duplicates={} queueHighWaterMark={} shadowAsyncEnabled={} shadowQueueDepth={} shadowQueueCapacity={} activeShadowWorkers={} expectedShadowWorkers={} shadowWorkerRestarts={} shadowWorkerStops={} shadowEnqueued={} shadowRecorded={} shadowDuplicates={} shadowDeferred={} shadowDropped={} shadowFailed={} shadowQueueHighWaterMark={}",
                queueDepth, properties.getQueueCapacity(), activeWorkers.get(), laneCount, workerRestarts.get(), workerStops.get(), workerUncaughtFailures.get(), accepted.get(), processed.get(), failed.get(), duplicates.get(), queueHighWaterMark.get(),
                shadowAsyncEnabled, shadowQueueDepth, shadowQueueCapacity, activeShadowWorkers.get(), shadowWorkerCount, shadowWorkerRestarts.get(), shadowWorkerStops.get(), shadowEnqueued.get(), shadowRecorded.get(), shadowDuplicates.get(), shadowDeferred.get(), shadowDropped.get(), shadowFailed.get(), shadowQueueHighWaterMark.get());
        if (properties.getQueueCapacity() > 0 && queueDepth >= Math.max(1, (int) (properties.getQueueCapacity() * 0.80d))) {
            log.warn("event=hyperliquid.direct_ingest.queue_pressure reasonCode=direct_ingest_queue_pressure queueDepth={} queueCapacity={} activeWorkers={} expectedWorkers={} failed={} duplicates={} action=scale_workers_or_check_downstream",
                    queueDepth, properties.getQueueCapacity(), activeWorkers.get(), laneCount, failed.get(), duplicates.get());
        }
        if (shadowQueueCapacity > 0 && shadowQueueDepth >= Math.max(1, (int) (shadowQueueCapacity * 0.80d))) {
            log.warn("event=shadow_async_queue_pressure reasonCode=shadow_async_queue_pressure queueDepth={} queueCapacity={} activeShadowWorkers={} expectedShadowWorkers={} shadowDeferred={} shadowDropped={} shadowFailed={} liveImpact=LIVE_NOT_BLOCKED action=scale_shadow_workers_or_check_shadow_db",
                    shadowQueueDepth, shadowQueueCapacity, activeShadowWorkers.get(), shadowWorkerCount, shadowDeferred.get(), shadowDropped.get(), shadowFailed.get());
        }
    }

    @Override
    public HyperliquidDeltaAcceptedResponse accept(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null) {
            throw new IllegalArgumentException("mappedDelta is required");
        }
        if (!properties.isEnabled()) {
            if (properties.isRejectWhenDisabled()) {
                throw rejected("direct_ingest_disabled", mappedDelta, queueDepth());
            }
            return response(mappedDelta, false);
        }

        final String dedupeKey = buildDedupeKey(mappedDelta);
        if (properties.isDedupeEnabled() && recentKeys.asMap().putIfAbsent(dedupeKey, Boolean.TRUE) != null) {
            duplicates.incrementAndGet();
            incrementDuplicateMetric(mappedDelta.deltaType(), "in_memory");
            log.info("event=hyperliquid.direct_ingest.duplicate dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} reasonCode=movement_already_recorded queueDepth={} {}",
                    dedupeKey, mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), queueDepth(),
                    CopyLogAdvice.fields("movement_already_recorded", CopyLogAdvice.context(null, null, null, null, queueDepth(), null, null, "direct_ingest_dedupe")));
            return response(mappedDelta, true);
        }

        boolean distributedAcquired = idempotencyGuard.tryAcquire(mappedDelta, dedupeKey);
        if (!distributedAcquired) {
            duplicates.incrementAndGet();
            incrementDuplicateMetric(mappedDelta.deltaType(), "distributed");
            return response(mappedDelta, true);
        }

        QueuedDelta queuedDelta = new QueuedDelta(mappedDelta, dedupeKey, System.nanoTime());
        BlockingQueue<QueuedDelta> lane = lanes[laneFor(mappedDelta, dedupeKey)];
        if (!lane.offer(queuedDelta)) {
            if (properties.isDedupeEnabled()) {
                recentKeys.invalidate(dedupeKey);
            }
            idempotencyGuard.markRejected(mappedDelta, "queue_full", null);
            meterRegistry.counter("signals.hyperliquid.direct_ingest.rejected.total", "reason", "queue_full").increment();
            throw rejected("queue_full", mappedDelta, queueDepth());
        }

        accepted.incrementAndGet();
        recordQueueHighWater(queueDepth());
        ensureWorkersHealthy("accept");
        meterRegistry.counter("signals.hyperliquid.direct_ingest.accepted.total", "deltaType", safeTag(mappedDelta.deltaType())).increment();
        log.debug("event=hyperliquid.direct_ingest.accepted dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} queueDepth={}",
                dedupeKey, mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), queueDepth());
        return response(mappedDelta, false);
    }

    static String buildDedupeKey(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null) {
            return "hyperliquid:missing";
        }
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(mappedDelta.deltaType());
        if (!deltaType.canAdjustExistingCopy()) {
            return firstNonBlank(mappedDelta.idempotencyKey(), mappedDelta.positionKey(), "hyperliquid:missing");
        }
        if (mappedDelta.request() == null || mappedDelta.request().sourceTs() == null || mappedDelta.request().sourceTs() <= 0) {
            return firstNonBlank(mappedDelta.idempotencyKey(), mappedDelta.positionKey(), "hyperliquid:adjustment:missing-source-ts");
        }
        return String.join("|",
                "hyperliquid-adjustment",
                normalizeKey(mappedDelta.positionKey()),
                deltaType.name(),
                String.valueOf(mappedDelta.request().sourceTs()),
                decimalKey(firstNonNull(mappedDelta.request().sizeQty(), mappedDelta.request().signedSizeQty())),
                decimalKey(mappedDelta.request().notionalUsd()),
                decimalKey(mappedDelta.request().marginUsedUsd()),
                decimalKey(firstNonNull(mappedDelta.request().markPrice(), mappedDelta.request().entryPrice())));
    }

    private static String firstNonBlank(String first, String second, String fallback) {
        if (first != null && !first.isBlank()) {
            return first.trim();
        }
        if (second != null && !second.isBlank()) {
            return second.trim();
        }
        return fallback;
    }

    private static <T> T firstNonNull(T first, T second) {
        return first != null ? first : second;
    }

    private static String normalizeKey(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private static String decimalKey(BigDecimal value) {
        if (value == null) {
            return "NA";
        }
        return value.stripTrailingZeros().toPlainString();
    }

    private void startWorker(int laneIndex, String reason) {
        if (!properties.isEnabled() || !running.get()) {
            return;
        }
        if (laneIndex < 0 || laneIndex >= laneWorkerSlots.length) {
            return;
        }
        AtomicBoolean slot = laneWorkerSlots[laneIndex];
        if (!slot.compareAndSet(false, true)) {
            return;
        }
        if (!"startup".equals(reason)) {
            workerRestarts.incrementAndGet();
        }
        try {
            BlockingQueue<QueuedDelta> lane = lanes[laneIndex];
            workers.execute(() -> workerLoop(laneIndex, lane, reason));
        } catch (RuntimeException ex) {
            slot.set(false);
            log.error("event=hyperliquid.direct_ingest.worker_start_failed reasonCode=direct_ingest_worker_start_failed laneIndex={} reason={} errClass={} errMsg=\"{}\" queueDepth={} activeWorkers={}",
                    laneIndex, safeLog(reason), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), queueDepth(), activeWorkers.get(), ex);
        }
    }

    private void ensureWorkersHealthy(String reason) {
        if (!properties.isEnabled() || !running.get()) {
            return;
        }
        for (int i = 0; i < laneWorkerSlots.length; i++) {
            if (!laneWorkerSlots[i].get()) {
                log.warn("event=hyperliquid.direct_ingest.worker_missing reasonCode=direct_ingest_worker_missing laneIndex={} reason={} queueDepth={} activeWorkers={} expectedWorkers={} action=restart_worker",
                        i, safeLog(reason), queueDepth(), activeWorkers.get(), laneCount);
                startWorker(i, reason);
            }
        }
    }

    private void workerLoop(int laneIndex, BlockingQueue<QueuedDelta> lane, String startReason) {
        activeWorkers.incrementAndGet();
        log.info("event=hyperliquid.direct_ingest.worker_started laneIndex={} startReason={} queueDepth={} activeWorkers={} expectedWorkers={}",
                laneIndex, safeLog(startReason), queueDepth(), activeWorkers.get(), laneCount);
        try {
            while (running.get() || !lane.isEmpty()) {
                QueuedDelta task = lane.poll(250, TimeUnit.MILLISECONDS);
                if (task != null) {
                    processSafely(task, laneIndex);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.info("event=hyperliquid.direct_ingest.worker_interrupted laneIndex={} queueDepth={} activeWorkers={}",
                    laneIndex, queueDepth(), activeWorkers.get());
        } finally {
            workerStops.incrementAndGet();
            int remainingWorkers = activeWorkers.decrementAndGet();
            laneWorkerSlots[laneIndex].set(false);
            log.warn("event=hyperliquid.direct_ingest.worker_stopped laneIndex={} queueDepth={} activeWorkers={} expectedWorkers={} running={}",
                    laneIndex, queueDepth(), remainingWorkers, laneCount, running.get());
        }
    }

    private void processSafely(QueuedDelta task, int laneIndex) {
        try {
            process(task);
        } catch (Throwable ex) {
            failed.incrementAndGet();
            workerUncaughtFailures.incrementAndGet();
            HyperliquidMappedDelta mapped = task == null ? null : task.mappedDelta();
            try {
                if (mapped != null) {
                    idempotencyGuard.markFailed(mapped, "direct_ingest_worker_uncaught", ex);
                }
            } catch (RuntimeException markFailedEx) {
                log.warn("event=hyperliquid.direct_ingest.mark_failed_after_uncaught_failed laneIndex={} errClass={} errMsg=\"{}\"",
                        laneIndex, markFailedEx.getClass().getSimpleName(), safeLog(markFailedEx.getMessage()));
            }
            meterRegistry.counter("signals.hyperliquid.direct_ingest.worker_uncaught.total", "deltaType", safeTag(mapped == null ? null : mapped.deltaType())).increment();
            meterRegistry.counter(
                    "worker_failures_total",
                    "worker", "direct_ingest",
                    "delta_type", safeTag(mapped == null ? null : mapped.deltaType())
            ).increment();
            log.error("event=hyperliquid.direct_ingest.worker_uncaught reasonCode=direct_ingest_worker_uncaught laneIndex={} dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" queueDepth={} activeWorkers={} expectedWorkers={} action=keep_worker_alive",
                    laneIndex,
                    task == null ? "NA" : safeLog(task.dedupeKey()),
                    mapped == null ? "NA" : safeLog(mapped.idempotencyKey()),
                    mapped == null ? "NA" : safeLog(mapped.positionKey()),
                    mapped == null ? "NA" : safeLog(mapped.wallet()),
                    mapped == null ? "NA" : safeLog(mapped.symbol()),
                    mapped == null ? "NA" : safeLog(mapped.side()),
                    mapped == null ? "NA" : safeLog(mapped.deltaType()),
                    ex.getClass().getSimpleName(),
                    safeLog(ex.getMessage()),
                    queueDepth(),
                    activeWorkers.get(),
                    laneCount,
                    ex);
            if (ex instanceof VirtualMachineError) {
                throw (VirtualMachineError) ex;
            }
            if (ex instanceof ThreadDeath) {
                throw (ThreadDeath) ex;
            }
        }
    }

    private void process(QueuedDelta task) {
        long startedNs = System.nanoTime();
        HyperliquidMappedDelta mapped = task.mappedDelta();
        HyperliquidMappedDelta copyReady = mapped;
        try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", originTraceId(mapped))) {
            HyperliquidFlipExecutionBasisPolicy.Decision flipBasis =
                    flipExecutionBasisPolicy.evaluate(mapped);
            if (flipBasis.flip() && !flipBasis.allowed()) {
                blockFlipWithoutExecutionBasis(task, mapped, flipBasis, startedNs);
                return;
            }
            copyReady = originPositionStoreService.bindOriginIdForCopy(mapped);
            MDC.put("traceId", originTraceId(copyReady));
            ShadowEnqueueResult shadowEnqueue = enqueueShadowBeforeLive(copyReady, task.acceptedNs());
            long liveDispatchStartNs = System.nanoTime();
            HyperliquidDirectCopyDispatchResult dispatchResult = directCopyDispatchService.dispatch(copyReady, task.acceptedNs());
            long liveDispatchElapsedMs = elapsedMs(liveDispatchStartNs);
            if (shadowEnqueue.recoveryRequired()) {
                boolean durable = persistRecoverableShadowFailure(
                        copyReady,
                        shadowEnqueue.reasonCode(),
                        new IllegalStateException(shadowEnqueue.reasonCode()),
                        1
                );
                if (!durable) {
                    shadowDropped.incrementAndGet();
                    meterRegistry.counter("signals.copy.shadow.async.dropped.total", "reason", "recovery_persist_failed").increment();
                    log.error("event=shadow.recovery.persist_failed reasonCode=SHADOW_RECOVERY_PERSIST_FAILED_AFTER_LIVE_DISPATCH decision=RETRY_INGEST retryable=true shouldAlert=true idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} liveImpact=LIVE_ALREADY_DISPATCHED shadowImpact=NOT_DURABLE duplicateOrderRisk=PROTECTED_BY_DURABLE_COPY_INTENT",
                            copyReady.idempotencyKey(), copyReady.positionKey(), copyReady.wallet(), copyReady.symbol(), copyReady.side(), copyReady.deltaType());
                    throw new IllegalStateException("SHADOW_RECOVERY_PERSIST_FAILED_AFTER_LIVE_DISPATCH");
                }
                shadowDeferred.incrementAndGet();
                meterRegistry.counter("signals.copy.shadow.async.deferred.total", "reason", safeTag(shadowEnqueue.reasonCode())).increment();
                shadowEnqueue = shadowEnqueue.asDurableDeferred();
            }
            logShadowLiveSeparation(copyReady, dispatchResult, shadowEnqueue);
            operationMovementEventService.recordAsync(copyReady, dispatchResult, dispatchResult.reasonCode());
            originPositionStoreService.submitAfterCopy(copyReady, dispatchResult);
            idempotencyGuard.markProcessed(copyReady, dispatchResult.reasonCode());
            processed.incrementAndGet();
            long elapsedMs = elapsedMs(startedNs);
            long queueDelayMs = elapsedMs(task.acceptedNs());
            meterRegistry.timer("signals.hyperliquid.direct_ingest.process.duration", Tags.of("result", "ok", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            meterRegistry.timer("copy_event_ingest_duration", "source_platform", "hyperliquid", "result", "ok")
                    .record(System.nanoTime() - startedNs, java.util.concurrent.TimeUnit.NANOSECONDS);
            String copySkipReasonCode = dispatchResult.reasonCode();
            String copySkipDiagnostic = copySkipReasonCode == null ? "" : CopyLogAdvice.fields(
                    copySkipReasonCode,
                    CopyLogAdvice.context(dispatchResult.eligibleUsers(), dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(), dispatchResult.businessSkipped(), queueDepth(), null, null, "direct_ingest")
            );
            log.info("event=hyperliquid.direct_ingest.processed dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} eligibleUsers={} submittedTasks={} businessSkipped={} fallbackJobs={} fallbackUsed={} copySkipReasonCode={} queueDelayMs={} elapsedMs={} queueDepth={} {}",
                    task.dedupeKey(), copyReady.idempotencyKey(), copyReady.positionKey(), copyReady.wallet(), copyReady.symbol(), copyReady.side(), copyReady.deltaType(),
                    dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(), dispatchResult.businessSkipped(), dispatchResult.fallbackJobs(), dispatchResult.fallbackUsed(), safeLog(copySkipReasonCode),
                    queueDelayMs, elapsedMs, queueDepth(), copySkipDiagnostic);
            log.info("event=hyperliquid.direct_ingest.hot_path_completed dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} liveDispatchElapsedMs={} shadowEnqueued={} shadowReasonCode={} shadowEnqueueLatencyMs={} shadowQueueDepth={} shadowQueueRemainingCapacity={} totalElapsedMs={} liveImpact=LIVE_DISPATCH_NOT_BLOCKED_BY_SHADOW",
                    task.dedupeKey(), copyReady.idempotencyKey(), copyReady.positionKey(), copyReady.wallet(), copyReady.symbol(), copyReady.side(), copyReady.deltaType(),
                    liveDispatchElapsedMs, shadowEnqueue.enqueued(), safeLog(shadowEnqueue.reasonCode()), shadowEnqueue.enqueueLatencyMs(), shadowEnqueue.queueDepth(), shadowEnqueue.remainingCapacity(), elapsedMs);
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            failed.incrementAndGet();
            idempotencyGuard.markFailed(copyReady, "direct_ingest_processing_failed", ex);
            MDC.put("traceId", originTraceId(copyReady));
            meterRegistry.timer("signals.hyperliquid.direct_ingest.process.duration", Tags.of("result", "error", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            meterRegistry.timer("copy_event_ingest_duration", "source_platform", "hyperliquid", "result", "error")
                    .record(System.nanoTime() - startedNs, java.util.concurrent.TimeUnit.NANOSECONDS);
            log.error("event=hyperliquid.direct_ingest.failed dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={}",
                    task.dedupeKey(), copyReady.idempotencyKey(), copyReady.positionKey(), copyReady.wallet(), copyReady.symbol(), copyReady.side(), copyReady.deltaType(),
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(task.acceptedNs()), elapsedMs(startedNs), queueDepth());
        } finally {
            MDC.remove("traceId");
        }
    }

    private ShadowEnqueueResult enqueueShadowBeforeLive(HyperliquidMappedDelta mappedDelta, long eventReceivedNs) {
        long startedNs = System.nanoTime();
        if (mappedDelta == null || mappedDelta.event() == null) {
            return new ShadowEnqueueResult(false, "SHADOW_PAYLOAD_EMPTY", elapsedMs(startedNs), shadowQueueDepth(), shadowRemainingCapacity(), false, false);
        }
        if (!shadowAsyncEnabled) {
            ShadowSyncResult syncResult = recordShadowSynchronously(mappedDelta, startedNs, eventReceivedNs);
            return new ShadowEnqueueResult(
                    syncResult.recorded() > 0,
                    syncResult.reasonCode(),
                    elapsedMs(startedNs),
                    shadowQueueDepth(),
                    shadowRemainingCapacity(),
                    syncResult.recoveryRequired(),
                    syncResult.durableDeferred()
            );
        }
        long shadowEnqueuedNs = System.nanoTime();
        ShadowTask task = new ShadowTask(
                mappedDelta,
                eventReceivedNs > 0 ? eventReceivedNs : shadowEnqueuedNs,
                shadowEnqueuedNs,
                originTraceId(mappedDelta)
        );
        BlockingQueue<ShadowTask> lane = shadowLanes[shadowLaneFor(mappedDelta)];
        boolean offered;
        try {
            offered = shadowEnqueueTimeoutMs <= 0
                    ? lane.offer(task)
                    : lane.offer(task, shadowEnqueueTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            long elapsedMs = elapsedMs(startedNs);
            log.warn("event=shadow_enqueue_recovery_required reasonCode=SHADOW_ENQUEUE_INTERRUPTED decision=DEFER_TO_DURABLE_REPLAY retryable=true idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} enqueueLatencyMs={} queueDepth={} remainingCapacity={} shadowImpact=RECOVERY_REQUIRED liveImpact=LIVE_NOT_BLOCKED",
                    mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), elapsedMs, shadowQueueDepth(), shadowRemainingCapacity());
            return new ShadowEnqueueResult(false, "SHADOW_ENQUEUE_INTERRUPTED", elapsedMs, shadowQueueDepth(), shadowRemainingCapacity(), true, false);
        }

        long elapsedMs = elapsedMs(startedNs);
        if (!offered) {
            log.warn("event=shadow_enqueue_recovery_required reasonCode=SHADOW_QUEUE_FULL decision=DEFER_TO_DURABLE_REPLAY retryable=true idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} enqueueLatencyMs={} queueDepth={} remainingCapacity={} shadowImpact=RECOVERY_REQUIRED liveImpact=LIVE_NOT_BLOCKED",
                    mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), elapsedMs, shadowQueueDepth(), shadowRemainingCapacity());
            return new ShadowEnqueueResult(false, "SHADOW_QUEUE_FULL", elapsedMs, shadowQueueDepth(), shadowRemainingCapacity(), true, false);
        }

        shadowEnqueued.incrementAndGet();
        recordShadowQueueHighWater(shadowQueueDepth());
        ensureShadowWorkersHealthy("enqueue");
        meterRegistry.counter("signals.copy.shadow.async.enqueued.total", "deltaType", safeTag(mappedDelta.deltaType())).increment();
        meterRegistry.timer("signals.copy.shadow.async.enqueue.duration", Tags.of("result", "ok", "deltaType", safeTag(mappedDelta.deltaType())))
                .record(Duration.ofNanos(System.nanoTime() - startedNs));
        log.info("event=shadow_enqueue_before_live idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} reasonCode=SHADOW_ENQUEUED_BEFORE_LIVE shadowImpact=SHADOW_QUEUED liveImpact=LIVE_NOT_BLOCKED enqueueLatencyMs={} queueDepth={} remainingCapacity={}",
                mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), elapsedMs, shadowQueueDepth(), shadowRemainingCapacity());
        return new ShadowEnqueueResult(true, "SHADOW_ENQUEUED_BEFORE_LIVE", elapsedMs, shadowQueueDepth(), shadowRemainingCapacity(), false, false);
    }

    private ShadowSyncResult recordShadowSynchronously(HyperliquidMappedDelta mappedDelta, long startedNs, long eventReceivedNs) {
        if (mappedDelta == null || mappedDelta.event() == null) {
            return new ShadowSyncResult(0, "SHADOW_SYNC_SKIPPED", false, false);
        }
        try {
            int recorded = shadowCopyTradingService.recordShadowEvent(mappedDelta.event(), eventReceivedNs);
            if (recorded > 0) {
                log.warn("event=shadow_processed_sync_before_live idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} recorded={} reasonCode=SHADOW_SYNC_DEBUG_PATH reasonMessage=\"SHADOW async esta desactivado; LIVE puede esperar este write\" shadowImpact=SHADOW_EVENT_RECORDED elapsedMs={}",
                        mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), recorded, elapsedMs(startedNs));
            }
            return new ShadowSyncResult(
                    recorded,
                    recorded > 0 ? "SHADOW_SYNC_RECORDED" : "SHADOW_SYNC_SKIPPED",
                    false,
                    false
            );
        } catch (RuntimeException ex) {
            boolean durable = persistRecoverableShadowFailure(
                    mappedDelta, "SHADOW_SYNC_PROCESSING_FAILED", ex, 1);
            if (durable) {
                shadowDeferred.incrementAndGet();
                meterRegistry.counter("signals.copy.shadow.async.deferred.total", "reason", "sync_processing_failed").increment();
            }
            log.warn("event=shadow_ingest_failed idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} reasonCode=SHADOW_INGEST_FAILED result={} retryable={} copyImpact=LIVE_NOT_BLOCKED errClass={} errMsg=\"{}\" elapsedMs={}",
                    mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(),
                    durable ? "RECOVERABLE" : "RECOVERY_REQUIRED_AFTER_LIVE", true,
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
            return new ShadowSyncResult(
                    0,
                    durable ? "SHADOW_DEFERRED_TO_DEAD_LETTER" : "SHADOW_SYNC_PROCESSING_FAILED",
                    !durable,
                    durable
            );
        }
    }

    private void logShadowLiveSeparation(HyperliquidMappedDelta mappedDelta, HyperliquidDirectCopyDispatchResult dispatchResult, ShadowEnqueueResult shadowEnqueue) {
        if (mappedDelta == null || dispatchResult == null || shadowEnqueue == null) {
            return;
        }
        if (shadowEnqueue.durableDeferred()) {
            log.info("event=shadow.deferred.durable reasonCode=SHADOW_DEFERRED_TO_DEAD_LETTER decision=REPLAY_SCHEDULED expected=true shouldAlert=false idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} eligibleUsers={} submitted={} shadowQueueDepth={} shadowRemainingCapacity={} liveImpact=LIVE_ALREADY_DISPATCHED shadowImpact=RECOVERABLE",
                    mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(),
                    dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(), shadowEnqueue.queueDepth(), shadowEnqueue.remainingCapacity());
            return;
        }
        if (!shadowEnqueue.enqueued()) {
            log.warn("event=live_continued_after_shadow_enqueue_failed idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} eligibleUsers={} submitted={} shadowReasonCode={} shadowEnqueueLatencyMs={} shadowQueueDepth={} shadowRemainingCapacity={} reasonCode=SHADOW_NOT_IN_HOT_PATH liveImpact=LIVE_CONTINUED_WITHOUT_SHADOW_BLOCK",
                    mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(),
                    safeLog(shadowEnqueue.reasonCode()), shadowEnqueue.enqueueLatencyMs(), shadowEnqueue.queueDepth(), shadowEnqueue.remainingCapacity());
            return;
        }
        if (dispatchResult.eligibleUsers() == 0 && dispatchResult.submittedTasks() == 0) {
            log.info("event=live_skipped_but_shadow_enqueued idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} eligibleUsers={} submitted={} shadowReasonCode={} reasonCode=SHADOW_INDEPENDENT_FROM_LIVE reasonMessage=\"El evento fue encolado en SHADOW aunque no existen usuarios LIVE\" shadowImpact=SHADOW_EVENT_QUEUED liveImpact=NO_LIVE_USERS",
                    mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(), safeLog(shadowEnqueue.reasonCode()));
        }
    }

    private void startShadowWorkers(String reason) {
        if (!shadowAsyncEnabled || !properties.isEnabled()) {
            log.info("event=shadow_async.disabled enabled={} directIngestEnabled={} queueCapacity={} workerThreads={}",
                    shadowAsyncEnabled, properties.isEnabled(), shadowQueueCapacity(), shadowWorkerCount);
            return;
        }
        shadowRunning.set(true);
        for (int i = 0; i < shadowWorkerCount; i++) {
            startShadowWorker(i, reason);
        }
    }

    private void startShadowWorker(int workerIndex, String reason) {
        if (!shadowAsyncEnabled || !shadowRunning.get()) {
            return;
        }
        if (workerIndex < 0 || workerIndex >= shadowWorkerSlots.length) {
            return;
        }
        AtomicBoolean slot = shadowWorkerSlots[workerIndex];
        if (!slot.compareAndSet(false, true)) {
            return;
        }
        if (!"startup".equals(reason)) {
            shadowWorkerRestarts.incrementAndGet();
        }
        try {
            shadowWorkers.execute(() -> shadowWorkerLoop(workerIndex, shadowLanes[workerIndex], reason));
        } catch (RuntimeException ex) {
            slot.set(false);
            shadowFailed.incrementAndGet();
            log.error("event=shadow_worker_start_failed reasonCode=SHADOW_WORKER_START_FAILED workerIndex={} reason={} errClass={} errMsg=\"{}\" queueDepth={} activeShadowWorkers={}",
                    workerIndex, safeLog(reason), ex.getClass().getSimpleName(), safeLog(ex.getMessage()), shadowQueueDepth(), activeShadowWorkers.get(), ex);
        }
    }

    private void ensureShadowWorkersHealthy(String reason) {
        if (!shadowAsyncEnabled || !shadowRunning.get()) {
            return;
        }
        for (int i = 0; i < shadowWorkerSlots.length; i++) {
            if (!shadowWorkerSlots[i].get()) {
                log.warn("event=shadow_worker_missing reasonCode=SHADOW_WORKER_MISSING workerIndex={} reason={} queueDepth={} activeShadowWorkers={} expectedShadowWorkers={} liveImpact=LIVE_NOT_BLOCKED action=restart_worker",
                        i, safeLog(reason), shadowQueueDepth(), activeShadowWorkers.get(), shadowWorkerCount);
                startShadowWorker(i, reason);
            }
        }
    }

    private void shadowWorkerLoop(int workerIndex, BlockingQueue<ShadowTask> lane, String startReason) {
        activeShadowWorkers.incrementAndGet();
        log.info("event=shadow_worker_started workerIndex={} startReason={} queueDepth={} activeShadowWorkers={} expectedShadowWorkers={}",
                workerIndex, safeLog(startReason), shadowQueueDepth(), activeShadowWorkers.get(), shadowWorkerCount);
        try {
            while (shadowRunning.get() || !lane.isEmpty()) {
                ShadowTask task = lane.poll(250, TimeUnit.MILLISECONDS);
                if (task != null) {
                    processShadowTask(task, workerIndex);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.info("event=shadow_worker_interrupted workerIndex={} queueDepth={} activeShadowWorkers={}",
                    workerIndex, shadowQueueDepth(), activeShadowWorkers.get());
        } finally {
            shadowWorkerStops.incrementAndGet();
            int remaining = activeShadowWorkers.decrementAndGet();
            shadowWorkerSlots[workerIndex].set(false);
            log.warn("event=shadow_worker_stopped workerIndex={} queueDepth={} activeShadowWorkers={} expectedShadowWorkers={} running={}",
                    workerIndex, shadowQueueDepth(), remaining, shadowWorkerCount, shadowRunning.get());
        }
    }

    private void processShadowTask(ShadowTask task, int workerIndex) {
        long shadowDequeuedNs = System.nanoTime();
        long queueDelayMs = elapsedMs(task.acceptedNs(), shadowDequeuedNs);
        long startedNs = shadowDequeuedNs;
        HyperliquidMappedDelta mappedDelta = task.mappedDelta();
        meterRegistry.timer("shadow_queue_delay", Tags.of("result", "dequeued"))
                .record(Duration.ofNanos(Math.max(0L, shadowDequeuedNs - task.acceptedNs())));
        try (MDC.MDCCloseable ignored = MDC.putCloseable("traceId", task.traceId())) {
            int recorded = recordShadowWithDeadlockRetry(mappedDelta, task.eventReceivedNs());
            if (recorded > 0) {
                shadowRecorded.addAndGet(recorded);
            } else {
                shadowDuplicates.incrementAndGet();
            }
            long elapsedMs = elapsedMs(startedNs);
            meterRegistry.timer("signals.copy.shadow.async.worker.duration", Tags.of("result", "ok", "deltaType", safeTag(mappedDelta.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            meterRegistry.timer("shadow_processing_duration", Tags.of("stage", "total", "result", "success"))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            if (elapsedMs >= shadowSlowLogMs) {
                log.warn("event=shadow.processing.slow reasonCode=SHADOW_PROCESSING_SLOW workerIndex={} walletId={} profileKey=MULTIPLE strategyCode=MULTIPLE idempotencyKey={} positionKey={} symbol={} side={} deltaType={} recorded={} queueDepth={} queueDelayMs={} lockWaitMs=NA dbPersistMs=NA totalElapsedMs={} slowestStage=SHADOW_PERSIST shouldAlert=false liveImpact=LIVE_NOT_BLOCKED",
                        workerIndex, mappedDelta.wallet(), mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), recorded, shadowQueueDepth(), queueDelayMs, elapsedMs);
            }
            log.info("event=shadow_worker_completed workerIndex={} idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} recorded={} reasonCode={} shadowImpact={} queueDelayMs={} elapsedMs={} queueDepth={} liveImpact=LIVE_NOT_BLOCKED",
                    workerIndex, mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), recorded,
                    recorded > 0 ? "SHADOW_EVENT_RECORDED" : "DUPLICATE_OR_NO_SHADOW_EVENT",
                    recorded > 0 ? "SHADOW_EVENT_RECORDED" : "NO_SHADOW_EVENT",
                    queueDelayMs, elapsedMs, shadowQueueDepth());
        } catch (RuntimeException ex) {
            shadowFailed.incrementAndGet();
            boolean deadlockExhausted = PostgresDeadlockRetryExecutor.isDeadlock(ex);
            int attempts = deadlockExhausted && postgresDeadlockRetryExecutor != null
                    ? postgresDeadlockRetryExecutor.maxAttempts()
                    : 1;
            String recoveryReason = deadlockExhausted
                    ? "SHADOW_DEADLOCK_RETRY_EXHAUSTED"
                    : "SHADOW_WORKER_FAILED";
            boolean durable = persistRecoverableShadowFailure(mappedDelta, recoveryReason, ex, attempts);
            if (durable) {
                shadowDeferred.incrementAndGet();
                meterRegistry.counter("signals.copy.shadow.async.deferred.total", "reason", safeTag(recoveryReason)).increment();
            } else {
                shadowDropped.incrementAndGet();
                meterRegistry.counter("signals.copy.shadow.async.dropped.total", "reason", "worker_recovery_persist_failed").increment();
            }
            meterRegistry.timer("signals.copy.shadow.async.worker.duration", Tags.of("result", "error", "deltaType", safeTag(mappedDelta == null ? null : mappedDelta.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            meterRegistry.timer("shadow_processing_duration", Tags.of("stage", "total", "result", "failure"))
                    .record(Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNs)));
            log.warn("event=shadow_worker_failed reasonCode={} workerIndex={} idempotencyKey={} positionKey={} walletId={} symbol={} side={} deltaType={} attempt={} result={} retryable={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={} liveImpact=LIVE_NOT_BLOCKED",
                    recoveryReason,
                    workerIndex,
                    mappedDelta == null ? "NA" : safeLog(mappedDelta.idempotencyKey()),
                    mappedDelta == null ? "NA" : safeLog(mappedDelta.positionKey()),
                    mappedDelta == null ? "NA" : safeLog(mappedDelta.wallet()),
                    mappedDelta == null ? "NA" : safeLog(mappedDelta.symbol()),
                    mappedDelta == null ? "NA" : safeLog(mappedDelta.side()),
                    mappedDelta == null ? "NA" : safeLog(mappedDelta.deltaType()),
                    attempts,
                    durable ? "RECOVERABLE" : "NOT_DURABLE",
                    durable,
                    ex.getClass().getSimpleName(),
                    safeLog(ex.getMessage()),
                    queueDelayMs,
                    elapsedMs(startedNs),
                    shadowQueueDepth(),
                    ex);
        } finally {
            MDC.remove("traceId");
        }
    }

    private int recordShadowWithDeadlockRetry(HyperliquidMappedDelta mappedDelta, long eventReceivedNs) {
        if (postgresDeadlockRetryExecutor == null) {
            return shadowCopyTradingService.recordShadowEvent(mappedDelta.event(), eventReceivedNs);
        }
        return postgresDeadlockRetryExecutor.execute(
                "shadow_event",
                "shadow_copy_allocation",
                () -> shadowCopyTradingService.recordShadowEvent(mappedDelta.event(), eventReceivedNs)
        );
    }

    private boolean persistRecoverableShadowFailure(HyperliquidMappedDelta mappedDelta, RuntimeException failure) {
        int attempts = postgresDeadlockRetryExecutor == null ? 1 : postgresDeadlockRetryExecutor.maxAttempts();
        return persistRecoverableShadowFailure(mappedDelta, null, failure, attempts);
    }

    private boolean persistRecoverableShadowFailure(
            HyperliquidMappedDelta mappedDelta,
            String reasonCode,
            RuntimeException failure,
            int attempts
    ) {
        if (shadowEventDeadLetterStore == null || mappedDelta == null) {
            log.error("event=shadow_dead_letter_unavailable idempotencyKey={} reasonCode=SHADOW_DEAD_LETTER_STORE_UNAVAILABLE result=NOT_RECOVERABLE",
                    mappedDelta == null ? "NA" : safeLog(mappedDelta.idempotencyKey()));
            return false;
        }
        try {
            shadowEventDeadLetterStore.recordRecoverable(mappedDelta, reasonCode, failure, attempts);
            return true;
        } catch (RuntimeException storeFailure) {
            log.error("event=shadow_dead_letter_failed idempotencyKey={} reasonCode=SHADOW_DEAD_LETTER_PERSIST_FAILED result=NOT_RECOVERABLE errorClass={} errorMessage=\"{}\"",
                    safeLog(mappedDelta.idempotencyKey()), storeFailure.getClass().getSimpleName(), safeLog(storeFailure.getMessage()), storeFailure);
            return false;
        }
    }

    private void blockFlipWithoutExecutionBasis(
            QueuedDelta task,
            HyperliquidMappedDelta mapped,
            HyperliquidFlipExecutionBasisPolicy.Decision decision,
            long startedNs
    ) {
        String reasonCode = HyperliquidFlipExecutionBasisPolicy.MISSING_REASON_CODE;
        HyperliquidDirectCopyDispatchResult blocked = HyperliquidDirectCopyDispatchResult.ok(
                0, 0, 1, 0, false, reasonCode);
        operationMovementEventService.recordAsync(mapped, blocked, reasonCode);
        idempotencyGuard.markProcessed(mapped, reasonCode);
        processed.incrementAndGet();
        meterRegistry.counter(
                "flip_previous_close_missing_total",
                "reason", safeTag(decision.reason()),
                "economic_kind", safeTag(mapped.request() == null
                        ? null
                        : mapped.request().economicEventKind())
        ).increment();
        meterRegistry.counter(
                "estimated_flip_blocked_total",
                "reason", safeTag(decision.reason())
        ).increment();
        meterRegistry.timer(
                "signals.hyperliquid.direct_ingest.process.duration",
                Tags.of("result", "blocked", "deltaType", "FLIP")
        ).record(Duration.ofNanos(System.nanoTime() - startedNs));
        log.warn("event=hyperliquid.direct_ingest.flip_blocked reasonCode={} decision=NO_SHADOW_NO_LIVE auditLedger=true retryable=false copyImpact=FAIL_CLOSED originImpact=NO_NEW_LIFECYCLE idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} economicEventKind={} sourceEstimated={} basisReason={} queueDelayMs={} elapsedMs={} queueDepth={}",
                reasonCode,
                safeLog(mapped.idempotencyKey()),
                safeLog(mapped.positionKey()),
                safeLog(mapped.wallet()),
                safeLog(mapped.symbol()),
                safeLog(mapped.side()),
                safeLog(mapped.deltaType()),
                safeLog(mapped.request() == null ? null : mapped.request().economicEventKind()),
                mapped.request() == null ? null : mapped.request().sourceEstimated(),
                safeLog(decision.reason()),
                elapsedMs(task.acceptedNs()),
                elapsedMs(startedNs),
                queueDepth());
    }

    private String originTraceId(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null) {
            return CopyTraceIdUtil.copyTraceId("NA", "origin", "NA", "NA");
        }
        String originId = "NA";
        String wallet = mappedDelta.wallet();
        String symbol = mappedDelta.symbol();
        if (mappedDelta.event() != null && mappedDelta.event().getOperacion() != null) {
            if (mappedDelta.event().getOperacion().getIdOperacion() != null) {
                originId = mappedDelta.event().getOperacion().getIdOperacion().toString();
            }
            wallet = firstNonBlank(wallet, mappedDelta.event().getOperacion().getIdCuenta(), "NA");
            symbol = firstNonBlank(symbol, mappedDelta.event().getOperacion().getParSymbol(), "NA");
        }
        return CopyTraceIdUtil.copyTraceId(originId, "origin", wallet, symbol);
    }

    private HyperliquidDeltaAcceptedResponse response(HyperliquidMappedDelta mappedDelta, boolean duplicate) {
        return HyperliquidDeltaAcceptedResponse.accepted(
                mappedDelta.idempotencyKey(),
                mappedDelta.positionKey(),
                mappedDelta.wallet(),
                mappedDelta.symbol(),
                mappedDelta.side(),
                mappedDelta.deltaType(),
                duplicate,
                queueDepth()
        );
    }

    private HyperliquidDirectIngestRejectedException rejected(String reason, HyperliquidMappedDelta mappedDelta, int queueDepth) {
        return new HyperliquidDirectIngestRejectedException(
                "Hyperliquid direct ingest rejected: " + reason,
                Map.of(
                        "reason", reason,
                        "idempotencyKey", mappedDelta.idempotencyKey(),
                        "positionKey", mappedDelta.positionKey(),
                        "wallet", mappedDelta.wallet(),
                        "symbol", mappedDelta.symbol(),
                        "side", mappedDelta.side(),
                        "deltaType", mappedDelta.deltaType(),
                        "queueDepth", queueDepth,
                        "queueCapacity", properties.getQueueCapacity()
                )
        );
    }

    private void incrementDuplicateMetric(String deltaType, String scope) {
        meterRegistry.counter(
                "signals.hyperliquid.direct_ingest.duplicates.total",
                "deltaType", safeTag(deltaType),
                "scope", safeTag(scope)
        ).increment();
    }

    private void registerMetrics() {
        Gauge.builder("signals.hyperliquid.direct_ingest.queue.depth", this, svc -> svc.queueDepth())
                .description("Hyperliquid direct ingest queue depth")
                .register(meterRegistry);
        Gauge.builder("worker_queue_depth", this, svc -> svc.queueDepth())
                .tag("worker", "direct_ingest")
                .description("Semantic pipeline worker queue depth")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.direct_ingest.workers.active", activeWorkers, a -> a.get())
                .description("Hyperliquid direct ingest active worker loops")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.direct_ingest.accepted.total.gauge", accepted, a -> a.get())
                .description("Hyperliquid direct ingest accepted counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.direct_ingest.processed.total.gauge", processed, a -> a.get())
                .description("Hyperliquid direct ingest processed counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.direct_ingest.failed.total.gauge", failed, a -> a.get())
                .description("Hyperliquid direct ingest failed counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.direct_ingest.queue.high_water_mark", queueHighWaterMark, AtomicLong::get)
                .description("Hyperliquid direct ingest queue high-water mark")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.direct_ingest.workers.uncaught_failures", workerUncaughtFailures, AtomicLong::get)
                .description("Hyperliquid direct ingest uncaught worker failures")
                .register(meterRegistry);
        Gauge.builder("signals.hyperliquid.direct_ingest.workers.restarts", workerRestarts, AtomicLong::get)
                .description("Hyperliquid direct ingest self-healed worker restarts")
                .register(meterRegistry);
        Gauge.builder("signals.copy.shadow.async.queue.depth", this, svc -> svc.shadowQueueDepth())
                .description("Async SHADOW queue depth")
                .register(meterRegistry);
        Gauge.builder("signals.copy.shadow.async.queue.high_water_mark", shadowQueueHighWaterMark, AtomicLong::get)
                .description("Async SHADOW queue high-water mark")
                .register(meterRegistry);
        Gauge.builder("signals.copy.shadow.async.workers.active", activeShadowWorkers, AtomicInteger::get)
                .description("Async SHADOW active worker loops")
                .register(meterRegistry);
        Gauge.builder("signals.copy.shadow.async.enqueued.total.gauge", shadowEnqueued, AtomicLong::get)
                .description("Async SHADOW enqueued counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.copy.shadow.async.recorded.total.gauge", shadowRecorded, AtomicLong::get)
                .description("Async SHADOW recorded counter gauge")
                .register(meterRegistry);
        Gauge.builder("signals.copy.shadow.async.deferred.total.gauge", shadowDeferred, AtomicLong::get)
                .description("Async SHADOW events durably deferred for replay")
                .register(meterRegistry);
        Gauge.builder("signals.copy.shadow.async.dropped.total.gauge", shadowDropped, AtomicLong::get)
                .description("Async SHADOW events whose durable recovery could not be persisted")
                .register(meterRegistry);
        Gauge.builder("signals.copy.shadow.async.failed.total.gauge", shadowFailed, AtomicLong::get)
                .description("Async SHADOW failed counter gauge")
                .register(meterRegistry);
    }

    private void recordQueueHighWater(int depth) {
        if (depth > 0) {
            queueHighWaterMark.accumulateAndGet(depth, Math::max);
        }
    }

    private void recordShadowQueueHighWater(int depth) {
        if (depth > 0) {
            shadowQueueHighWaterMark.accumulateAndGet(depth, Math::max);
        }
    }


    private int laneFor(HyperliquidMappedDelta mappedDelta, String dedupeKey) {
        String key = firstNonBlank(
                mappedDelta == null ? null : mappedDelta.positionKey(),
                mappedDelta == null ? null : String.join("|",
                        normalizeKey(mappedDelta.wallet()),
                        normalizeKey(mappedDelta.symbol()),
                        normalizeKey(mappedDelta.side())),
                dedupeKey
        );
        return Math.floorMod(key.hashCode(), laneCount);
    }

    private int queueDepth() {
        int total = 0;
        for (BlockingQueue<QueuedDelta> lane : lanes) {
            total += lane.size();
        }
        return total;
    }

    private int shadowQueueDepth() {
        int total = 0;
        for (BlockingQueue<ShadowTask> lane : shadowLanes) {
            total += lane.size();
        }
        return total;
    }

    private int shadowRemainingCapacity() {
        int total = 0;
        for (BlockingQueue<ShadowTask> lane : shadowLanes) {
            total += lane.remainingCapacity();
        }
        return total;
    }

    private int shadowQueueCapacity() {
        return shadowQueueDepth() + shadowRemainingCapacity();
    }

    private int shadowLaneFor(HyperliquidMappedDelta mappedDelta) {
        String key = firstNonBlank(
                mappedDelta == null ? null : mappedDelta.positionKey(),
                mappedDelta == null ? null : String.join("|",
                        normalizeKey(mappedDelta.wallet()),
                        normalizeKey(mappedDelta.symbol()),
                        normalizeKey(mappedDelta.side())),
                firstNonBlank(mappedDelta == null ? null : mappedDelta.idempotencyKey(), null, "shadow:missing")
        );
        return Math.floorMod(key.hashCode(), shadowWorkerCount);
    }

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private long elapsedMs(long startedNs, long finishedNs) {
        return Duration.ofNanos(Math.max(0L, finishedNs - startedNs)).toMillis();
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
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }

    private record QueuedDelta(HyperliquidMappedDelta mappedDelta, String dedupeKey, long acceptedNs) {
    }

    private record ShadowTask(HyperliquidMappedDelta mappedDelta, long eventReceivedNs, long acceptedNs, String traceId) {
    }

    private record ShadowSyncResult(
            int recorded,
            String reasonCode,
            boolean recoveryRequired,
            boolean durableDeferred
    ) {
    }

    private record ShadowEnqueueResult(
            boolean enqueued,
            String reasonCode,
            long enqueueLatencyMs,
            int queueDepth,
            int remainingCapacity,
            boolean recoveryRequired,
            boolean durableDeferred
    ) {
        private ShadowEnqueueResult asDurableDeferred() {
            return new ShadowEnqueueResult(
                    false,
                    "SHADOW_DEFERRED_TO_DEAD_LETTER",
                    enqueueLatencyMs,
                    queueDepth,
                    remainingCapacity,
                    false,
                    true
            );
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicLong sequence = new AtomicLong(1L);

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName(prefix + sequence.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
