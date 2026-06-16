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
    private final MeterRegistry meterRegistry;
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

    public HyperliquidDirectDeltaIngestServiceImpl(
            HyperliquidDirectIngestProperties properties,
            HyperliquidDirectCopyDispatchService directCopyDispatchService,
            HyperliquidDirectIngestIdempotencyGuard idempotencyGuard,
            HyperliquidOriginPositionStoreService originPositionStoreService,
            OperationMovementEventService operationMovementEventService,
            MeterRegistry meterRegistry
    ) {
        this.properties = properties;
        this.directCopyDispatchService = directCopyDispatchService;
        this.idempotencyGuard = idempotencyGuard;
        this.originPositionStoreService = originPositionStoreService;
        this.operationMovementEventService = operationMovementEventService;
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
        log.info("event=hyperliquid.direct_ingest.started queueCapacity={} workerThreads={} dedupeEnabled={} dedupeTtlSeconds={} distributedDedupeEnabled={} dedupeLeaseTtlMs={} failOpenOnDedupeError={} humanMessage=direct_ingest_listo_con_idempotencia_para_varias_instancias",
                properties.getQueueCapacity(), properties.getWorkerThreads(), properties.isDedupeEnabled(), properties.getDedupeTtlSeconds(),
                properties.isDistributedDedupeEnabled(), properties.getDedupeLeaseTtlMs(), properties.isFailOpenOnDedupeError());
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        workers.shutdownNow();
        log.info("event=hyperliquid.direct_ingest.stopped queueDepth={} accepted={} processed={} failed={} duplicates={}",
                queueDepth(), accepted.get(), processed.get(), failed.get(), duplicates.get());
    }

    @Scheduled(fixedDelayString = "${hyperliquid.direct-ingest.log-interval-ms:10000}")
    public void logMetrics() {
        if (!properties.isEnabled()) {
            return;
        }
        ensureWorkersHealthy("metrics");
        int queueDepth = queueDepth();
        log.info("event=hyperliquid.direct_ingest.metrics queueDepth={} queueCapacity={} activeWorkers={} expectedWorkers={} workerRestarts={} workerStops={} workerUncaughtFailures={} accepted={} processed={} failed={} duplicates={} queueHighWaterMark={}",
                queueDepth, properties.getQueueCapacity(), activeWorkers.get(), laneCount, workerRestarts.get(), workerStops.get(), workerUncaughtFailures.get(), accepted.get(), processed.get(), failed.get(), duplicates.get(), queueHighWaterMark.get());
        if (properties.getQueueCapacity() > 0 && queueDepth >= Math.max(1, (int) (properties.getQueueCapacity() * 0.80d))) {
            log.warn("event=hyperliquid.direct_ingest.queue_pressure reasonCode=direct_ingest_queue_pressure queueDepth={} queueCapacity={} activeWorkers={} expectedWorkers={} failed={} duplicates={} action=scale_workers_or_check_downstream",
                    queueDepth, properties.getQueueCapacity(), activeWorkers.get(), laneCount, failed.get(), duplicates.get());
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
            copyReady = originPositionStoreService.bindOriginIdForCopy(mapped);
            MDC.put("traceId", originTraceId(copyReady));
            HyperliquidDirectCopyDispatchResult dispatchResult = directCopyDispatchService.dispatch(copyReady);
            operationMovementEventService.recordAsync(copyReady, dispatchResult, dispatchResult.reasonCode());
            originPositionStoreService.submitAfterCopy(copyReady, dispatchResult);
            idempotencyGuard.markProcessed(copyReady, dispatchResult.reasonCode());
            processed.incrementAndGet();
            long elapsedMs = elapsedMs(startedNs);
            long queueDelayMs = elapsedMs(task.acceptedNs());
            meterRegistry.timer("signals.hyperliquid.direct_ingest.process.duration", Tags.of("result", "ok", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            String copySkipReasonCode = dispatchResult.reasonCode();
            String copySkipDiagnostic = copySkipReasonCode == null ? "" : CopyLogAdvice.fields(
                    copySkipReasonCode,
                    CopyLogAdvice.context(dispatchResult.eligibleUsers(), dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(), dispatchResult.businessSkipped(), queueDepth(), null, null, "direct_ingest")
            );
            log.info("event=hyperliquid.direct_ingest.processed dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} eligibleUsers={} submittedTasks={} businessSkipped={} fallbackJobs={} fallbackUsed={} copySkipReasonCode={} queueDelayMs={} elapsedMs={} queueDepth={} {}",
                    task.dedupeKey(), copyReady.idempotencyKey(), copyReady.positionKey(), copyReady.wallet(), copyReady.symbol(), copyReady.side(), copyReady.deltaType(),
                    dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(), dispatchResult.businessSkipped(), dispatchResult.fallbackJobs(), dispatchResult.fallbackUsed(), safeLog(copySkipReasonCode),
                    queueDelayMs, elapsedMs, queueDepth(), copySkipDiagnostic);
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            failed.incrementAndGet();
            idempotencyGuard.markFailed(copyReady, "direct_ingest_processing_failed", ex);
            MDC.put("traceId", originTraceId(copyReady));
            meterRegistry.timer("signals.hyperliquid.direct_ingest.process.duration", Tags.of("result", "error", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            log.error("event=hyperliquid.direct_ingest.failed dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={}",
                    task.dedupeKey(), copyReady.idempotencyKey(), copyReady.positionKey(), copyReady.wallet(), copyReady.symbol(), copyReady.side(), copyReady.deltaType(),
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(task.acceptedNs()), elapsedMs(startedNs), queueDepth());
        } finally {
            MDC.remove("traceId");
        }
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
    }

    private void recordQueueHighWater(int depth) {
        if (depth > 0) {
            queueHighWaterMark.accumulateAndGet(depth, Math::max);
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

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
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
