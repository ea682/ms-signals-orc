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
    private final HyperliquidOriginPositionStoreService originPositionStoreService;
    private final OperationMovementEventService operationMovementEventService;
    private final MeterRegistry meterRegistry;
    private final int laneCount;
    private final BlockingQueue<QueuedDelta>[] lanes;
    private final ExecutorService workers;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicLong accepted = new AtomicLong(0);
    private final AtomicLong processed = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);
    private final AtomicLong duplicates = new AtomicLong(0);
    private final Cache<String, Boolean> recentKeys;

    public HyperliquidDirectDeltaIngestServiceImpl(
            HyperliquidDirectIngestProperties properties,
            HyperliquidDirectCopyDispatchService directCopyDispatchService,
            HyperliquidOriginPositionStoreService originPositionStoreService,
            OperationMovementEventService operationMovementEventService,
            MeterRegistry meterRegistry
    ) {
        this.properties = properties;
        this.directCopyDispatchService = directCopyDispatchService;
        this.originPositionStoreService = originPositionStoreService;
        this.operationMovementEventService = operationMovementEventService;
        this.meterRegistry = meterRegistry;
        this.laneCount = Math.max(1, properties.getWorkerThreads());
        int perLaneCapacity = Math.max(1, (Math.max(1, properties.getQueueCapacity()) + laneCount - 1) / laneCount);
        this.lanes = new BlockingQueue[laneCount];
        for (int i = 0; i < laneCount; i++) {
            this.lanes[i] = new ArrayBlockingQueue<>(perLaneCapacity);
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
            final BlockingQueue<QueuedDelta> lane = lanes[i];
            workers.execute(() -> workerLoop(lane));
        }
        log.info("event=hyperliquid.direct_ingest.started queueCapacity={} workerThreads={} dedupeEnabled={} dedupeTtlSeconds={}",
                properties.getQueueCapacity(), properties.getWorkerThreads(), properties.isDedupeEnabled(), properties.getDedupeTtlSeconds());
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
        log.info("event=hyperliquid.direct_ingest.metrics queueDepth={} queueCapacity={} activeWorkers={} accepted={} processed={} failed={} duplicates={}",
                queueDepth(), properties.getQueueCapacity(), activeWorkers.get(), accepted.get(), processed.get(), failed.get(), duplicates.get());
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
            meterRegistry.counter("signals.hyperliquid.direct_ingest.duplicates.total", "deltaType", safeTag(mappedDelta.deltaType())).increment();
            log.info("event=hyperliquid.direct_ingest.duplicate dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} queueDepth={}",
                    dedupeKey, mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), queueDepth());
            return response(mappedDelta, true);
        }

        QueuedDelta queuedDelta = new QueuedDelta(mappedDelta, dedupeKey, System.nanoTime());
        BlockingQueue<QueuedDelta> lane = lanes[laneFor(mappedDelta, dedupeKey)];
        if (!lane.offer(queuedDelta)) {
            if (properties.isDedupeEnabled()) {
                recentKeys.invalidate(dedupeKey);
            }
            meterRegistry.counter("signals.hyperliquid.direct_ingest.rejected.total", "reason", "queue_full").increment();
            throw rejected("queue_full", mappedDelta, queueDepth());
        }

        accepted.incrementAndGet();
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

    private void workerLoop(BlockingQueue<QueuedDelta> lane) {
        activeWorkers.incrementAndGet();
        try {
            while (running.get() || !lane.isEmpty()) {
                QueuedDelta task = lane.poll(250, TimeUnit.MILLISECONDS);
                if (task != null) {
                    process(task);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            activeWorkers.decrementAndGet();
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
            operationMovementEventService.recordAsync(copyReady, dispatchResult, null);
            originPositionStoreService.submitAfterCopy(copyReady, dispatchResult);
            processed.incrementAndGet();
            long elapsedMs = elapsedMs(startedNs);
            long queueDelayMs = elapsedMs(task.acceptedNs());
            meterRegistry.timer("signals.hyperliquid.direct_ingest.process.duration", Tags.of("result", "ok", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            log.info("event=hyperliquid.direct_ingest.processed dedupeKey={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} eligibleUsers={} submittedTasks={} businessSkipped={} fallbackJobs={} fallbackUsed={} queueDelayMs={} elapsedMs={} queueDepth={}",
                    task.dedupeKey(), copyReady.idempotencyKey(), copyReady.positionKey(), copyReady.wallet(), copyReady.symbol(), copyReady.side(), copyReady.deltaType(),
                    dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(), dispatchResult.businessSkipped(), dispatchResult.fallbackJobs(), dispatchResult.fallbackUsed(),
                    queueDelayMs, elapsedMs, queueDepth());
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            failed.incrementAndGet();
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
