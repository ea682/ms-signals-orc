package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.hyperliquid.config.HyperliquidDirectIngestProperties;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaAcceptedResponse;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.exception.HyperliquidDirectIngestRejectedException;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectDeltaIngestService;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectCopyDispatchService;
import com.apunto.engine.shared.exception.EngineException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.Map;
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
public class HyperliquidDirectDeltaIngestServiceImpl implements HyperliquidDirectDeltaIngestService {

    private final HyperliquidDirectIngestProperties properties;
    private final HyperliquidDirectCopyDispatchService directCopyDispatchService;
    private final HyperliquidOriginPositionStoreService originPositionStoreService;
    private final MeterRegistry meterRegistry;
    private final BlockingQueue<QueuedDelta> queue;
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
            MeterRegistry meterRegistry
    ) {
        this.properties = properties;
        this.directCopyDispatchService = directCopyDispatchService;
        this.originPositionStoreService = originPositionStoreService;
        this.meterRegistry = meterRegistry;
        this.queue = new ArrayBlockingQueue<>(Math.max(1, properties.getQueueCapacity()));
        this.workers = Executors.newFixedThreadPool(
                Math.max(1, properties.getWorkerThreads()),
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
        for (int i = 0; i < Math.max(1, properties.getWorkerThreads()); i++) {
            workers.execute(this::workerLoop);
        }
        log.info("event=hyperliquid.direct_ingest.started queueCapacity={} workerThreads={} dedupeEnabled={} dedupeTtlSeconds={}",
                properties.getQueueCapacity(), properties.getWorkerThreads(), properties.isDedupeEnabled(), properties.getDedupeTtlSeconds());
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        workers.shutdownNow();
        log.info("event=hyperliquid.direct_ingest.stopped queueDepth={} accepted={} processed={} failed={} duplicates={}",
                queue.size(), accepted.get(), processed.get(), failed.get(), duplicates.get());
    }

    @Scheduled(fixedDelayString = "${hyperliquid.direct-ingest.log-interval-ms:10000}")
    public void logMetrics() {
        if (!properties.isEnabled()) {
            return;
        }
        log.info("event=hyperliquid.direct_ingest.metrics queueDepth={} queueCapacity={} activeWorkers={} accepted={} processed={} failed={} duplicates={}",
                queue.size(), properties.getQueueCapacity(), activeWorkers.get(), accepted.get(), processed.get(), failed.get(), duplicates.get());
    }

    @Override
    public HyperliquidDeltaAcceptedResponse accept(HyperliquidMappedDelta mappedDelta) {
        if (mappedDelta == null) {
            throw new IllegalArgumentException("mappedDelta is required");
        }
        if (!properties.isEnabled()) {
            if (properties.isRejectWhenDisabled()) {
                throw rejected("direct_ingest_disabled", mappedDelta, queue.size());
            }
            return response(mappedDelta, false);
        }

        if (properties.isDedupeEnabled() && recentKeys.asMap().putIfAbsent(mappedDelta.idempotencyKey(), Boolean.TRUE) != null) {
            duplicates.incrementAndGet();
            meterRegistry.counter("signals.hyperliquid.direct_ingest.duplicates.total", "deltaType", safeTag(mappedDelta.deltaType())).increment();
            log.debug("event=hyperliquid.direct_ingest.duplicate idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} queueDepth={}",
                    mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), queue.size());
            return response(mappedDelta, true);
        }

        QueuedDelta queuedDelta = new QueuedDelta(mappedDelta, System.nanoTime());
        if (!queue.offer(queuedDelta)) {
            if (properties.isDedupeEnabled()) {
                recentKeys.invalidate(mappedDelta.idempotencyKey());
            }
            meterRegistry.counter("signals.hyperliquid.direct_ingest.rejected.total", "reason", "queue_full").increment();
            throw rejected("queue_full", mappedDelta, queue.size());
        }

        accepted.incrementAndGet();
        meterRegistry.counter("signals.hyperliquid.direct_ingest.accepted.total", "deltaType", safeTag(mappedDelta.deltaType())).increment();
        log.debug("event=hyperliquid.direct_ingest.accepted idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} queueDepth={}",
                mappedDelta.idempotencyKey(), mappedDelta.positionKey(), mappedDelta.wallet(), mappedDelta.symbol(), mappedDelta.side(), mappedDelta.deltaType(), queue.size());
        return response(mappedDelta, false);
    }

    private void workerLoop() {
        activeWorkers.incrementAndGet();
        try {
            while (running.get() || !queue.isEmpty()) {
                QueuedDelta task = queue.take();
                process(task);
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
        try {
            HyperliquidMappedDelta copyReady = originPositionStoreService.bindOriginIdForCopy(mapped);
            HyperliquidDirectCopyDispatchResult dispatchResult = directCopyDispatchService.dispatch(copyReady);
            originPositionStoreService.submitAfterCopy(copyReady, dispatchResult);
            processed.incrementAndGet();
            long elapsedMs = elapsedMs(startedNs);
            long queueDelayMs = elapsedMs(task.acceptedNs());
            meterRegistry.timer("signals.hyperliquid.direct_ingest.process.duration", Tags.of("result", "ok", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            log.info("event=hyperliquid.direct_ingest.processed idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} eligibleUsers={} submittedTasks={} businessSkipped={} fallbackJobs={} fallbackUsed={} queueDelayMs={} elapsedMs={} queueDepth={}",
                    mapped.idempotencyKey(), mapped.positionKey(), mapped.wallet(), mapped.symbol(), mapped.side(), mapped.deltaType(),
                    dispatchResult.eligibleUsers(), dispatchResult.submittedTasks(), dispatchResult.businessSkipped(), dispatchResult.fallbackJobs(), dispatchResult.fallbackUsed(),
                    queueDelayMs, elapsedMs, queue.size());
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            failed.incrementAndGet();
            meterRegistry.timer("signals.hyperliquid.direct_ingest.process.duration", Tags.of("result", "error", "deltaType", safeTag(mapped.deltaType())))
                    .record(Duration.ofNanos(System.nanoTime() - startedNs));
            log.error("event=hyperliquid.direct_ingest.failed idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} errClass={} errMsg=\"{}\" queueDelayMs={} elapsedMs={} queueDepth={}",
                    mapped.idempotencyKey(), mapped.positionKey(), mapped.wallet(), mapped.symbol(), mapped.side(), mapped.deltaType(),
                    ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(task.acceptedNs()), elapsedMs(startedNs), queue.size());
        }
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
                queue.size()
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
        Gauge.builder("signals.hyperliquid.direct_ingest.queue.depth", queue, q -> q.size())
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

    private record QueuedDelta(HyperliquidMappedDelta mappedDelta, long acceptedNs) {
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
