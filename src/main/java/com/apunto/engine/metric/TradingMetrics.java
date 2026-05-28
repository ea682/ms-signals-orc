package com.apunto.engine.metric;

import com.apunto.engine.entity.CopyExecutionJobEntity;
import com.apunto.engine.jobs.model.CopyJobStatus;
import com.apunto.engine.repository.CopyExecutionJobRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.ThreadPoolExecutor;

@Component
public class TradingMetrics {

    private final MeterRegistry registry;

    public TradingMetrics(
            MeterRegistry registry,
            CopyExecutionJobRepository repository,
            @Qualifier("copyJobExecutor") ThreadPoolTaskExecutor executor
    ) {
        this.registry = registry;

        Gauge.builder("signals.copy.jobs.pending", repository, r -> r.countByStatus(CopyJobStatus.PENDING))
                .description("Jobs pendientes")
                .register(registry);

        Gauge.builder("signals.copy.jobs.processing", repository, r -> r.countByStatus(CopyJobStatus.PROCESSING))
                .description("Jobs en procesamiento")
                .register(registry);

        Gauge.builder("signals.copy.executor.active", executor,
                        e -> threadPool(e) == null ? 0 : threadPool(e).getActiveCount())
                .description("Hilos activos del executor")
                .register(registry);

        Gauge.builder("signals.copy.executor.queue.size", executor,
                        e -> threadPool(e) == null ? 0 : threadPool(e).getQueue().size())
                .description("Tamano de la cola del executor")
                .register(registry);
    }

    public void kafkaReceived(String topic) {
        registry.counter("signals.kafka.received.total", "topic", safeTag(topic)).increment();
    }

    public void jobsEnqueued(String action, int usersCached, int enqueued) {
        registry.summary("signals.copy.users.cached").record(usersCached);
        registry.summary("signals.copy.jobs.enqueued.batch", "action", safeTag(action)).record(enqueued);

        if (enqueued > 0) {
            registry.counter("signals.copy.jobs.enqueued.total", "action", safeTag(action))
                    .increment(enqueued);
        }
    }

    public void ingestDuration(String tipo, String result, long nanos) {
        registry.timer("signals.copy.ingest.duration",
                        "tipo", safeTag(tipo),
                        "result", safeTag(result))
                .record(Duration.ofNanos(nanos));
    }

    public void claimedBatch(int batchSize) {
        registry.summary("signals.copy.job.claim.batch").record(batchSize);
    }

    public void jobQueueWait(CopyExecutionJobEntity job) {
        if (job.getCreatedAt() == null || job.getAction() == null) return;

        Duration wait = Duration.between(job.getCreatedAt(), OffsetDateTime.now());
        if (!wait.isNegative()) {
            registry.timer("signals.copy.job.queue.wait", "action", job.getAction().name())
                    .record(wait);
        }
    }

    public void jobExecution(CopyExecutionJobEntity job, String result, long nanos) {
        String action = job.getAction() == null ? "UNKNOWN" : job.getAction().name();

        registry.timer("signals.copy.job.execution",
                        "action", action,
                        "result", safeTag(result))
                .record(Duration.ofNanos(nanos));
    }

    public void jobResult(CopyExecutionJobEntity job, String result, String reason) {
        String action = job.getAction() == null ? "UNKNOWN" : job.getAction().name();
        String normalizedReason = (reason == null || reason.isBlank()) ? "none" : reason;

        registry.counter("signals.copy.job.result.total",
                        "action", action,
                        "result", safeTag(result),
                        "reason", safeTag(normalizedReason))
                .increment();
    }


    public void directCopyDispatch(String intent, int eligibleUsers, int submitted, int businessSkipped, int fallbackJobs, boolean fallbackUsed, String reasonCode, long elapsedMs) {
        registry.summary("signals.hyperliquid.direct_copy.eligible_users").record(Math.max(0, eligibleUsers));
        registry.summary("signals.hyperliquid.direct_copy.submitted.batch", "intent", safeTag(intent)).record(Math.max(0, submitted));
        registry.summary("signals.hyperliquid.direct_copy.business_skipped.batch", "intent", safeTag(intent)).record(Math.max(0, businessSkipped));
        registry.timer("signals.hyperliquid.direct_copy.dispatch.duration",
                        "intent", safeTag(intent),
                        "reason", safeTag(reasonOrNone(reasonCode)),
                        "fallback_used", String.valueOf(fallbackUsed))
                .record(Duration.ofMillis(Math.max(0L, elapsedMs)));

        if (submitted > 0) {
            registry.counter("signals.hyperliquid.direct_copy.submitted.total",
                            "intent", safeTag(intent))
                    .increment(submitted);
        }
        if (businessSkipped > 0) {
            registry.counter("signals.hyperliquid.direct_copy.business_skipped.total",
                            "intent", safeTag(intent),
                            "reason", safeTag(reasonOrNone(reasonCode)))
                    .increment(businessSkipped);
        }
        if (fallbackJobs > 0) {
            registry.counter("signals.hyperliquid.direct_copy.fallback_jobs.total",
                            "intent", safeTag(intent),
                            "reason", safeTag(reasonOrNone(reasonCode)))
                    .increment(fallbackJobs);
        }
    }

    public void directCopyExecution(String intent, String result, String reasonCode, long elapsedMs) {
        registry.counter("signals.hyperliquid.direct_copy.execution.total",
                        "intent", safeTag(intent),
                        "result", safeTag(result),
                        "reason", safeTag(reasonOrNone(reasonCode)))
                .increment();
        registry.timer("signals.hyperliquid.direct_copy.execution.duration",
                        "intent", safeTag(intent),
                        "result", safeTag(result),
                        "reason", safeTag(reasonOrNone(reasonCode)))
                .record(Duration.ofMillis(Math.max(0L, elapsedMs)));
    }

    public void directCopyRejected(String intent, String reasonCode) {
        registry.counter("signals.hyperliquid.direct_copy.rejected.total",
                        "intent", safeTag(intent),
                        "reason", safeTag(reasonOrNone(reasonCode)))
                .increment();
    }

    public void directCopyFallback(String result, String reasonCode) {
        registry.counter("signals.hyperliquid.direct_copy.fallback.total",
                        "result", safeTag(result),
                        "reason", safeTag(reasonOrNone(reasonCode)))
                .increment();
    }

    private String reasonOrNone(String reason) {
        return reason == null || reason.isBlank() ? "none" : reason;
    }

    private ThreadPoolExecutor threadPool(ThreadPoolTaskExecutor executor) {
        return executor.getThreadPoolExecutor();
    }

    private String safeTag(String value) {
        if (value == null || value.isBlank()) return "unknown";
        return value;
    }
}
