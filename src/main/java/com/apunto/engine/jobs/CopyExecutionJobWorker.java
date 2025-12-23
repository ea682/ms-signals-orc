package com.apunto.engine.jobs;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.entity.CopyExecutionJobEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.jobs.model.CopyJobErrorCategory;
import com.apunto.engine.service.BinanceCopyExecutionService;
import com.apunto.engine.service.CopyExecutionJobService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.shared.exception.BinanceRateLimitException;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

import java.net.InetAddress;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
@Component
@ConditionalOnProperty(
        name = "engine.copy.execution-job-worker.enabled",
        havingValue = "true"
)
public class CopyExecutionJobWorker {

    private static final Duration STALE_LOCK_TTL = Duration.ofMinutes(10);
    private static final Duration BACKOFF_BASE = Duration.ofSeconds(1);
    private static final Duration BACKOFF_MAX = Duration.ofMinutes(2);
    private static final Duration BACKOFF_RATE_LIMIT_BASE = Duration.ofSeconds(5);
    private static final Duration BACKOFF_RATE_LIMIT_MAX = Duration.ofMinutes(5);

    private final CopyExecutionJobService jobService;
    private final BinanceCopyExecutionService binanceCopyExecutionService;
    private final UserDetailCachedService userDetailCachedService;
    private final ObjectMapper objectMapper;
    private final ThreadPoolTaskExecutor executor;

    private final String workerId;

    @Value("${copy.job.worker.max-batch:50}")
    private int maxBatch;

    @Value("${copy.job.worker.max-attempts:10}")
    private int maxAttempts;

    public CopyExecutionJobWorker(
            CopyExecutionJobService jobService,
            BinanceCopyExecutionService binanceCopyExecutionService,
            UserDetailCachedService userDetailCachedService,
            ObjectMapper objectMapper,
            @Qualifier("copyJobExecutor") ThreadPoolTaskExecutor executor
    ) {
        this.jobService = jobService;
        this.binanceCopyExecutionService = binanceCopyExecutionService;
        this.userDetailCachedService = userDetailCachedService;
        this.objectMapper = objectMapper;
        this.executor = executor;
        this.workerId = buildWorkerId();
    }

    @Scheduled(fixedDelayString = "${copy.job.worker.poll-ms:250}")
    public void tick() {
        try {
            OffsetDateTime now = OffsetDateTime.now();

            int requeued = jobService.requeueStaleProcessing(now.minus(STALE_LOCK_TTL));
            if (requeued > 0) {
                ExecSnapshot s = execSnapshot();
                log.warn("event=copy.job.requeued_stale workerId={} count={} staleTtlSec={} execPool={} execActive={} execQueue={} execQueueRemaining={}",
                        workerId, requeued, STALE_LOCK_TTL.toSeconds(),
                        s.poolSize(), s.activeCount(), s.queueSize(), s.queueRemaining());
            }

            List<CopyExecutionJobEntity> jobs = jobService.claimBatch(workerId, maxBatch);
            if (jobs.isEmpty()) return;

            ExecSnapshot s = execSnapshot();
            log.info("event=copy.job.claimed workerId={} batch={} execPool={} execActive={} execQueue={} execQueueRemaining={}",
                    workerId, jobs.size(),
                    s.poolSize(), s.activeCount(), s.queueSize(), s.queueRemaining());

            for (CopyExecutionJobEntity job : jobs) {
                submitOrRescheduleOnReject(job);
            }

        } catch (Exception e) {
            log.error("event=copy.tick.error workerId={} errClass={} err={}",
                    workerId, e.getClass().getSimpleName(), e.toString(), e);
        }
    }

    private void submitOrRescheduleOnReject(CopyExecutionJobEntity job) {
        try {
            executor.execute(() -> process(job));
        } catch (RejectedExecutionException rej) {
            // IMPORTANT: si el executor rechaza, el job ya está en PROCESSING.
            // Lo volvemos a PENDING inmediatamente para no dejarlo pegado esperando el stale TTL.
            long delayMs = 250L + ThreadLocalRandom.current().nextInt(750);
            OffsetDateTime nextRunAt = OffsetDateTime.now().plus(Duration.ofMillis(delayMs));

            jobService.reschedule(job, nextRunAt, CopyJobErrorCategory.REJECTED.name(), "executor_rejected");

            ExecSnapshot s = execSnapshot();
            log.warn("event=copy.job.rejected id={} originId={} userId={} action={} attempt={} nextRunAt={} workerId={} execPool={} execActive={} execQueue={} execQueueRemaining={} errClass={} err={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), job.getAttempt(),
                    nextRunAt, workerId,
                    s.poolSize(), s.activeCount(), s.queueSize(), s.queueRemaining(),
                    rej.getClass().getSimpleName(), safeMsg(rej));
        }
    }

    private void process(CopyExecutionJobEntity job) {
        long t0 = System.nanoTime();

        try {
            putJobMdc(job);

            log.info("event=copy.job.started id={} originId={} userId={} action={} attempt={} workerId={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), job.getAttempt(), workerId);

            OperacionEvent event = objectMapper.readValue(job.getPayload(), OperacionEvent.class);

            UserDetailDto user = resolveUserOrSkip(job.getUserId());

            if (job.getAction() == CopyJobAction.OPEN) {
                binanceCopyExecutionService.executeOpenForUser(event, user);
            } else {
                binanceCopyExecutionService.executeCloseForUser(event, user);
            }

            jobService.markDone(job);

            long ms = Duration.ofNanos(System.nanoTime() - t0).toMillis();
            log.info("event=copy.job.completed id={} originId={} userId={} action={} attempt={} workerId={} durationMs={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), job.getAttempt(), workerId, ms);

        } catch (SkipExecutionException skip) {
            jobService.markDead(job, CopyJobErrorCategory.SKIP.name(), safeMsg(skip));
            log.info("event=copy.job.skipped id={} originId={} userId={} action={} attempt={} workerId={} reason={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), job.getAttempt(), workerId, safeMsg(skip));

        } catch (Exception ex) {
            handleFailure(job, ex);

        } finally {
            MDC.clear();
        }
    }

    private void handleFailure(CopyExecutionJobEntity job, Exception ex) {
        int attempt = job.getAttempt() + 1;
        job.setAttempt(attempt);

        CopyJobErrorCategory category = classify(ex);
        String msg = safeMsg(ex);

        if (attempt >= maxAttempts) {
            jobService.markDead(job, category.name(), msg);
            log.error("event=copy.job.dead id={} originId={} userId={} action={} attempts={} category={} err={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), attempt, category, msg, ex);
            return;
        }

        OffsetDateTime nextRunAt = OffsetDateTime.now().plus(computeBackoff(category, attempt));
        jobService.reschedule(job, nextRunAt, category.name(), msg);

        log.warn("event=copy.job.retry_scheduled id={} originId={} userId={} action={} attempts={} category={} nextRunAt={} err={}",
                job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), attempt, category, nextRunAt, msg);
    }

    private Duration computeBackoff(CopyJobErrorCategory category, int attempt) {
        Duration base = (category == CopyJobErrorCategory.RATE_LIMIT) ? BACKOFF_RATE_LIMIT_BASE : BACKOFF_BASE;
        Duration max = (category == CopyJobErrorCategory.RATE_LIMIT) ? BACKOFF_RATE_LIMIT_MAX : BACKOFF_MAX;

        long pow = 1L << Math.min(attempt - 1, 20);
        long millis = base.toMillis() * pow;
        millis = Math.min(millis, max.toMillis());

        long jitterMs = (long) (millis * (0.15 * ThreadLocalRandom.current().nextDouble())); // 0-15%
        return Duration.ofMillis(millis + jitterMs);
    }

    private CopyJobErrorCategory classify(Throwable t) {
        Throwable cur = t;

        while (cur != null) {
            if (cur instanceof BinanceRateLimitException) return CopyJobErrorCategory.RATE_LIMIT;

            if (cur instanceof EngineException ee) {
                if (ee.getErrorCode() == ErrorCode.BINANCE_RATE_LIMIT) return CopyJobErrorCategory.RATE_LIMIT;
                if (ee.getErrorCode() == ErrorCode.BINANCE_CLIENT_ERROR) return CopyJobErrorCategory.VALIDATION;
            }

            if (cur instanceof RestClientResponseException rre) {
                int s = rre.getRawStatusCode();
                if (s == 429) return CopyJobErrorCategory.RATE_LIMIT;
                if (s >= 400 && s < 500) return CopyJobErrorCategory.VALIDATION;
                if (s >= 500) return CopyJobErrorCategory.TRANSIENT;
            }

            if (cur instanceof ResourceAccessException) return CopyJobErrorCategory.NETWORK;

            cur = cur.getCause();
        }

        return CopyJobErrorCategory.UNKNOWN;
    }

    private UserDetailDto resolveUserOrSkip(String userId) {
        List<UserDetailDto> users = userDetailCachedService.getUsers();
        Optional<UserDetailDto> user = users.stream()
                .filter(u -> u.getUser() != null
                        && u.getUser().getId() != null
                        && u.getUser().getId().toString().equals(userId))
                .findFirst();

        return user.orElseThrow(() -> new SkipExecutionException("Usuario no existe en cache: userId=" + userId));
    }

    private void putJobMdc(CopyExecutionJobEntity job) {
        if (job == null) return;
        if (job.getId() != null) MDC.put("copy.jobId", job.getId().toString());
        if (job.getOriginId() != null) MDC.put("copy.originId", job.getOriginId());
        if (job.getUserId() != null) MDC.put("copy.userId", job.getUserId());
        if (job.getAction() != null) MDC.put("copy.action", job.getAction().name());
        MDC.put("copy.workerId", workerId);
    }

    private ExecSnapshot execSnapshot() {
        try {
            ThreadPoolExecutor tpe = executor.getThreadPoolExecutor();
            if (tpe == null) return new ExecSnapshot(-1, -1, -1, -1);
            int pool = tpe.getPoolSize();
            int active = tpe.getActiveCount();
            int qSize = tpe.getQueue().size();
            int qRemaining = tpe.getQueue().remainingCapacity();
            return new ExecSnapshot(pool, active, qSize, qRemaining);
        } catch (Exception e) {
            return new ExecSnapshot(-1, -1, -1, -1);
        }
    }

    private record ExecSnapshot(int poolSize, int activeCount, int queueSize, int queueRemaining) {}

    private String safeMsg(Throwable t) {
        if (t == null) return null;
        String m = t.getMessage();
        if (m == null) return t.getClass().getSimpleName();
        return m.length() > 4000 ? m.substring(0, 4000) : m;
    }

    private String buildWorkerId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + System.nanoTime();
        } catch (Exception e) {
            return "worker-" + System.nanoTime();
        }
    }
}
