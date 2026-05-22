package com.apunto.engine.jobs;

import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.entity.CopyExecutionJobEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.model.HyperliquidCopyLifecycleDecision;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.hyperliquid.service.HyperliquidCopyLifecycleGuard;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.jobs.model.CopyJobErrorCategory;
import com.apunto.engine.metric.TradingMetrics;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.BinanceCopyExecutionService;
import com.apunto.engine.service.CopyExecutionJobService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.shared.exception.BinanceRateLimitException;
import com.apunto.engine.shared.exception.CopyExecutionException;
import com.apunto.engine.shared.exception.EngineException;
import com.apunto.engine.shared.exception.ErrorCode;
import com.apunto.engine.shared.exception.SkipExecutionException;
import com.apunto.engine.shared.enums.PositionSide;
import com.apunto.engine.shared.enums.Side;
import com.apunto.engine.shared.util.CopyLogAdvice;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final ActiveCopyOperationCache activeCopyOperationCache;
    private final HyperliquidCopyLifecycleGuard lifecycleGuard;
    private final UserDetailCachedService userDetailCachedService;
    private final ObjectMapper objectMapper;
    private final ThreadPoolTaskExecutor executor;
    private final TradingMetrics tradingMetrics;
    private final String workerId;

    @Value("${operation.job.worker.max-batch:${copy.job.worker.max-batch:50}}")
    private int maxBatch;

    @Value("${operation.job.worker.max-attempts:${copy.job.worker.max-attempts:10}}")
    private int maxAttempts;

    public CopyExecutionJobWorker(
            CopyExecutionJobService jobService,
            BinanceCopyExecutionService binanceCopyExecutionService,
            ActiveCopyOperationCache activeCopyOperationCache,
            HyperliquidCopyLifecycleGuard lifecycleGuard,
            UserDetailCachedService userDetailCachedService,
            ObjectMapper objectMapper,
            @Qualifier("copyJobExecutor") ThreadPoolTaskExecutor executor,
            TradingMetrics tradingMetrics
    ) {
        this.jobService = jobService;
        this.binanceCopyExecutionService = binanceCopyExecutionService;
        this.activeCopyOperationCache = activeCopyOperationCache;
        this.lifecycleGuard = lifecycleGuard;
        this.userDetailCachedService = userDetailCachedService;
        this.objectMapper = objectMapper;
        this.executor = executor;
        this.tradingMetrics = tradingMetrics;
        this.workerId = buildWorkerId();
    }

    @Scheduled(fixedDelayString = "${operation.job.worker.poll-ms:${copy.job.worker.poll-ms:50}}")
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

            tradingMetrics.claimedBatch(jobs.size());

            ExecSnapshot s = execSnapshot();
            log.info("event=copy.job.claimed workerId={} batch={} execPool={} execActive={} execQueue={} execQueueRemaining={}",
                    workerId, jobs.size(),
                    s.poolSize(), s.activeCount(), s.queueSize(), s.queueRemaining());

            for (CopyExecutionJobEntity job : jobs) {
                submitOrRescheduleOnReject(job);
            }

        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException e) {
            log.error("event=copy.tick.error workerId={} errClass={} errMsg=\"{}\"",
                    workerId, e.getClass().getSimpleName(), safeMsgForLog(safeMsg(e)), e);
        }
    }


    private void assertBusinessLifecycleAllowed(CopyExecutionJobEntity job, OperacionEvent event) {
        if (event == null || event.getDeltaType() == null || event.getDeltaType().isBlank()) {
            return;
        }
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(event.getDeltaType());
        boolean active = activeCopyOperationCache.isActive(job.getOriginId(), job.getUserId());
        HyperliquidCopyLifecycleDecision decision = lifecycleGuard.decide(job.getAction(), deltaType, active);
        if (decision.allowed()) {
            return;
        }
        OperacionDto op = event.getOperacion();
        throw new SkipExecutionException(
                decision.reasonCode(),
                "Regla de lifecycle de copia bloqueó el job",
                com.apunto.engine.shared.util.LogFmt.kv(
                        "originId", job.getOriginId(),
                        "userId", job.getUserId(),
                        "wallet", op == null ? null : op.getIdCuenta(),
                        "symbol", op == null ? null : op.getParSymbol(),
                        "deltaType", deltaType.name(),
                        "cacheActive", Boolean.toString(active),
                        "activeCacheSize", Integer.toString(activeCopyOperationCache.activeSize())
                )
        );
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
            log.warn("event=copy.job.rejected id={} originId={} userId={} action={} attempts={} category={} retryable=true nextRunAt={} workerId={} execPool={} execActive={} execQueue={} execQueueRemaining={} errClass={} errMsg=\"{}\"",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), job.getAttempt() + 1,
                    CopyJobErrorCategory.REJECTED,
                    nextRunAt, workerId,
                    s.poolSize(), s.activeCount(), s.queueSize(), s.queueRemaining(),
                    rej.getClass().getSimpleName(), safeMsgForLog(safeMsg(rej)));
        }
    }

    private void process(CopyExecutionJobEntity job) {
        long t0 = System.nanoTime();
        OperacionEvent event = null;

        try {
            putJobMdc(job);
            tradingMetrics.jobQueueWait(job);

            log.info("event=copy.job.started id={} originId={} userId={} action={} attempt={} workerId={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), job.getAttempt(), workerId);

            long phaseNs = System.nanoTime();
            event = readPayload(job);
            log.info("event=copy.job.phase id={} originId={} userId={} action={} phase=load_job elapsedMs={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), elapsedMsSince(phaseNs));

            assertBusinessLifecycleAllowed(job, event);

            phaseNs = System.nanoTime();
            UserDetailDto user = resolveUserOrSkip(job.getUserId());
            log.info("event=copy.job.phase id={} originId={} userId={} action={} phase=load_user elapsedMs={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), elapsedMsSince(phaseNs));

            phaseNs = System.nanoTime();
            if (job.getAction() == CopyJobAction.OPEN) {
                binanceCopyExecutionService.executeOpenForUser(event, user);
            } else {
                binanceCopyExecutionService.executeCloseForUser(event, user);
            }
            log.info("event=copy.job.phase id={} originId={} userId={} action={} phase=execute_copy elapsedMs={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), elapsedMsSince(phaseNs));

            phaseNs = System.nanoTime();
            jobService.markDone(job);
            log.info("event=copy.job.phase id={} originId={} userId={} action={} phase=mark_done elapsedMs={}",
                    job.getId(), job.getOriginId(), job.getUserId(), job.getAction(), elapsedMsSince(phaseNs));

            tradingMetrics.jobExecution(job, "done", System.nanoTime() - t0);
            tradingMetrics.jobResult(job, "done", null);

            long ms = Duration.ofNanos(System.nanoTime() - t0).toMillis();
            CopyJobLogContext ctx = copyJobLogContext(event, null);
            log.info("event=copy.job.completed id={} originId={} userId={} action={} wallet={} symbol={} side={} positionSide={} qty={} attempts={} category={} workerId={} durationMs={}",
                    job.getId(),
                    job.getOriginId(),
                    job.getUserId(),
                    job.getAction(),
                    ctx.wallet(),
                    ctx.symbol(),
                    ctx.side(),
                    ctx.positionSide(),
                    ctx.qty(),
                    job.getAttempt() + 1,
                    CopyJobErrorCategory.NONE,
                    workerId,
                    ms);

        } catch (SkipExecutionException skip) {
            jobService.markDead(job, CopyJobErrorCategory.SKIP.name(), safeMsg(skip));

            tradingMetrics.jobExecution(job, "skipped", System.nanoTime() - t0);
            tradingMetrics.jobResult(job, "skipped", skip.getReasonCode());

            CopyJobLogContext ctx = copyJobLogContext(event, skip);
            log.info("event=copy.job.skipped id={} originId={} userId={} action={} wallet={} symbol={} side={} positionSide={} qty={} attempts={} category={} retryable=false workerId={} reasonCode={} errCode={} httpStatus={} binanceCode={} traceId={} reason=\"{}\" errMsg=\"{}\" details=\"{}\" {}",
                    job.getId(),
                    job.getOriginId(),
                    job.getUserId(),
                    job.getAction(),
                    ctx.wallet(),
                    ctx.symbol(),
                    ctx.side(),
                    ctx.positionSide(),
                    ctx.qty(),
                    job.getAttempt() + 1,
                    CopyJobErrorCategory.SKIP,
                    workerId,
                    skip.getReasonCode(),
                    skip.getReasonCode(),
                    orNA(extractLogFmtValue(skip.getDetails(), "httpStatus")),
                    orNA(extractLogFmtValue(skip.getDetails(), "binanceCode")),
                    orNA(extractLogFmtValue(skip.getDetails(), "traceId")),
                    safeMsgForLog(skip.getReason()),
                    safeMsgForLog(skip.getMessage()),
                    safeMsgForLog(skip.getDetails()),
                    CopyLogAdvice.fields(skip.getReasonCode(), CopyLogAdvice.context(null, null, null, 1, null, null, null, "copy_job_worker")));

        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            handleFailure(job, event, ex);

            tradingMetrics.jobExecution(job, "failed", System.nanoTime() - t0);
            tradingMetrics.jobResult(job, "failed", ex.getClass().getSimpleName());

        } finally {
            MDC.clear();
        }
    }

    private void handleFailure(CopyExecutionJobEntity job, OperacionEvent event, Throwable ex) {
        int attempt = job.getAttempt() + 1;
        job.setAttempt(attempt);

        CopyJobErrorCategory category = classify(ex);
        String msg = safeMsg(ex);
        ErrorSummary summary = summarizeError(ex, event);
        boolean retryable = shouldRetry(category, ex);

        if (!retryable || attempt >= maxAttempts) {
            jobService.markDead(job, category.name(), msg);
            if (shouldLogStacktrace(category, ex)) {
                log.error("event=copy.job.dead id={} originId={} userId={} action={} wallet={} attempts={} category={} retryable={} errClass={} errCode={} httpStatus={} symbol={} side={} positionSide={} qty={} binanceCode={} traceId={} errMsg=\"{}\"",
                        job.getId(),
                        job.getOriginId(),
                        job.getUserId(),
                        job.getAction(),
                        summary.wallet(),
                        attempt,
                        category,
                        retryable,
                        summary.errClass(),
                        summary.errCode(),
                        summary.httpStatus(),
                        summary.symbol(),
                        summary.side(),
                        summary.positionSide(),
                        summary.qty(),
                        summary.binanceCode(),
                        summary.traceId(),
                        safeMsgForLog(summary.errMsg()),
                        ex);
            } else {
                log.error("event=copy.job.dead id={} originId={} userId={} action={} wallet={} attempts={} category={} retryable={} errClass={} errCode={} httpStatus={} symbol={} side={} positionSide={} qty={} binanceCode={} traceId={} errMsg=\"{}\"",
                        job.getId(),
                        job.getOriginId(),
                        job.getUserId(),
                        job.getAction(),
                        summary.wallet(),
                        attempt,
                        category,
                        retryable,
                        summary.errClass(),
                        summary.errCode(),
                        summary.httpStatus(),
                        summary.symbol(),
                        summary.side(),
                        summary.positionSide(),
                        summary.qty(),
                        summary.binanceCode(),
                        summary.traceId(),
                        safeMsgForLog(summary.errMsg()));
            }
            return;
        }

        OffsetDateTime nextRunAt = OffsetDateTime.now().plus(computeBackoff(category, attempt));
        jobService.reschedule(job, nextRunAt, category.name(), msg);

        log.warn("event=copy.job.retry_scheduled id={} originId={} userId={} action={} wallet={} attempts={} category={} nextRunAt={} errClass={} errCode={} httpStatus={} symbol={} side={} positionSide={} qty={} binanceCode={} traceId={} errMsg=\"{}\"",
                job.getId(),
                job.getOriginId(),
                job.getUserId(),
                job.getAction(),
                summary.wallet(),
                attempt,
                category,
                nextRunAt,
                summary.errClass(),
                summary.errCode(),
                summary.httpStatus(),
                summary.symbol(),
                summary.side(),
                summary.positionSide(),
                summary.qty(),
                summary.binanceCode(),
                summary.traceId(),
                safeMsgForLog(summary.errMsg()));
    }

    private boolean shouldLogStacktrace(CopyJobErrorCategory category, Throwable t) {
        if (category == CopyJobErrorCategory.UNKNOWN) {
            return true;
        }
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof EngineException || cur instanceof RestClientResponseException) {
                return false;
            }
            if (cur instanceof ResourceAccessException) {
                return false;
            }
            cur = cur.getCause();
        }
        return true;
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

    private boolean shouldRetry(CopyJobErrorCategory category, Throwable t) {
        if (category == CopyJobErrorCategory.VALIDATION || category == CopyJobErrorCategory.SKIP) {
            return false;
        }

        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof EngineException ee) {
                if (ee.getErrorCode() == ErrorCode.BINANCE_CLIENT_ERROR) {
                    return false;
                }
            }

            if (cur instanceof RestClientResponseException rre) {
                int s = rre.getRawStatusCode();
                if (s >= 400 && s < 500 && s != 429) {
                    return false;
                }
            }

            cur = cur.getCause();
        }

        return true;
    }

    private ErrorSummary summarizeError(Throwable t, OperacionEvent event) {
        CopyJobLogContext ctx = copyJobLogContext(event, null);
        Throwable cur = t;

        while (cur != null) {
            if (cur instanceof EngineException ee) {
                Map<String, Object> d = ee.getDetails() == null ? Map.of() : ee.getDetails();
                return new ErrorSummary(
                        cur.getClass().getSimpleName(),
                        copyErrorCode(ee),
                        asString(d.get("httpStatus")),
                        firstNonBlank(asString(d.get("wallet")), ctx.wallet()),
                        firstNonBlank(asString(d.get("symbol")), ctx.symbol()),
                        firstNonBlank(asString(d.get("side")), ctx.side()),
                        firstNonBlank(asString(d.get("positionSide")), ctx.positionSide()),
                        firstNonBlank(asString(d.get("quantity")), asString(d.get("qty")), asString(d.get("qtyFinal")), ctx.qty()),
                        asString(d.get("binanceCode")),
                        asString(d.get("traceId")),
                        ee.getMessage()
                );
            }

            if (cur instanceof RestClientResponseException rre) {
                return new ErrorSummary(
                        cur.getClass().getSimpleName(),
                        null,
                        Integer.toString(rre.getRawStatusCode()),
                        ctx.wallet(),
                        ctx.symbol(),
                        ctx.side(),
                        ctx.positionSide(),
                        ctx.qty(),
                        null,
                        null,
                        rre.getStatusText()
                );
            }

            cur = cur.getCause();
        }

        return new ErrorSummary(
                t == null ? null : t.getClass().getSimpleName(),
                null,
                null,
                ctx.wallet(),
                ctx.symbol(),
                ctx.side(),
                ctx.positionSide(),
                ctx.qty(),
                null,
                null,
                safeMsg(t)
        );
    }

    private CopyJobLogContext copyJobLogContext(OperacionEvent event, SkipExecutionException skip) {
        OperacionDto op = event == null ? null : event.getOperacion();
        String details = skip == null ? null : skip.getDetails();

        String eventWallet = op == null ? null : op.getIdCuenta();
        String eventSymbol = op == null ? null : op.getParSymbol();
        String eventPositionSide = op == null || op.getTipoOperacion() == null ? null : op.getTipoOperacion().name();
        String eventSide = deriveOrderSide(eventPositionSide);
        String eventQty = op == null || op.getSizeQty() == null ? null : op.getSizeQty().toPlainString();

        String positionSide = firstNonBlank(
                extractLogFmtFirst(details, "positionSide", "sourcePositionSide"),
                eventPositionSide
        );
        String side = firstNonBlank(
                extractLogFmtFirst(details, "side", "orderSide"),
                eventSide,
                deriveOrderSide(positionSide)
        );
        String qty = firstNonBlank(
                extractLogFmtFirst(details,
                        "qty",
                        "quantity",
                        "qtyFinal",
                        "qtyAdjusted",
                        "qtyAfterMinNotional",
                        "qtyAfterRules",
                        "qtyRaw",
                        "sourceSizeQty"),
                eventQty
        );

        return new CopyJobLogContext(
                orNA(firstNonBlank(extractLogFmtValue(details, "wallet"), eventWallet)),
                orNA(firstNonBlank(extractLogFmtValue(details, "symbol"), eventSymbol)),
                orNA(side),
                orNA(positionSide),
                orNA(qty)
        );
    }

    private String deriveOrderSide(String positionSide) {
        if (positionSide == null || positionSide.isBlank()) return null;
        try {
            PositionSide ps = PositionSide.valueOf(positionSide.trim().toUpperCase());
            if (ps == PositionSide.LONG) return Side.BUY.name();
            if (ps == PositionSide.SHORT) return Side.SELL.name();
        } catch (IllegalArgumentException ignored) {
            return null;
        }
        return null;
    }

    private String extractLogFmtFirst(String details, String... keys) {
        if (keys == null || keys.length == 0) return null;
        for (String key : keys) {
            String value = extractLogFmtValue(details, key);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private String extractLogFmtValue(String details, String key) {
        if (details == null || details.isBlank() || key == null || key.isBlank()) return null;
        String token = key + "=";
        int start = details.indexOf(token);
        if (start < 0) return null;
        start += token.length();
        int end = details.indexOf(' ', start);
        String value = end < 0 ? details.substring(start) : details.substring(start, end);
        if (value == null || value.isBlank() || "null".equalsIgnoreCase(value) || "NA".equalsIgnoreCase(value)) return null;
        return value;
    }

    private String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    private String orNA(String value) {
        return value == null || value.isBlank() ? "NA" : value;
    }

    private record CopyJobLogContext(String wallet, String symbol, String side, String positionSide, String qty) {}

    private String copyErrorCode(EngineException ex) {
        if (ex instanceof CopyExecutionException copyEx && copyEx.getErrCode() != null) {
            return copyEx.getErrCode();
        }
        return ex.getErrorCode() == null ? null : ex.getErrorCode().name();
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private record ErrorSummary(
            String errClass,
            String errCode,
            String httpStatus,
            String wallet,
            String symbol,
            String side,
            String positionSide,
            String qty,
            String binanceCode,
            String traceId,
            String errMsg
    ) {}

    private CopyJobErrorCategory classify(Throwable t) {
        Throwable cur = t;

        while (cur != null) {
            if (cur instanceof BinanceRateLimitException) return CopyJobErrorCategory.RATE_LIMIT;

            if (cur instanceof EngineException ee) {
                if (ee.getErrorCode() == ErrorCode.BINANCE_RATE_LIMIT) return CopyJobErrorCategory.RATE_LIMIT;
                if (ee.getErrorCode() == ErrorCode.BINANCE_CLIENT_ERROR) return CopyJobErrorCategory.VALIDATION;
                if (ee.getErrorCode() == ErrorCode.EXTERNAL_SERVICE_ERROR) return CopyJobErrorCategory.TRANSIENT;
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

    private OperacionEvent readPayload(CopyExecutionJobEntity job) {
        try {
            return objectMapper.readValue(job.getPayload(), OperacionEvent.class);
        } catch (JsonProcessingException ex) {
            Map<String, Object> details = new HashMap<>();
            if (job.getId() != null) details.put("jobId", job.getId().toString());
            if (job.getOriginId() != null) details.put("originId", job.getOriginId());
            if (job.getUserId() != null) details.put("userId", job.getUserId());
            if (job.getAction() != null) details.put("action", job.getAction().name());

            throw new EngineException(
                    ErrorCode.INTERNAL_ERROR,
                    "No se pudo deserializar payload de copy_execution_job",
                    ex,
                    details
            );
        }
    }

    private UserDetailDto resolveUserOrSkip(String userId) {
        return userDetailCachedService.getUserById(userId)
                .orElseThrow(() -> new SkipExecutionException(
                        "user_cache_miss",
                        "Usuario no existe en cache",
                        com.apunto.engine.shared.util.LogFmt.kv("userId", userId)
                ));
    }

    private void putJobMdc(CopyExecutionJobEntity job) {
        if (job == null) return;
        if (job.getId() != null) MDC.put("copy.jobId", job.getId().toString());
        if (job.getOriginId() != null) MDC.put("copy.originId", job.getOriginId());
        if (job.getUserId() != null) MDC.put("copy.userId", job.getUserId());
        if (job.getAction() != null) MDC.put("copy.action", job.getAction().name());
        MDC.put("copy.workerId", workerId);
    }

    private long elapsedMsSince(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
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
        } catch (IllegalStateException ex) {
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

    private String safeMsgForLog(String s) {
        if (s == null) return "";
        String clean = s
                .replace("\n", " ")
                .replace("\r", " ")
                .replace("\t", " ")
                .replace('"', '\'');
        return clean.length() > 4000 ? clean.substring(0, 4000) : clean;
    }


    private String buildWorkerId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + System.nanoTime();
        } catch (UnknownHostException ex) {
            return "worker-" + System.nanoTime();
        }
    }
}
