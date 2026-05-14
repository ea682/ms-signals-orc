package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDirectCopyDispatchResult;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectCopyDispatchService;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.service.BinanceCopyExecutionService;
import com.apunto.engine.service.OperacionEventIngestService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.shared.exception.EngineException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HyperliquidDirectCopyDispatchServiceImpl implements HyperliquidDirectCopyDispatchService {

    private final UserDetailCachedService userDetailCachedService;
    private final UserCopyAllocationService userCopyAllocationService;
    private final BinanceCopyExecutionService binanceCopyExecutionService;
    private final OperacionEventIngestService fallbackIngestService;
    private final ThreadPoolTaskExecutor copyJobExecutor;

    @Value("${copy.job.ingest.filter-by-wallet-allocation:true}")
    private boolean filterByWalletAllocation;

    @Value("${copy.job.ingest.fallback-all-users-on-empty-allocation:false}")
    private boolean fallbackAllUsersOnEmptyAllocation;

    @Value("${hyperliquid.direct-ingest.fallback-db-on-direct-failure:true}")
    private boolean fallbackDbOnDirectFailure;

    public HyperliquidDirectCopyDispatchServiceImpl(
            UserDetailCachedService userDetailCachedService,
            UserCopyAllocationService userCopyAllocationService,
            BinanceCopyExecutionService binanceCopyExecutionService,
            OperacionEventIngestService fallbackIngestService,
            @Qualifier("copyJobExecutor") ThreadPoolTaskExecutor copyJobExecutor
    ) {
        this.userDetailCachedService = userDetailCachedService;
        this.userCopyAllocationService = userCopyAllocationService;
        this.binanceCopyExecutionService = binanceCopyExecutionService;
        this.fallbackIngestService = fallbackIngestService;
        this.copyJobExecutor = copyJobExecutor;
    }

    @Override
    public HyperliquidDirectCopyDispatchResult dispatch(OperacionEvent event) {
        long startedNs = System.nanoTime();
        requireEvent(event);

        OperacionDto operacion = event.getOperacion();
        String originId = operacion.getIdOperacion().toString();
        String wallet = operacion.getIdCuenta();
        String symbol = operacion.getParSymbol();
        CopyJobAction action = mapAction(event.getTipo());
        List<UserDetailDto> usersCached = safeUsers(userDetailCachedService.getUsers());
        List<UserDetailDto> eligibleUsers = resolveEligibleUsers(wallet, usersCached);
        AtomicBoolean fallbackSubmitted = new AtomicBoolean(false);
        AtomicInteger submitted = new AtomicInteger(0);
        AtomicInteger fallbackJobs = new AtomicInteger(0);

        for (UserDetailDto user : eligibleUsers) {
            try {
                copyJobExecutor.execute(() -> executeCopy(event, user, action, fallbackSubmitted, fallbackJobs));
                submitted.incrementAndGet();
            } catch (RejectedExecutionException rejected) {
                log.warn("event=hyperliquid.direct_copy.rejected originId={} wallet={} symbol={} action={} eligibleUsers={} submitted={} errClass={} errMsg=\"{}\"",
                        originId, safeLog(wallet), safeLog(symbol), action, eligibleUsers.size(), submitted.get(),
                        rejected.getClass().getSimpleName(), safeLog(rejected.getMessage()));
                fallbackJobs.addAndGet(submitFallbackOnce(event, fallbackSubmitted, "executor_rejected"));
                break;
            }
        }

        long elapsedMs = Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
        log.info("event=hyperliquid.direct_copy.dispatched originId={} wallet={} symbol={} action={} usersCached={} eligibleUsers={} eligibleUserIds={} submitted={} fallbackJobs={} fallbackUsed={} elapsedMs={}",
                originId,
                safeLog(wallet),
                safeLog(symbol),
                action,
                usersCached.size(),
                eligibleUsers.size(),
                userIdsCsv(eligibleUsers),
                submitted.get(),
                fallbackJobs.get(),
                fallbackSubmitted.get(),
                elapsedMs);

        return new HyperliquidDirectCopyDispatchResult(
                eligibleUsers.size(),
                submitted.get(),
                fallbackJobs.get(),
                fallbackSubmitted.get()
        );
    }

    private void executeCopy(
            OperacionEvent event,
            UserDetailDto user,
            CopyJobAction action,
            AtomicBoolean fallbackSubmitted,
            AtomicInteger fallbackJobs
    ) {
        long startedNs = System.nanoTime();
        OperacionDto operacion = event.getOperacion();
        String originId = operacion.getIdOperacion().toString();
        String wallet = operacion.getIdCuenta();
        String symbol = operacion.getParSymbol();
        String userId = user != null && user.getUser() != null && user.getUser().getId() != null
                ? user.getUser().getId().toString()
                : "unknown";
        try {
            if (action == CopyJobAction.OPEN) {
                binanceCopyExecutionService.executeOpenForUser(event, user);
            } else {
                binanceCopyExecutionService.executeCloseForUser(event, user);
            }
            log.info("event=hyperliquid.direct_copy.completed originId={} userId={} wallet={} symbol={} action={} elapsedMs={}",
                    originId, userId, safeLog(wallet), safeLog(symbol), action, elapsedMs(startedNs));
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException | ArithmeticException ex) {
            int fallback = submitFallbackOnce(event, fallbackSubmitted, "direct_execution_failed");
            fallbackJobs.addAndGet(fallback);
            if (shouldLogStacktrace(ex)) {
                log.error("event=hyperliquid.direct_copy.failed originId={} userId={} wallet={} symbol={} action={} fallbackJobs={} errClass={} errMsg=\"{}\" elapsedMs={}",
                        originId, userId, safeLog(wallet), safeLog(symbol), action, fallback,
                        ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs), ex);
            } else {
                log.error("event=hyperliquid.direct_copy.failed originId={} userId={} wallet={} symbol={} action={} fallbackJobs={} errClass={} errMsg=\"{}\" elapsedMs={}",
                        originId, userId, safeLog(wallet), safeLog(symbol), action, fallback,
                        ex.getClass().getSimpleName(), safeLog(ex.getMessage()), elapsedMs(startedNs));
            }
        }
    }

    private int submitFallbackOnce(OperacionEvent event, AtomicBoolean fallbackSubmitted, String reason) {
        if (!fallbackDbOnDirectFailure || !fallbackSubmitted.compareAndSet(false, true)) {
            return 0;
        }
        try {
            int jobs = fallbackIngestService.ingest(event);
            OperacionDto operacion = event.getOperacion();
            log.warn("event=hyperliquid.direct_copy.fallback_db_enqueued reason={} originId={} wallet={} symbol={} jobs={}",
                    reason,
                    operacion.getIdOperacion(),
                    safeLog(operacion.getIdCuenta()),
                    safeLog(operacion.getParSymbol()),
                    jobs);
            return jobs;
        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException fallbackEx) {
            OperacionDto operacion = event.getOperacion();
            log.error("event=hyperliquid.direct_copy.fallback_db_failed reason={} originId={} wallet={} symbol={} errClass={} errMsg=\"{}\"",
                    reason,
                    operacion.getIdOperacion(),
                    safeLog(operacion.getIdCuenta()),
                    safeLog(operacion.getParSymbol()),
                    fallbackEx.getClass().getSimpleName(),
                    safeLog(fallbackEx.getMessage()),
                    fallbackEx);
            return 0;
        }
    }

    private boolean shouldLogStacktrace(Throwable ex) {
        return !(ex instanceof EngineException || ex instanceof RestClientException || ex instanceof IllegalArgumentException);
    }

    private void requireEvent(OperacionEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("OperacionEvent is required");
        }
        if (event.getTipo() == null) {
            throw new IllegalArgumentException("OperacionEvent.tipo is required");
        }
        if (event.getOperacion() == null) {
            throw new IllegalArgumentException("OperacionEvent.operacion is required");
        }
        if (event.getOperacion().getIdOperacion() == null) {
            throw new IllegalArgumentException("OperacionEvent.operacion.idOperacion is required");
        }
    }

    private CopyJobAction mapAction(OperacionEvent.Tipo tipo) {
        return switch (tipo) {
            case ABIERTA -> CopyJobAction.OPEN;
            case CERRADA -> CopyJobAction.CLOSE;
        };
    }

    private List<UserDetailDto> resolveEligibleUsers(String walletId, List<UserDetailDto> usersCached) {
        if (!filterByWalletAllocation) {
            return usersCached;
        }
        if (walletId == null || walletId.isBlank()) {
            log.warn("event=hyperliquid.direct_copy.user_filter_skipped reason=wallet_missing usersCached={}", usersCached.size());
            return fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
        }

        Set<UUID> activeUserIds = userCopyAllocationService.getActiveUserIdsByWallet(walletId);
        if (activeUserIds.isEmpty()) {
            log.info("event=hyperliquid.direct_copy.user_filter wallet={} activeAllocationUsers=0 usersCached={} eligibleUsers=0 fallbackAllUsers={}",
                    safeLog(walletId), usersCached.size(), fallbackAllUsersOnEmptyAllocation);
            return fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
        }

        List<UserDetailDto> eligible = usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> activeUserIds.contains(u.getUser().getId()))
                .toList();

        log.info("event=hyperliquid.direct_copy.user_filter wallet={} activeAllocationUsers={} usersCached={} eligibleUsers={} eligibleUserIds={}",
                safeLog(walletId), activeUserIds.size(), usersCached.size(), eligible.size(), userIdsCsv(eligible));
        return eligible;
    }

    private List<UserDetailDto> safeUsers(List<UserDetailDto> users) {
        return users == null ? Collections.emptyList() : users;
    }

    private String userIdsCsv(List<UserDetailDto> users) {
        if (users == null || users.isEmpty()) {
            return "";
        }
        return users.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .map(u -> u.getUser().getId().toString())
                .sorted()
                .limit(20)
                .collect(Collectors.joining(","));
    }

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }
}
