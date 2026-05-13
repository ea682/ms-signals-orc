package com.apunto.engine.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.metric.TradingMetrics;
import com.apunto.engine.service.CopyExecutionJobService;
import com.apunto.engine.service.OperacionEventIngestService;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailCachedService;
import com.apunto.engine.shared.exception.EngineException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class OperacionEventIngestServiceImpl implements OperacionEventIngestService {

    private static final String ERR_EVENT_NULL = "El evento no puede ser null";
    private static final String ERR_TIPO_NULL = "El tipo del evento no puede ser null";
    private static final String ERR_OPERACION_NULL = "La operacion no puede ser null";
    private static final String ERR_ID_NULL = "El idOperacion no puede ser null";

    private static final String LOG_ENQUEUED =
            "event=copy.execution.enqueued originId={} wallet={} tipo={} usersCached={} eligibleUsers={} eligibleUserIds={} enqueued={} allocationFilter={}";

    private final UserDetailCachedService userDetailCachedService;
    private final UserCopyAllocationService userCopyAllocationService;
    private final CopyExecutionJobService copyExecutionJobService;
    private final TradingMetrics tradingMetrics;

    @Value("${copy.job.ingest.filter-by-wallet-allocation:true}")
    private boolean filterByWalletAllocation;

    @Value("${copy.job.ingest.fallback-all-users-on-empty-allocation:false}")
    private boolean fallbackAllUsersOnEmptyAllocation;

    @Override
    public int ingest(OperacionEvent event) {
        long t0 = System.nanoTime();
        String tipo = "UNKNOWN";
        String result = "ok";

        try {
            requireNonNull(event, ERR_EVENT_NULL);
            requireNonNull(event.getTipo(), ERR_TIPO_NULL);
            requireNonNull(event.getOperacion(), ERR_OPERACION_NULL);
            requireNonNull(event.getOperacion().getIdOperacion(), ERR_ID_NULL);

            final OperacionDto operacion = event.getOperacion();
            tipo = event.getTipo().name();

            final String originId = operacion.getIdOperacion().toString();
            final String walletId = operacion.getIdCuenta();
            final CopyJobAction action = mapAction(event.getTipo());

            final List<UserDetailDto> usersCached = safeUsers(userDetailCachedService.getUsers());
            final List<UserDetailDto> eligibleUsers = resolveEligibleUsers(walletId, usersCached);
            final int enqueued = copyExecutionJobService.enqueueForUsers(event, eligibleUsers, action);

            tradingMetrics.jobsEnqueued(action.name(), eligibleUsers.size(), enqueued);

            log.info(LOG_ENQUEUED,
                    originId,
                    walletId,
                    event.getTipo(),
                    usersCached.size(),
                    eligibleUsers.size(),
                    userIdsCsv(eligibleUsers),
                    enqueued,
                    filterByWalletAllocation);
            return enqueued;

        } catch (EngineException | DataAccessException | RestClientException | IllegalStateException | IllegalArgumentException ex) {
            result = "error";
            throw ex;
        } finally {
            tradingMetrics.ingestDuration(tipo, result, System.nanoTime() - t0);
        }
    }

    private void requireNonNull(Object value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
    }

    private List<UserDetailDto> resolveEligibleUsers(String walletId, List<UserDetailDto> usersCached) {
        if (!filterByWalletAllocation) {
            return usersCached;
        }
        if (walletId == null || walletId.isBlank()) {
            log.warn("event=copy.execution.user_filter_skipped reason=wallet_missing usersCached={}", usersCached.size());
            return fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
        }

        final Set<UUID> activeUserIds = userCopyAllocationService.getActiveUserIdsByWallet(walletId);
        if (activeUserIds.isEmpty()) {
            log.info("event=copy.execution.user_filter wallet={} activeAllocationUsers=0 usersCached={} eligibleUsers=0 eligibleUserIds=\"\" fallbackAllUsers={}",
                    walletId, usersCached.size(), fallbackAllUsersOnEmptyAllocation);
            return fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
        }

        final List<UserDetailDto> eligible = usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> activeUserIds.contains(u.getUser().getId()))
                .toList();

        log.info("event=copy.execution.user_filter wallet={} activeAllocationUsers={} usersCached={} eligibleUsers={} activeUserIds={} eligibleUserIds={}",
                walletId,
                activeUserIds.size(),
                usersCached.size(),
                eligible.size(),
                uuidSetCsv(activeUserIds),
                userIdsCsv(eligible));
        return eligible;
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

    private String uuidSetCsv(Set<UUID> ids) {
        if (ids == null || ids.isEmpty()) {
            return "";
        }
        return ids.stream()
                .filter(Objects::nonNull)
                .map(UUID::toString)
                .sorted()
                .limit(20)
                .collect(Collectors.joining(","));
    }

    private CopyJobAction mapAction(OperacionEvent.Tipo tipo) {
        return switch (tipo) {
            case ABIERTA -> CopyJobAction.OPEN;
            case CERRADA -> CopyJobAction.CLOSE;
        };
    }

    private List<UserDetailDto> safeUsers(List<UserDetailDto> users) {
        return users == null ? Collections.emptyList() : users;
    }
}
