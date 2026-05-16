package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.model.HyperliquidDeltaType;
import com.apunto.engine.jobs.model.CopyJobAction;
import com.apunto.engine.service.ActiveCopyOperationCache;
import com.apunto.engine.service.UserCopyAllocationService;
import com.apunto.engine.service.UserDetailCachedService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HyperliquidCopyCandidateResolver {

    private final UserDetailCachedService userDetailCachedService;
    private final UserCopyAllocationService userCopyAllocationService;
    private final ActiveCopyOperationCache activeCopyOperationCache;

    @Value("${copy.job.ingest.filter-by-wallet-allocation:true}")
    private boolean filterByWalletAllocation;

    @Value("${copy.job.ingest.fallback-all-users-on-empty-allocation:false}")
    private boolean fallbackAllUsersOnEmptyAllocation;

    public HyperliquidCopyCandidateResolver(
            UserDetailCachedService userDetailCachedService,
            UserCopyAllocationService userCopyAllocationService,
            ActiveCopyOperationCache activeCopyOperationCache
    ) {
        this.userDetailCachedService = userDetailCachedService;
        this.userCopyAllocationService = userCopyAllocationService;
        this.activeCopyOperationCache = activeCopyOperationCache;
    }

    public CandidateUsers resolve(HyperliquidMappedDelta mappedDelta, CopyJobAction action) {
        OperacionEvent event = mappedDelta.event();
        OperacionDto operation = event.getOperacion();
        List<UserDetailDto> usersCached = safeUsers(userDetailCachedService.getUsers());
        HyperliquidDeltaType deltaType = HyperliquidDeltaType.from(mappedDelta.deltaType() == null ? event.getDeltaType() : mappedDelta.deltaType());

        if (action == CopyJobAction.CLOSE) {
            return activeCopyUsers(operation, usersCached, "active_copy_close");
        }

        if (action == CopyJobAction.OPEN && deltaType.canAdjustExistingCopy()) {
            if (deltaType == HyperliquidDeltaType.FLIP) {
                return activeCopyUsersByWalletAndSymbol(operation, usersCached, "active_copy_flip");
            }
            return activeCopyUsers(operation, usersCached, "active_copy_adjustment");
        }

        return allocationUsers(operation, usersCached);
    }

    private CandidateUsers activeCopyUsers(OperacionDto operation, List<UserDetailDto> usersCached, String source) {
        String originId = operation.getIdOperacion() == null ? null : operation.getIdOperacion().toString();
        Set<String> activeUserIds = activeCopyOperationCache.activeUserIds(originId);
        List<UserDetailDto> eligible = filterUsersById(usersCached, activeUserIds);
        log.info("event=hyperliquid.direct_copy.user_filter source={} originId={} wallet={} symbol={} activeCopyUsers={} usersCached={} eligibleUsers={} eligibleUserIds={}",
                source, safeLog(originId), safeLog(operation.getIdCuenta()), safeLog(operation.getParSymbol()), activeUserIds.size(), usersCached.size(), eligible.size(), userIdsCsv(eligible));
        return new CandidateUsers(usersCached, eligible, source);
    }

    private CandidateUsers activeCopyUsersByWalletAndSymbol(OperacionDto operation, List<UserDetailDto> usersCached, String source) {
        Set<String> activeUserIds = activeCopyOperationCache.activeUserIdsByWalletAndSymbol(operation.getIdCuenta(), operation.getParSymbol());
        List<UserDetailDto> eligible = filterUsersById(usersCached, activeUserIds);
        log.info("event=hyperliquid.direct_copy.user_filter source={} originId={} wallet={} symbol={} activeCopyUsers={} usersCached={} eligibleUsers={} eligibleUserIds={}",
                source,
                operation.getIdOperacion(),
                safeLog(operation.getIdCuenta()),
                safeLog(operation.getParSymbol()),
                activeUserIds.size(),
                usersCached.size(),
                eligible.size(),
                userIdsCsv(eligible));
        return new CandidateUsers(usersCached, eligible, source);
    }

    private CandidateUsers allocationUsers(OperacionDto operation, List<UserDetailDto> usersCached) {
        String walletId = operation.getIdCuenta();
        if (!filterByWalletAllocation) {
            return new CandidateUsers(usersCached, usersCached, "all_users_config");
        }
        if (walletId == null || walletId.isBlank()) {
            log.warn("event=hyperliquid.direct_copy.user_filter_skipped reason=wallet_missing usersCached={}", usersCached.size());
            List<UserDetailDto> eligible = fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
            return new CandidateUsers(usersCached, eligible, "allocation_wallet_missing");
        }

        Set<UUID> activeUserIds = userCopyAllocationService.getActiveUserIdsByWallet(walletId);
        if (activeUserIds.isEmpty()) {
            log.info("event=hyperliquid.direct_copy.user_filter source=allocation wallet={} activeAllocationUsers=0 usersCached={} eligibleUsers=0 fallbackAllUsers={}",
                    safeLog(walletId), usersCached.size(), fallbackAllUsersOnEmptyAllocation);
            List<UserDetailDto> eligible = fallbackAllUsersOnEmptyAllocation ? usersCached : List.of();
            return new CandidateUsers(usersCached, eligible, "allocation_empty");
        }

        List<UserDetailDto> eligible = usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> activeUserIds.contains(u.getUser().getId()))
                .toList();

        log.info("event=hyperliquid.direct_copy.user_filter source=allocation wallet={} activeAllocationUsers={} usersCached={} eligibleUsers={} eligibleUserIds={}",
                safeLog(walletId), activeUserIds.size(), usersCached.size(), eligible.size(), userIdsCsv(eligible));
        return new CandidateUsers(usersCached, eligible, "allocation");
    }

    private List<UserDetailDto> filterUsersById(List<UserDetailDto> usersCached, Set<String> userIds) {
        if (userIds == null || userIds.isEmpty() || usersCached == null || usersCached.isEmpty()) {
            return List.of();
        }
        return usersCached.stream()
                .filter(Objects::nonNull)
                .filter(u -> u.getUser() != null && u.getUser().getId() != null)
                .filter(u -> userIds.contains(u.getUser().getId().toString()))
                .toList();
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

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }

    public record CandidateUsers(
            List<UserDetailDto> usersCached,
            List<UserDetailDto> eligibleUsers,
            String source
    ) {
    }
}
