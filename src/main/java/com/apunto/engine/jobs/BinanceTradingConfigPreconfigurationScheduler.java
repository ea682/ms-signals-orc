package com.apunto.engine.jobs;

import com.apunto.engine.client.BinanceClient;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.TradingConfigPreconfigureClientRequest;
import com.apunto.engine.dto.client.TradingConfigPreconfigureClientResponse;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.entity.UserCopyAllocationEntity;
import com.apunto.engine.repository.CopyOperationRepository;
import com.apunto.engine.repository.UserCopyAllocationRepository;
import com.apunto.engine.service.UserDetailService;
import com.apunto.engine.shared.dto.ApiResponse;
import com.apunto.engine.shared.enums.PositionSide;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class BinanceTradingConfigPreconfigurationScheduler {

    private final BinanceClient binanceClient;
    private final UserDetailService userDetailService;
    private final UserCopyAllocationRepository allocationRepository;
    private final CopyOperationRepository copyOperationRepository;
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Value("${binance.trading-config-preconfigure.enabled:true}")
    private boolean enabled;

    @Value("${binance.trading-config-preconfigure.recent-lookback-days:14}")
    private int recentLookbackDays;

    @Value("${binance.trading-config-preconfigure.max-symbols-per-allocation:20}")
    private int maxSymbolsPerAllocation;

    @Value("${binance.trading-config-preconfigure.margin-type:CROSSED}")
    private String marginType;

    public BinanceTradingConfigPreconfigurationScheduler(
            @Qualifier("binanceInfoClient") BinanceClient binanceClient,
            UserDetailService userDetailService,
            UserCopyAllocationRepository allocationRepository,
            CopyOperationRepository copyOperationRepository
    ) {
        this.binanceClient = binanceClient;
        this.userDetailService = userDetailService;
        this.allocationRepository = allocationRepository;
        this.copyOperationRepository = copyOperationRepository;
    }

    @Scheduled(
            initialDelayString = "${binance.trading-config-preconfigure.initial-delay-ms:60000}",
            fixedDelayString = "${binance.trading-config-preconfigure.fixed-delay-ms:600000}"
    )
    public void run() {
        if (!enabled) {
            log.debug("event=binance.trading_config.preconfigure.scheduler.skip reasonCode=disabled");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            log.warn("event=binance.trading_config.preconfigure.scheduler.skip reasonCode=previous_run_still_active copyImpact=no_overlap");
            return;
        }

        long startedNs = System.nanoTime();
        String traceId = "preconfig-" + System.currentTimeMillis();
        int users = 0;
        int candidates = 0;
        int ready = 0;
        int pending = 0;
        int skipped = 0;
        int failed = 0;

        try {
            List<UserDetailDto> activeUsers = safeList(userDetailService.findAllActive());
            for (UserDetailDto userDetail : activeUsers) {
                users++;
                List<PreconfigureCandidate> userCandidates = resolveCandidates(userDetail);
                candidates += userCandidates.size();
                for (PreconfigureCandidate candidate : userCandidates) {
                    try {
                        PreconfigureOutcome outcome = preconfigure(candidate, traceId);
                        if (outcome == PreconfigureOutcome.READY) {
                            ready++;
                        } else if (outcome == PreconfigureOutcome.PENDING) {
                            pending++;
                        } else {
                            skipped++;
                        }
                    } catch (RestClientException | IllegalStateException | IllegalArgumentException ex) {
                        failed++;
                        log.warn("event=binance.trading_config.preconfigure.failed traceId={} userId={} wallet={} allocationId={} symbol={} leverage={} errClass={} errMsg=\"{}\"",
                                traceId,
                                candidate.userId(),
                                candidate.walletId(),
                                candidate.allocationId(),
                                candidate.symbol(),
                                candidate.leverage(),
                                ex.getClass().getSimpleName(),
                                safeLog(ex.getMessage()));
                    }
                }
            }
        } finally {
            long elapsedMs = (System.nanoTime() - startedNs) / 1_000_000L;
            running.set(false);
            log.info("event=binance.trading_config.preconfigure.scheduler.done traceId={} users={} candidates={} ready={} pendingUntilFlat={} skipped={} failed={} elapsedMs={}",
                    traceId, users, candidates, ready, pending, skipped, failed, elapsedMs);
        }
    }

    private List<PreconfigureCandidate> resolveCandidates(UserDetailDto userDetail) {
        if (!validUser(userDetail)) {
            return List.of();
        }

        String userId = userDetail.getUser().getId().toString();
        List<UserCopyAllocationEntity> allocations = safeList(allocationRepository.findActiveByIdUser(userDetail.getUser().getId()));
        if (allocations.isEmpty()) {
            return List.of();
        }

        Map<String, PreconfigureCandidate> byUserSymbol = new HashMap<>();
        Set<String> conflicts = new HashSet<>();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        for (UserCopyAllocationEntity allocation : allocations) {
            if (allocation == null || !allocation.allowsNewEntries(now)) {
                continue;
            }
            Integer leverage = resolveLeverage(userDetail, allocation);
            if (leverage == null) {
                log.warn("event=binance.trading_config.preconfigure.skip reasonCode=invalid_leverage userId={} wallet={} allocationId={} leverageOverride={} userLeverage={}",
                        userId,
                        allocation.getWalletId(),
                        allocation.getId(),
                        allocation.getLeverageOverride(),
                        userDetail.getDetail() == null ? null : userDetail.getDetail().getLeverage());
                continue;
            }

            List<String> symbols = safeList(copyOperationRepository.findRecentLiveSymbolsForAllocation(
                    allocation.getId(),
                    Math.max(1, recentLookbackDays),
                    Math.max(1, maxSymbolsPerAllocation)
            ));

            for (String rawSymbol : symbols) {
                String symbol = normalizeSymbol(rawSymbol);
                if (symbol == null) {
                    continue;
                }
                String key = userId + '|' + symbol;
                PreconfigureCandidate candidate = new PreconfigureCandidate(
                        userDetail,
                        allocation.getId(),
                        allocation.getWalletId(),
                        symbol,
                        leverage,
                        normalizeMarginType(marginType)
                );
                PreconfigureCandidate existing = byUserSymbol.get(key);
                if (existing != null && !existing.sameTradingConfig(candidate)) {
                    conflicts.add(key);
                    log.warn("event=binance.trading_config.preconfigure.conflict userId={} symbol={} existingAllocationId={} existingLeverage={} newAllocationId={} newLeverage={} action=skip_symbol",
                            userId,
                            symbol,
                            existing.allocationId(),
                            existing.leverage(),
                            candidate.allocationId(),
                            candidate.leverage());
                    continue;
                }
                byUserSymbol.putIfAbsent(key, candidate);
            }
        }

        if (conflicts.isEmpty()) {
            return new ArrayList<>(byUserSymbol.values());
        }

        return byUserSymbol.entrySet().stream()
                .filter(entry -> !conflicts.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .toList();
    }

    private PreconfigureOutcome preconfigure(PreconfigureCandidate candidate, String traceId) {
        UserApiKeyEntity apiKey = candidate.userDetail().getUserApiKey();
        TradingConfigPreconfigureClientRequest request = TradingConfigPreconfigureClientRequest.builder()
                .symbol(candidate.symbol())
                .positionSide(PositionSide.BOTH)
                .leverage(candidate.leverage())
                .marginType(candidate.marginType())
                .skipIfOpenPosition(true)
                .build();

        ApiResponse<TradingConfigPreconfigureClientResponse> response = binanceClient.preconfigureTradingSettings(
                apiKey.getApiKey(),
                apiKey.getApiSecret(),
                candidate.userId(),
                candidate.walletId(),
                traceId,
                request
        );
        TradingConfigPreconfigureClientResponse data = response == null ? null : response.getData();
        if (data == null) {
            log.warn("event=binance.trading_config.preconfigure.skip reasonCode=empty_response traceId={} userId={} wallet={} allocationId={} symbol={}",
                    traceId, candidate.userId(), candidate.walletId(), candidate.allocationId(), candidate.symbol());
            return PreconfigureOutcome.SKIPPED;
        }

        String status = safeUpper(data.getStatus());
        log.info("event=binance.trading_config.preconfigure.result traceId={} userId={} wallet={} allocationId={} symbol={} leverage={} marginType={} status={} reasonCode={} marginTypeAction={} leverageAction={} openPositionDetected={}",
                traceId,
                candidate.userId(),
                candidate.walletId(),
                candidate.allocationId(),
                candidate.symbol(),
                candidate.leverage(),
                candidate.marginType(),
                status,
                safeLog(data.getReasonCode()),
                safeLog(data.getMarginTypeAction()),
                safeLog(data.getLeverageAction()),
                data.isOpenPositionDetected());

        if ("READY".equals(status)) {
            return PreconfigureOutcome.READY;
        }
        if ("PENDING_UNTIL_FLAT".equals(status)) {
            return PreconfigureOutcome.PENDING;
        }
        return PreconfigureOutcome.SKIPPED;
    }

    private boolean validUser(UserDetailDto userDetail) {
        return userDetail != null
                && userDetail.getUser() != null
                && userDetail.getUser().getId() != null
                && userDetail.getDetail() != null
                && userDetail.getDetail().isUserActive()
                && userDetail.getDetail().isApiKeyBinar()
                && userDetail.getUserApiKey() != null
                && hasText(userDetail.getUserApiKey().getApiKey())
                && hasText(userDetail.getUserApiKey().getApiSecret());
    }

    private Integer resolveLeverage(UserDetailDto userDetail, UserCopyAllocationEntity allocation) {
        Integer leverage = null;
        BigDecimal override = allocation == null ? null : allocation.getLeverageOverride();
        if (override != null && override.compareTo(BigDecimal.ZERO) > 0) {
            leverage = override.setScale(0, java.math.RoundingMode.HALF_UP).intValue();
        } else if (userDetail != null && userDetail.getDetail() != null) {
            leverage = userDetail.getDetail().getLeverage();
        }
        if (leverage == null || leverage < 1 || leverage > 125) {
            return null;
        }
        return leverage;
    }

    private static String normalizeSymbol(String value) {
        if (!hasText(value)) {
            return null;
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }

    private static String normalizeMarginType(String value) {
        if (!hasText(value)) {
            return "CROSSED";
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        return "CROSS".equals(normalized) ? "CROSSED" : normalized;
    }

    private static String safeUpper(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static <T> List<T> safeList(List<T> values) {
        return values == null ? List.of() : values.stream().filter(Objects::nonNull).toList();
    }

    private static String safeLog(String value) {
        if (value == null) {
            return "";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 1000 ? clean.substring(0, 1000) : clean;
    }

    private record PreconfigureCandidate(
            UserDetailDto userDetail,
            Long allocationId,
            String walletId,
            String symbol,
            Integer leverage,
            String marginType
    ) {
        private String userId() {
            return userDetail.getUser().getId().toString();
        }

        private boolean sameTradingConfig(PreconfigureCandidate other) {
            return other != null
                    && Objects.equals(leverage, other.leverage)
                    && Objects.equals(normalizeMarginType(marginType), normalizeMarginType(other.marginType));
        }
    }

    private enum PreconfigureOutcome {
        READY,
        PENDING,
        SKIPPED
    }
}
