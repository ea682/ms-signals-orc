package com.apunto.engine.service.copy.position;

import com.apunto.copytarget.ExistingTargetPosition;
import com.apunto.copytarget.SourceSide;
import com.apunto.copytarget.TargetPositionSnapshotStatus;
import com.apunto.engine.dto.UserDetailDto;
import com.apunto.engine.dto.client.BinanceFuturesPositionClientDto;
import com.apunto.engine.entity.UserApiKeyEntity;
import com.apunto.engine.service.ProcesBinanceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class BinanceTargetPositionSnapshotService {

    static final String SOURCE = "BINANCE_POSITION_RISK";
    static final String UNAVAILABLE = "BLOCKED_TARGET_POSITION_SNAPSHOT_UNAVAILABLE";
    static final String STALE = "BLOCKED_TARGET_POSITION_SNAPSHOT_STALE";

    private final ProcesBinanceService binance;
    private final Map<String, CachedSnapshot> snapshots = new ConcurrentHashMap<>();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    @Value("${copy.target-position-authority.cache-ttl-ms:250}")
    private long cacheTtlMs = 250L;

    @Value("${copy.target-position-authority.max-age-ms:1000}")
    private long maxAgeMs = 1_000L;

    public BinanceTargetPositionSnapshotService(ProcesBinanceService binance) {
        this.binance = binance;
    }

    public BinanceTargetPositionSnapshot load(UserDetailDto userDetail, String traceId) {
        UserApiKeyEntity credentials = userDetail == null ? null : userDetail.getUserApiKey();
        String userId = userDetail == null || userDetail.getUser() == null
                || userDetail.getUser().getId() == null
                ? null
                : userDetail.getUser().getId().toString();
        if (userId == null || !hasText(credentials == null ? null : credentials.getApiKey())
                || !hasText(credentials == null ? null : credentials.getApiSecret())) {
            return unavailable("Binance credentials or user identity are missing");
        }

        CachedSnapshot cached = snapshots.get(userId);
        if (isFresh(cached)) {
            return authoritative(cached);
        }

        Object lock = locks.computeIfAbsent(userId, ignored -> new Object());
        synchronized (lock) {
            cached = snapshots.get(userId);
            if (isFresh(cached)) {
                return authoritative(cached);
            }
            try {
                List<BinanceFuturesPositionClientDto> response = binance.getPositions(
                        credentials.getApiKey(), credentials.getApiSecret(), traceId);
                CachedSnapshot loaded = new CachedSnapshot(
                        Instant.now(), System.nanoTime(), mapPositions(response));
                snapshots.put(userId, loaded);
                log.info("event=copy.target_positions.snapshot_loaded reasonCode=TARGET_POSITION_SNAPSHOT_AUTHORITATIVE traceId={} userId={} positions={} cacheTtlMs={} maxAgeMs={} source={}",
                        safe(traceId), userId, loaded.positions().size(), nonNegative(cacheTtlMs),
                        nonNegative(maxAgeMs), SOURCE);
                return authoritative(loaded);
            } catch (RuntimeException failure) {
                if (cached != null) {
                    long ageMs = ageMs(cached);
                    log.warn("event=copy.target_positions.snapshot_stale reasonCode={} traceId={} userId={} ageMs={} maxAgeMs={} retryable=true shouldAlert=true errorClass={} errorMessage=\"{}\"",
                            STALE, safe(traceId), userId, ageMs, nonNegative(maxAgeMs),
                            failure.getClass().getSimpleName(), safe(failure.getMessage()));
                    return new BinanceTargetPositionSnapshot(
                            TargetPositionSnapshotStatus.STALE,
                            cached.observedAt(), SOURCE, cached.positions(), STALE,
                            "Authoritative Binance position refresh failed: " + failure.getClass().getSimpleName());
                }
                log.warn("event=copy.target_positions.snapshot_unavailable reasonCode={} traceId={} userId={} retryable=true shouldAlert=true errorClass={} errorMessage=\"{}\"",
                        UNAVAILABLE, safe(traceId), userId, failure.getClass().getSimpleName(),
                        safe(failure.getMessage()));
                return unavailable("Authoritative Binance position read failed: "
                        + failure.getClass().getSimpleName());
            }
        }
    }

    public void invalidate(String userId, String reasonCode) {
        if (!hasText(userId)) {
            return;
        }
        boolean removed = snapshots.remove(userId.trim()) != null;
        log.info("event=copy.target_positions.snapshot_invalidated reasonCode={} userId={} cacheEntryRemoved={}",
                safe(reasonCode), userId.trim(), removed);
    }

    static List<ExistingTargetPosition> mapPositions(List<BinanceFuturesPositionClientDto> values) {
        List<ExistingTargetPosition> result = new ArrayList<>();
        for (BinanceFuturesPositionClientDto value : values == null ? List.<BinanceFuturesPositionClientDto>of() : values) {
            if (value == null) {
                continue;
            }
            String symbol = required(value.getSymbol(), "symbol");
            BigDecimal signedQuantity = decimal(value.getPositionAmt(), "positionAmt");
            if (signedQuantity.signum() == 0) {
                continue;
            }
            SourceSide side = side(value.getPositionSide(), signedQuantity);
            BigDecimal markPrice = optionalDecimal(value.getMarkPrice());
            BigDecimal margin = optionalDecimal(value.getIsolatedMargin());
            result.add(new ExistingTargetPosition(
                    symbol, side, signedQuantity.abs(), markPrice, margin));
        }
        return List.copyOf(result);
    }

    void setCachePolicyForTest(long cacheTtlMs, long maxAgeMs) {
        this.cacheTtlMs = cacheTtlMs;
        this.maxAgeMs = maxAgeMs;
    }

    private boolean isFresh(CachedSnapshot cached) {
        return cached != null
                && ageMs(cached) <= nonNegative(cacheTtlMs)
                && ageMs(cached) <= nonNegative(maxAgeMs);
    }

    private long ageMs(CachedSnapshot cached) {
        return Duration.ofNanos(Math.max(0L, System.nanoTime() - cached.loadedAtNanos())).toMillis();
    }

    private BinanceTargetPositionSnapshot authoritative(CachedSnapshot cached) {
        return new BinanceTargetPositionSnapshot(
                TargetPositionSnapshotStatus.AUTHORITATIVE,
                cached.observedAt(), SOURCE, cached.positions(),
                "TARGET_POSITION_SNAPSHOT_AUTHORITATIVE", "");
    }

    private BinanceTargetPositionSnapshot unavailable(String detail) {
        return new BinanceTargetPositionSnapshot(
                TargetPositionSnapshotStatus.UNAVAILABLE,
                null, SOURCE, List.of(), UNAVAILABLE, detail);
    }

    private static SourceSide side(String positionSide, BigDecimal signedQuantity) {
        String normalized = required(positionSide, "positionSide").toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "LONG" -> SourceSide.LONG;
            case "SHORT" -> SourceSide.SHORT;
            case "BOTH" -> signedQuantity.signum() > 0 ? SourceSide.LONG : SourceSide.SHORT;
            default -> throw new IllegalArgumentException("Unsupported Binance positionSide: " + normalized);
        };
    }

    private static BigDecimal decimal(String value, String field) {
        try {
            return new BigDecimal(required(value, field));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(field + " must be decimal", failure);
        }
    }

    private static BigDecimal optionalDecimal(String value) {
        if (!hasText(value)) {
            return BigDecimal.ZERO;
        }
        try {
            return new BigDecimal(value.trim()).abs();
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("Binance position decimal is invalid", failure);
        }
    }

    private static String required(String value, String field) {
        if (!hasText(value)) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value.trim();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static long nonNegative(long value) {
        return Math.max(0L, value);
    }

    private static String safe(String value) {
        if (value == null) {
            return "";
        }
        return value.replace('\n', '_').replace('\r', '_').replace('"', '_');
    }

    private record CachedSnapshot(
            Instant observedAt,
            long loadedAtNanos,
            List<ExistingTargetPosition> positions
    ) { }
}
