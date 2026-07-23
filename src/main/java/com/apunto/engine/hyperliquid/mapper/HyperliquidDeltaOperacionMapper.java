package com.apunto.engine.hyperliquid.mapper;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.identity.HyperliquidSourceTradeIdentity;
import com.apunto.engine.shared.enums.PositionSide;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;

@Component
public class HyperliquidDeltaOperacionMapper {

    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private final MeterRegistry meterRegistry;

    public HyperliquidDeltaOperacionMapper() {
        this.meterRegistry = null;
    }

    @Autowired
    public HyperliquidDeltaOperacionMapper(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public HyperliquidMappedDelta map(HyperliquidDeltaRequest request, String headerIdempotencyKey) {
        if (request == null) {
            throw new IllegalArgumentException("Hyperliquid delta request is required");
        }

        final String suppliedIdempotencyKey = firstNonBlank(headerIdempotencyKey, request.idempotencyKey());
        if (suppliedIdempotencyKey == null) {
            throw new IllegalArgumentException("X-Idempotency-Key or idempotencyKey is required");
        }

        final String wallet = requireText(request.wallet(), "wallet is required").toLowerCase(Locale.ROOT);
        final String rawSymbol = requireText(request.symbol(), "symbol is required");
        final String symbol = toEngineSymbol(rawSymbol);
        final String idempotencyKey = canonicalIdempotencyKey(
                request, suppliedIdempotencyKey, wallet, symbol);
        final PositionSide side = mapSide(request.side());
        final String deltaType = requireText(request.deltaType(), "deltaType is required").toUpperCase(Locale.ROOT);
        final OperacionEvent.Tipo eventType = mapEventType(deltaType, request.status(), request.sizeQty());
        final boolean active = eventType == OperacionEvent.Tipo.ABIERTA;
        final UUID positionId = stablePositionId(wallet, symbol, side.name());
        final BigDecimal sizeQty = abs(defaultZero(firstNonNull(request.sizeQty(), request.signedSizeQty())));
        final BigDecimal notionalUsd = resolveNotionalUsd(request, sizeQty, eventType);
        final BigDecimal marginUsedUsd = resolveMarginUsedUsd(request, eventType);
        final BigDecimal entryPriceRef = firstNonNull(request.effectiveEntryPrice(), request.entryPrice());
        final BigDecimal closePriceRef = resolveClosePrice(request);
        final BigDecimal marketPriceRef = resolveMarketPrice(request);
        final Instant eventTime = resolveEventTime(request);

        OperacionDto operacion = OperacionDto.builder()
                .idOperacion(positionId)
                .idCuenta(wallet)
                .parSymbol(symbol)
                .tipoOperacion(side)
                .size(notionalUsd)
                .sizeQty(sizeQty)
                .notionalUsd(notionalUsd)
                .marginUsedUsd(marginUsedUsd)
                .leverage(request.leverage())
                .precioEntrada(entryPriceRef)
                .precioCierre(active ? null : closePriceRef)
                .precioMercado(marketPriceRef)
                .fechaCreacion(eventTime)
                .fechaCierre(active ? null : eventTime)
                .operacionActiva(active)
                .sourceAccountEquityUsd(request.sourceAccountEquityUsd())
                .equityObservedAt(request.equityObservedAt())
                .equitySource(request.equitySource())
                .equityFreshnessMs(request.equityFreshnessMs())
                .equityQuality(request.equityQuality())
                .sourceSnapshotVersion(request.sourceSnapshotVersion())
                .sourceEventId(request.sourceEventId())
                .sourcePositionNotionalUsd(request.sourcePositionNotionalUsd())
                .sourcePortfolioPositions(request.sourcePortfolioPositions())
                .sourcePortfolioSnapshotVersion(request.sourcePortfolioSnapshotVersion())
                .sourcePortfolioComplete(request.sourcePortfolioComplete())
                .build();

        return new HyperliquidMappedDelta(
                idempotencyKey,
                positionKey(wallet, symbol, side.name()),
                wallet,
                symbol,
                side.name(),
                deltaType,
                new OperacionEvent(eventType, operacion, deltaType, idempotencyKey, positionKey(wallet, symbol, side.name())),
                request
        );
    }

    private String canonicalIdempotencyKey(HyperliquidDeltaRequest request,
                                           String supplied,
                                           String wallet,
                                           String symbol) {
        String externalId = request.externalId();
        if (externalId == null || externalId.isBlank()) {
            return supplied;
        }
        String cleanExternalId = externalId.trim();
        if (cleanExternalId.startsWith("snapshot-recovery-open|")) {
            return HyperliquidSourceTradeIdentity.recoveryKey(
                    wallet, symbol, cleanExternalId);
        }
        HyperliquidSourceTradeIdentity.Evidence sourceTrade =
                HyperliquidSourceTradeIdentity.fromExternalId(
                        cleanExternalId, wallet, symbol);
        if (sourceTrade.zeroHash()) {
            recordZeroHashIdentity();
        }
        if (sourceTrade.tid() != null) {
            String canonical =
                    HyperliquidSourceTradeIdentity.canonicalTradeKey(
                    wallet, sourceTrade.tid());
            if (!canonical.equals(supplied)) {
                recordLegacyIdentityMatched();
            }
            return canonical;
        }
        String[] parts = cleanExternalId.split("\\|", 4);
        if (parts.length != 4
                || !parts[0].equalsIgnoreCase(wallet)
                || !toEngineSymbol(parts[1]).equalsIgnoreCase(symbol)
                || !isSourceDeltaType(parts[2])) {
            return supplied;
        }
        return HyperliquidSourceTradeIdentity.fallbackTradeKey(
                wallet, symbol, parts[3]);
    }

    private void recordZeroHashIdentity() {
        if (meterRegistry != null) {
            meterRegistry.counter(
                    "zero_hash_identity_total",
                    "source", "direct_ingest"
            ).increment();
        }
    }

    private void recordLegacyIdentityMatched() {
        if (meterRegistry != null) {
            meterRegistry.counter(
                    "legacy_identity_matched_total",
                    "source", "direct_ingest"
            ).increment();
        }
    }

    private boolean isSourceDeltaType(String value) {
        if (value == null || value.isBlank()) return false;
        return switch (value.trim().toUpperCase(Locale.ROOT)) {
            case "OPEN", "CLOSE", "RESIZE", "FLIP", "UPDATE", "NO_CHANGE" -> true;
            default -> false;
        };
    }

    private OperacionEvent.Tipo mapEventType(String deltaType, String status, BigDecimal sizeQty) {
        if ("NO_CHANGE".equals(deltaType)) {
            throw new IllegalArgumentException("NO_CHANGE delta is not copyable");
        }
        if ("CLOSE".equals(deltaType) || "CLOSED".equalsIgnoreCase(nullToEmpty(status))) {
            return OperacionEvent.Tipo.CERRADA;
        }
        if (abs(sizeQty).compareTo(ZERO) == 0 && ("UPDATE".equals(deltaType) || "RESIZE".equals(deltaType))) {
            return OperacionEvent.Tipo.CERRADA;
        }
        if ("OPEN".equals(deltaType) || "RESIZE".equals(deltaType) || "FLIP".equals(deltaType) || "UPDATE".equals(deltaType)) {
            return OperacionEvent.Tipo.ABIERTA;
        }
        throw new IllegalArgumentException("Unsupported deltaType: " + deltaType);
    }

    private PositionSide mapSide(String rawSide) {
        final String value = requireText(rawSide, "side is required").toUpperCase(Locale.ROOT);
        try {
            return PositionSide.valueOf(value);
        } catch (IllegalArgumentException invalidSide) {
            throw new IllegalArgumentException("Unsupported side: " + rawSide, invalidSide);
        }
    }

    private UUID stablePositionId(String wallet, String symbol, String side) {
        return UUID.nameUUIDFromBytes(positionKey(wallet, symbol, side).getBytes(StandardCharsets.UTF_8));
    }

    private String positionKey(String wallet, String symbol, String side) {
        return "hyperliquid-position:" + wallet + ':' + symbol + ':' + side;
    }

    private String toEngineSymbol(String rawSymbol) {
        String normalized = requireText(rawSymbol, "symbol is required")
                .trim()
                .toUpperCase(Locale.ROOT)
                .replace("-", "")
                .replace("_", "")
                .replace("/", "")
                .replace(".", "")
                .replace(" ", "");

        if (normalized.endsWith("USDT")
                || normalized.endsWith("USDC")
                || normalized.endsWith("FDUSD")
                || normalized.endsWith("BUSD")
                || normalized.endsWith("USD")) {
            return normalized;
        }
        return normalized + "USD";
    }

    private Instant resolveEventTime(HyperliquidDeltaRequest request) {
        if (request.sourceTs() != null && request.sourceTs() > 0) {
            return Instant.ofEpochMilli(request.sourceTs());
        }
        return firstNonNull(request.detectedAt(), Instant.now());
    }

    private BigDecimal resolveClosePrice(HyperliquidDeltaRequest request) {
        return firstNonNull(request.effectiveExitPrice(), firstNonNull(request.markPrice(), firstNonNull(request.effectiveEntryPrice(), request.entryPrice())));
    }

    private BigDecimal resolveMarketPrice(HyperliquidDeltaRequest request) {
        return firstNonNull(request.markPrice(), firstNonNull(request.effectiveExitPrice(), firstNonNull(request.effectiveEntryPrice(), request.entryPrice())));
    }

    private BigDecimal resolveMarginUsedUsd(HyperliquidDeltaRequest request, OperacionEvent.Tipo eventType) {
        if (eventType == OperacionEvent.Tipo.CERRADA) {
            BigDecimal closedMargin = abs(request.closedMarginUsedUsd());
            if (closedMargin.compareTo(ZERO) > 0) {
                return closedMargin;
            }
        }
        return abs(request.marginUsedUsd());
    }

    private BigDecimal resolveNotionalUsd(HyperliquidDeltaRequest request, BigDecimal sizeQty, OperacionEvent.Tipo eventType) {
        if (eventType == OperacionEvent.Tipo.CERRADA) {
            BigDecimal closedNotional = abs(request.closedNotionalUsd());
            if (closedNotional.compareTo(ZERO) > 0) {
                return closedNotional;
            }
        }
        BigDecimal positionNotional = abs(firstNonNull(request.positionNotionalUsd(), request.notionalUsd()));
        if (positionNotional.compareTo(ZERO) > 0) {
            return positionNotional;
        }
        BigDecimal effectiveNotional = abs(request.closedNotionalUsd());
        if (effectiveNotional.compareTo(ZERO) > 0) {
            return effectiveNotional;
        }
        BigDecimal price = firstNonNull(request.markPrice(), request.entryPrice());
        if (price == null || price.compareTo(ZERO) <= 0 || sizeQty == null) {
            return ZERO;
        }
        return abs(sizeQty).multiply(price.abs());
    }

    private String requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
        return value.trim();
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first.trim();
        }
        if (second != null && !second.isBlank()) {
            return second.trim();
        }
        return null;
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private BigDecimal defaultZero(BigDecimal value) {
        return value == null ? ZERO : value;
    }

    private BigDecimal abs(BigDecimal value) {
        return value == null ? ZERO : value.abs();
    }

    private <T> T firstNonNull(T first, T second) {
        return first != null ? first : second;
    }
}
