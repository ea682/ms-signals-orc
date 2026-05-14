package com.apunto.engine.hyperliquid.service.impl;

import com.apunto.engine.dto.OperacionDto;
import com.apunto.engine.entity.FuturesPositionEntity;
import com.apunto.engine.events.OperacionEvent;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.repository.FuturesPositionRepository;
import com.apunto.engine.shared.enums.PositionStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class HyperliquidOriginPositionStoreService {

    private static final String PLATFORM = "hyperliquid";
    private static final String VENUE = "HYPERLIQUID";
    private static final BigDecimal ZERO = BigDecimal.ZERO;
    private static final BigDecimal ONE = BigDecimal.ONE;

    private final FuturesPositionRepository repository;
    private final ObjectMapper objectMapper;

    @Transactional
    public HyperliquidMappedDelta persistAndBind(HyperliquidMappedDelta mapped) {
        if (mapped == null || mapped.event() == null || mapped.event().getOperacion() == null) {
            throw new IllegalArgumentException("mapped hyperliquid delta is required");
        }

        long startedNs = System.nanoTime();
        OperacionEvent event = mapped.event();
        OperacionDto operation = event.getOperacion();
        UUID originId = operation.getIdOperacion();
        if (originId == null) {
            throw new IllegalArgumentException("mapped operation id is required");
        }

        FuturesPositionEntity entity = repository.findByIdFuturesPosition(originId).orElse(null);
        boolean created = entity == null;
        if (created) {
            entity = new FuturesPositionEntity();
            entity.setIdFuturesPosition(originId);
            entity.setPlatform(PLATFORM);
            entity.setVenue(VENUE);
            entity.setExternalId(resolveInitialExternalId(mapped));
            entity.setCreatedAt(resolveCreatedAt(operation, mapped.request()));
            entity.setIngestedAt(OffsetDateTime.now(ZoneOffset.UTC));
            entity.setFailedAttempts(0);
            entity.setHasAccountIssue(false);
        }

        applyCommonFields(entity, mapped, operation);
        applyStatusFields(entity, event, operation);

        FuturesPositionEntity saved = repository.saveAndFlush(entity);
        OperacionEvent rebound = new OperacionEvent(event.getTipo(), operationWithId(operation, saved.getIdFuturesPosition()));

        log.info("event=futures_position.origin_upsert_ok action={} originId={} created={} platform={} wallet={} symbol={} side={} status={} deltaType={} sourceTs={} elapsedMs={}",
                created ? "insert" : "update",
                saved.getIdFuturesPosition(),
                created,
                safeLog(saved.getPlatform()),
                safeLog(saved.getAccountId()),
                safeLog(saved.getSymbol()),
                saved.getSide(),
                saved.getStatus(),
                safeLog(mapped.deltaType()),
                saved.getSourceTs(),
                elapsedMs(startedNs));

        return new HyperliquidMappedDelta(
                mapped.idempotencyKey(),
                mapped.positionKey(),
                mapped.wallet(),
                mapped.symbol(),
                mapped.side(),
                mapped.deltaType(),
                rebound,
                mapped.request()
        );
    }

    private void applyCommonFields(FuturesPositionEntity entity, HyperliquidMappedDelta mapped, OperacionDto operation) {
        HyperliquidDeltaRequest request = mapped.request();
        entity.setPlatform(PLATFORM);
        entity.setVenue(VENUE);
        entity.setAccountId(lower(firstNonBlank(operation.getIdCuenta(), mapped.wallet())));
        entity.setSymbol(firstNonBlank(operation.getParSymbol(), mapped.symbol()));
        entity.setSide(operation.getTipoOperacion());
        entity.setOperationType(operation.getTipoOperacion() == null ? safeUpper(mapped.side()) : operation.getTipoOperacion().name());
        entity.setLeverage(positiveOrDefault(request == null ? null : request.leverage(), ONE));
        entity.setSizeQty(nonNegative(operation.getSizeQty()));
        entity.setNotionalUsd(nonNegative(operation.getNotionalUsd()));
        entity.setMarginUsedUsd(nonNegative(operation.getMarginUsedUsd()));
        entity.setSizeLegacy(nonNegative(firstNonNull(operation.getSize(), operation.getNotionalUsd(), operation.getSizeQty())));
        entity.setEntryPrice(nonNegative(firstNonNull(operation.getPrecioEntrada(), operation.getPrecioMercado(), ZERO)));
        entity.setMarkPrice(nonNegative(firstNonNull(operation.getPrecioMercado(), operation.getPrecioEntrada(), ZERO)));
        entity.setUnrealizedPnlUsd(ZERO);
        entity.setSourceTs(resolveSourceTs(operation, request));
        entity.setUpdatedAt(OffsetDateTime.now(ZoneOffset.UTC));
        entity.setRaw(raw(mapped, operation));
    }

    private void applyStatusFields(FuturesPositionEntity entity, OperacionEvent event, OperacionDto operation) {
        boolean open = event.getTipo() == OperacionEvent.Tipo.ABIERTA && operation.isOperacionActiva();
        if (open) {
            entity.setStatus(PositionStatus.OPEN);
            entity.setIsActive(true);
            entity.setClosedAt(null);
            entity.setExitPrice(null);
            return;
        }

        OffsetDateTime closedAt = toOffsetDateTime(firstNonNull(operation.getFechaCierre(), operation.getFechaCreacion(), Instant.now()));
        BigDecimal exitPrice = nonNegative(firstNonNull(operation.getPrecioCierre(), operation.getPrecioMercado(), operation.getPrecioEntrada(), ZERO));
        entity.setStatus(PositionStatus.CLOSED);
        entity.setIsActive(false);
        entity.setClosedAt(closedAt);
        entity.setExitPrice(exitPrice);
    }

    private OperacionDto operationWithId(OperacionDto source, UUID id) {
        return OperacionDto.builder()
                .idOperacion(id)
                .idCuenta(source.getIdCuenta())
                .parSymbol(source.getParSymbol())
                .tipoOperacion(source.getTipoOperacion())
                .size(source.getSize())
                .sizeQty(source.getSizeQty())
                .notionalUsd(source.getNotionalUsd())
                .marginUsedUsd(source.getMarginUsedUsd())
                .precioEntrada(source.getPrecioEntrada())
                .precioCierre(source.getPrecioCierre())
                .precioMercado(source.getPrecioMercado())
                .fechaCreacion(source.getFechaCreacion())
                .fechaCierre(source.getFechaCierre())
                .operacionActiva(source.isOperacionActiva())
                .build();
    }

    private String resolveInitialExternalId(HyperliquidMappedDelta mapped) {
        HyperliquidDeltaRequest request = mapped.request();
        String externalId = request == null ? null : request.externalId();
        if (externalId != null && !externalId.isBlank()) {
            return externalId.trim();
        }
        return "hyperliquid|direct|" + mapped.positionKey();
    }

    private OffsetDateTime resolveCreatedAt(OperacionDto operation, HyperliquidDeltaRequest request) {
        if (operation.getFechaCreacion() != null) {
            return toOffsetDateTime(operation.getFechaCreacion());
        }
        if (request != null && request.sourceTs() != null && request.sourceTs() > 0) {
            return OffsetDateTime.ofInstant(Instant.ofEpochMilli(request.sourceTs()), ZoneOffset.UTC);
        }
        return OffsetDateTime.now(ZoneOffset.UTC);
    }

    private OffsetDateTime resolveSourceTs(OperacionDto operation, HyperliquidDeltaRequest request) {
        if (request != null && request.sourceTs() != null && request.sourceTs() > 0) {
            return OffsetDateTime.ofInstant(Instant.ofEpochMilli(request.sourceTs()), ZoneOffset.UTC);
        }
        if (operation.getFechaCreacion() != null) {
            return toOffsetDateTime(operation.getFechaCreacion());
        }
        return OffsetDateTime.now(ZoneOffset.UTC);
    }

    private JsonNode raw(HyperliquidMappedDelta mapped, OperacionDto operation) {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("source", "ms-signals-orc.hyperliquid.direct_ingest");
        raw.put("idempotencyKey", mapped.idempotencyKey());
        raw.put("positionKey", mapped.positionKey());
        raw.put("deltaType", mapped.deltaType());
        raw.put("wallet", mapped.wallet());
        raw.put("symbol", mapped.symbol());
        raw.put("side", mapped.side());
        raw.put("operationActive", operation.isOperacionActiva());
        raw.put("request", mapped.request());
        return objectMapper.valueToTree(raw);
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private BigDecimal nonNegative(BigDecimal value) {
        if (value == null) {
            return ZERO;
        }
        return value.signum() < 0 ? value.abs() : value;
    }

    private BigDecimal positiveOrDefault(BigDecimal value, BigDecimal fallback) {
        if (value == null || value.compareTo(ZERO) <= 0) {
            return fallback;
        }
        return value;
    }

    private String lower(String value) {
        return value == null ? null : value.toLowerCase(Locale.ROOT);
    }

    private String safeUpper(String value) {
        return value == null ? "UNKNOWN" : value.toUpperCase(Locale.ROOT);
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first.trim();
        }
        if (second != null && !second.isBlank()) {
            return second.trim();
        }
        return "NA";
    }

    @SafeVarargs
    private <T> T firstNonNull(T... values) {
        if (values == null) {
            return null;
        }
        for (T value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private long elapsedMs(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs).toMillis();
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 500 ? clean.substring(0, 500) : clean;
    }
}
