package com.apunto.engine.hyperliquid.controller;

import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaAcceptedResponse;
import com.apunto.engine.hyperliquid.dto.HyperliquidDeltaRequest;
import com.apunto.engine.hyperliquid.dto.HyperliquidMappedDelta;
import com.apunto.engine.hyperliquid.mapper.HyperliquidDeltaOperacionMapper;
import com.apunto.engine.hyperliquid.service.HyperliquidDirectDeltaIngestService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;

@Slf4j
@RestController
@RequestMapping("/internal/v1/hyperliquid/deltas")
@RequiredArgsConstructor
public class HyperliquidDirectDeltaController {

    private final HyperliquidDeltaOperacionMapper mapper;
    private final HyperliquidDirectDeltaIngestService ingestService;

    @PostMapping
    public ResponseEntity<HyperliquidDeltaAcceptedResponse> receiveDelta(
            @RequestHeader(value = "X-Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Source-Service", required = false) String sourceService,
            @Valid @RequestBody HyperliquidDeltaRequest request
    ) {
        long startedNs = System.nanoTime();
        HyperliquidMappedDelta mappedDelta = mapper.map(request, idempotencyKey);
        HyperliquidDeltaAcceptedResponse response = ingestService.accept(mappedDelta);
        log.debug("event=hyperliquid.direct_delta.accepted sourceService={} idempotencyKey={} positionKey={} wallet={} symbol={} side={} deltaType={} duplicate={} queueDepth={} httpElapsedMs={}",
                safeLog(sourceService),
                response.idempotencyKey(),
                response.positionKey(),
                response.wallet(),
                response.symbol(),
                response.side(),
                response.deltaType(),
                response.duplicate(),
                response.queueDepth(),
                Duration.ofNanos(System.nanoTime() - startedNs).toMillis());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    private String safeLog(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        String clean = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('"', '\'');
        return clean.length() > 200 ? clean.substring(0, 200) : clean;
    }
}
