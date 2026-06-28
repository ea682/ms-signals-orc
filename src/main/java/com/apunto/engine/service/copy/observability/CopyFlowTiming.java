package com.apunto.engine.service.copy.observability;

import java.util.EnumMap;
import java.util.Map;

public final class CopyFlowTiming {

    public enum Stage {
        DEDUPE,
        QUEUE,
        CLASSIFICATION,
        PRICE_RESOLUTION,
        COPY_GUARD,
        PRE_BINANCE,
        BINANCE_HTTP,
        BINANCE_RESPONSE_TO_PERSIST,
        ACCOUNTING,
        DB_PERSIST
    }

    private final long eventReceivedNs;
    private final EnumMap<Stage, Long> stageMs = new EnumMap<>(Stage.class);

    private CopyFlowTiming(long eventReceivedNs) {
        this.eventReceivedNs = eventReceivedNs <= 0 ? System.nanoTime() : eventReceivedNs;
    }

    public static CopyFlowTiming start() {
        return new CopyFlowTiming(System.nanoTime());
    }

    public long mark() {
        return System.nanoTime();
    }

    public void add(Stage stage, long startedNs) {
        if (stage == null || startedNs <= 0) {
            return;
        }
        addMs(stage, elapsedMs(startedNs, System.nanoTime()));
    }

    public void addMs(Stage stage, long millis) {
        if (stage == null || millis < 0) {
            return;
        }
        stageMs.merge(stage, millis, Long::sum);
    }

    public long totalMs() {
        return elapsedMs(eventReceivedNs, System.nanoTime());
    }

    public long stageMs(Stage stage) {
        return stageMs.getOrDefault(stage, 0L);
    }

    public Map<Stage, Long> stages() {
        return Map.copyOf(stageMs);
    }

    public String logfmtCore(String flow, String result) {
        long total = totalMs();
        return "event=copy_flow_latency service=ms-signals-orc flow=" + safe(flow)
                + " stage=total result=" + safe(result)
                + " totalMs=" + total
                + " endToEndMs=" + total
                + " queueDelayMs=" + stageMs(Stage.QUEUE)
                + " dedupeMs=" + stageMs(Stage.DEDUPE)
                + " classificationMs=" + stageMs(Stage.CLASSIFICATION)
                + " priceResolutionMs=" + stageMs(Stage.PRICE_RESOLUTION)
                + " copyGuardMs=" + stageMs(Stage.COPY_GUARD)
                + " preBinanceMs=" + stageMs(Stage.PRE_BINANCE)
                + " binanceHttpMs=" + stageMs(Stage.BINANCE_HTTP)
                + " binanceResponseToPersistMs=" + stageMs(Stage.BINANCE_RESPONSE_TO_PERSIST)
                + " accountingMs=" + stageMs(Stage.ACCOUNTING)
                + " dbPersistMs=" + stageMs(Stage.DB_PERSIST);
    }

    private long elapsedMs(long startNs, long endNs) {
        return Math.max(0L, (endNs - startNs) / 1_000_000L);
    }

    private String safe(String value) {
        if (value == null || value.isBlank()) {
            return "NA";
        }
        return value.replace(' ', '_').replace('\n', '_').replace('\r', '_').replace('\t', '_');
    }
}
