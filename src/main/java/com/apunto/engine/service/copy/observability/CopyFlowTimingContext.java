package com.apunto.engine.service.copy.observability;

public final class CopyFlowTimingContext {

    private static final ThreadLocal<CopyFlowTiming> LIVE = new ThreadLocal<>();

    private CopyFlowTimingContext() {
    }

    public static CopyFlowTiming currentLive() {
        return LIVE.get();
    }

    public static Scope openLive(long eventReceivedNs) {
        CopyFlowTiming previous = LIVE.get();
        CopyFlowTiming timing = previous == null
                ? CopyFlowTiming.fromEventReceivedNs(eventReceivedNs)
                : previous;
        if (previous == null) {
            LIVE.set(timing);
        }
        return new Scope(previous);
    }

    public static final class Scope implements AutoCloseable {
        private final CopyFlowTiming previous;
        private boolean closed;

        private Scope(CopyFlowTiming previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (previous == null) {
                LIVE.remove();
            } else {
                LIVE.set(previous);
            }
        }
    }
}
