package com.apunto.engine.service.copy.observability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyFlowTimingTest {

    @Test
    void logfmtCoreHasStableLowCardinalityFields() {
        CopyFlowTiming timing = CopyFlowTiming.start();
        long mark = timing.mark();
        timing.add(CopyFlowTiming.Stage.BINANCE_HTTP, mark);

        String logfmt = timing.logfmtCore("live", "success");

        assertTrue(logfmt.contains("event=copy_flow_latency"));
        assertTrue(logfmt.contains("service=ms-signals-orc"));
        assertTrue(logfmt.contains("flow=live"));
        assertTrue(logfmt.contains("stage=total"));
        assertTrue(logfmt.contains("result=success"));
        assertTrue(logfmt.contains("totalMs="));
        assertTrue(logfmt.contains("binanceHttpMs="));
        assertFalse(logfmt.contains("walletId="));
        assertFalse(logfmt.contains("originId="));
        assertFalse(logfmt.contains("profileKey="));
    }
}
