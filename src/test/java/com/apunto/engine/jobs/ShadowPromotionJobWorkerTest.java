package com.apunto.engine.jobs;

import com.apunto.engine.service.copy.promotion.ShadowPromotionResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ShadowPromotionJobWorkerTest {

    @Test
    void doesNotCrashOnServiceException() {
        ShadowPromotionJobWorker worker = new ShadowPromotionJobWorker(() -> {
            throw new IllegalStateException("boom");
        });
        enable(worker);

        assertDoesNotThrow(worker::promoteShadowToMicroLive);
    }

    @Test
    void runsServiceWhenEnabled() {
        ShadowPromotionJobWorker worker = new ShadowPromotionJobWorker(() -> new ShadowPromotionResult(1, 1, 1, 0, 0));
        enable(worker);

        assertDoesNotThrow(worker::promoteShadowToMicroLive);
    }

    private static void enable(ShadowPromotionJobWorker worker) {
        try {
            var field = ShadowPromotionJobWorker.class.getDeclaredField("enabled");
            field.setAccessible(true);
            field.setBoolean(worker, true);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
