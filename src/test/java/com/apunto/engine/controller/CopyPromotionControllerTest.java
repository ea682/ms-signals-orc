package com.apunto.engine.controller;

import com.apunto.engine.service.copy.promotion.LivePromotionResult;
import com.apunto.engine.service.copy.promotion.ShadowPromotionResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CopyPromotionControllerTest {

    @Test
    void manualRunReturnsSummary() {
        CopyPromotionController controller = new CopyPromotionController(
                () -> new ShadowPromotionResult(2, 1, 1, 1, 0),
                () -> new LivePromotionResult(1, 1, 1, 0, 0)
        );

        ShadowPromotionResult result = controller.promoteShadowToMicroLive();

        assertEquals(2, result.evaluated());
        assertEquals(1, result.created());
        assertEquals(1, result.rejected());
    }

    @Test
    void manualMicroLiveToLiveRunReturnsSummary() {
        CopyPromotionController controller = new CopyPromotionController(
                () -> new ShadowPromotionResult(0, 0, 0, 0, 0),
                () -> new LivePromotionResult(1, 1, 1, 0, 0)
        );

        LivePromotionResult result = controller.promoteMicroLiveToLive();

        assertEquals(1, result.evaluated());
        assertEquals(1, result.promoted());
    }
}
