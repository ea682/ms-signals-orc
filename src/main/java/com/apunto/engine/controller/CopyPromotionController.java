package com.apunto.engine.controller;

import com.apunto.engine.service.MicroLivePromotionService;
import com.apunto.engine.service.ShadowPromotionService;
import com.apunto.engine.service.copy.promotion.LivePromotionResult;
import com.apunto.engine.service.copy.promotion.ShadowPromotionResult;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/v1/copy/promotions")
public class CopyPromotionController {

    private final ShadowPromotionService shadowPromotionService;
    private final MicroLivePromotionService microLivePromotionService;

    @PostMapping("/shadow-to-micro-live/run")
    public ShadowPromotionResult promoteShadowToMicroLive() {
        return shadowPromotionService.promoteShadowToMicroLive();
    }

    @PostMapping("/micro-live-to-live/run")
    public LivePromotionResult promoteMicroLiveToLive() {
        return microLivePromotionService.promoteMicroLiveToLive();
    }
}
