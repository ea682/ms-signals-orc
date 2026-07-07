package com.apunto.engine.service.copy.promotion;

public record ShadowPromotionResult(
        int evaluated,
        int ready,
        int created,
        int rejected,
        int skipped
) {

    public static ShadowPromotionResult empty() {
        return new ShadowPromotionResult(0, 0, 0, 0, 0);
    }
}
