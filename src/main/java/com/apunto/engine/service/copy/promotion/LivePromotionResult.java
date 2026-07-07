package com.apunto.engine.service.copy.promotion;

public record LivePromotionResult(
        int evaluated,
        int ready,
        int promoted,
        int rejected,
        int skipped
) {

    public static LivePromotionResult empty() {
        return new LivePromotionResult(0, 0, 0, 0, 0);
    }
}
