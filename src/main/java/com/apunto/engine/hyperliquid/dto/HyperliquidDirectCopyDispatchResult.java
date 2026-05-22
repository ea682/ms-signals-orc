package com.apunto.engine.hyperliquid.dto;

public record HyperliquidDirectCopyDispatchResult(
        int eligibleUsers,
        int submittedTasks,
        int businessSkipped,
        int fallbackJobs,
        boolean fallbackUsed,
        String reasonCode
) {
    public static HyperliquidDirectCopyDispatchResult ok(
            int eligibleUsers,
            int submittedTasks,
            int businessSkipped,
            int fallbackJobs,
            boolean fallbackUsed,
            String reasonCode
    ) {
        return new HyperliquidDirectCopyDispatchResult(
                eligibleUsers,
                submittedTasks,
                businessSkipped,
                fallbackJobs,
                fallbackUsed,
                normalizeReason(reasonCode)
        );
    }

    private static String normalizeReason(String reasonCode) {
        if (reasonCode == null || reasonCode.isBlank()) {
            return null;
        }
        return reasonCode.trim();
    }
}
