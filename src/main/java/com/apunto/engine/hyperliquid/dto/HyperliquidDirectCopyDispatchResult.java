package com.apunto.engine.hyperliquid.dto;

public record HyperliquidDirectCopyDispatchResult(
        int eligibleUsers,
        int submittedTasks,
        int businessSkipped,
        int fallbackJobs,
        boolean fallbackUsed
) {
}
