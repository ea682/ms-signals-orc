package com.apunto.engine.hyperliquid.dto;

public record HyperliquidDirectCopyDispatchResult(
        int eligibleUsers,
        int submittedTasks,
        int fallbackJobs,
        boolean fallbackUsed
) {
}
