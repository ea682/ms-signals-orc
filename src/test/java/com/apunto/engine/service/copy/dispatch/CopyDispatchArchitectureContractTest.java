package com.apunto.engine.service.copy.dispatch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CopyDispatchArchitectureContractTest {

    @Test
    void liveHotPathDoesNotCallMetricService() throws IOException {
        String dispatchSource = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyDispatchCoordinator.java");
        String resolverSource = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidCopyCandidateResolver.java");

        assertFalse(dispatchSource.contains("MetricWalletService"));
        assertFalse(dispatchSource.contains("MetricService"));
        assertFalse(resolverSource.contains("MetricWalletService"));
        assertTrue(Arrays.stream(CopyDispatchCoordinator.class.getDeclaredFields())
                .noneMatch(field -> field.getType().getSimpleName().toLowerCase().contains("metric")));
    }

    @Test
    void liveHotPathDoesNotCallFullSimulation() throws IOException {
        String dispatchSource = source("src/main/java/com/apunto/engine/service/copy/dispatch/CopyDispatchCoordinator.java");
        String resolverSource = source("src/main/java/com/apunto/engine/hyperliquid/service/impl/HyperliquidCopyCandidateResolver.java");
        String normalized = (dispatchSource + resolverSource).toLowerCase();

        assertFalse(normalized.contains("simulation=full"));
        assertFalse(normalized.contains("fullsimulation"));
        assertFalse(normalized.contains("simulationfull"));
    }

    @Test
    void microLiveBudgetSnapshotUsesOneAggregateDatabaseCall() throws IOException {
        String store = source("src/main/java/com/apunto/engine/service/copy/dispatch/PostgresCopyDispatchIntentStore.java");

        assertTrue(store.contains("repository.loadBudgetSnapshot("));
        assertFalse(store.contains("sumActiveMarginForAllocation("));
        assertFalse(store.contains("sumPendingReservedMargin("));
        assertFalse(store.contains("countActiveForAllocation("));
        assertFalse(store.contains("sumPendingReservedPositions("));
    }

    private String source(String path) throws IOException {
        Path file = Path.of(path);
        assertTrue(Files.isRegularFile(file), "missing architecture source: " + file.toAbsolutePath());
        return Files.readString(file);
    }
}
